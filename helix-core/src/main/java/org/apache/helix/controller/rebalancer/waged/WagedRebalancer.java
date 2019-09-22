package org.apache.helix.controller.rebalancer.waged;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Weight-Aware Globally-Even Distribute Rebalancer.
 * @see <a
 *      href="https://github.com/apache/helix/wiki/Design-Proposal---Weight-Aware-Globally-Even-Distribute-Rebalancer">
 *      Design Document
 *      </a>
 */
public class WagedRebalancer {
  private static final Logger LOG = LoggerFactory.getLogger(WagedRebalancer.class);

  // When any of the following change happens, the rebalancer needs to do a global rebalance which
  // contains 1. baseline recalculate, 2. partial rebalance that is based on the new baseline.
  private static final Set<HelixConstants.ChangeType> GLOBAL_REBALANCE_REQUIRED_CHANGE_TYPES =
      ImmutableSet.of(
          HelixConstants.ChangeType.RESOURCE_CONFIG,
          HelixConstants.ChangeType.CLUSTER_CONFIG,
          HelixConstants.ChangeType.INSTANCE_CONFIG);
  // The cluster change detector is a stateful object.
  // Make it static to avoid unnecessary reinitialization.
  private static final ThreadLocal<ResourceChangeDetector> CHANGE_DETECTOR_THREAD_LOCAL =
      new ThreadLocal<>();
  private final HelixManager _manager;
  private final MappingCalculator<ResourceControllerDataProvider> _mappingCalculator;
  private final AssignmentMetadataStore _assignmentMetadataStore;
  private final RebalanceAlgorithm _rebalanceAlgorithm;

  private static AssignmentMetadataStore constructAssignmentStore(HelixManager helixManager) {
    AssignmentMetadataStore assignmentMetadataStore = null;
    if (helixManager != null) {
      String metadataStoreAddrs = helixManager.getMetadataStoreConnectionString();
      String clusterName = helixManager.getClusterName();
      if (metadataStoreAddrs != null && clusterName != null) {
        assignmentMetadataStore = new AssignmentMetadataStore(metadataStoreAddrs, clusterName);
      }
    }
    return assignmentMetadataStore;
  }

  public WagedRebalancer(HelixManager helixManager,
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences) {
    this(constructAssignmentStore(helixManager),
        ConstraintBasedAlgorithmFactory.getInstance(preferences),
        // Use DelayedAutoRebalancer as the mapping calculator for the final assignment output.
        // Mapping calculator will translate the best possible assignment into the applicable state
        // mapping based on the current states.
        // TODO abstract and separate the main mapping calculator logic from DelayedAutoRebalancer
        new DelayedAutoRebalancer(), helixManager);
  }

  private WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm, MappingCalculator mappingCalculator, HelixManager manager) {
    if (assignmentMetadataStore == null) {
      LOG.warn("Assignment Metadata Store is not configured properly."
          + " The rebalancer will not access the assignment store during the rebalance.");
    }
    _assignmentMetadataStore = assignmentMetadataStore;
    _rebalanceAlgorithm = algorithm;
    _mappingCalculator = mappingCalculator;
    _manager = manager;
  }

  @VisibleForTesting
  protected WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm) {
    this(assignmentMetadataStore, algorithm, new DelayedAutoRebalancer(), null);
  }

  // Release all the resources.
  public void close() {
    if (_assignmentMetadataStore != null) {
      _assignmentMetadataStore.close();
    }
  }

  /**
   * Compute the new IdealStates for all the input resources. The IdealStates include both new
   * partition assignment (in the listFiles) and the new replica state mapping (in the mapFields).
   * @param clusterData The Cluster status data provider.
   * @param resourceMap A map containing all the rebalancing resources.
   * @param currentStateOutput The present Current States of the resources.
   * @return A map of the new IdealStates with the resource name as key.
   */
  public Map<String, IdealState> computeNewIdealStates(ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap, final CurrentStateOutput currentStateOutput)
      throws HelixRebalanceException {
    if (resourceMap.isEmpty()) {
      LOG.warn("There is no resource to be rebalanced by {}",
          this.getClass().getSimpleName());
      return Collections.emptyMap();
    }

    LOG.info("Start computing new ideal states for resources: {}", resourceMap.keySet().toString());
    validateInput(clusterData, resourceMap);

    // Calculate the target assignment based on the current cluster status.
    Map<String, IdealState> newIdealStates =
        computeBestPossibleStates(clusterData, resourceMap, currentStateOutput);

    // Construct the new best possible states according to the current state and target assignment.
    // Note that the new ideal state might be an intermediate state between the current state and
    // the target assignment.
    for (IdealState is : newIdealStates.values()) {
      String resourceName = is.getResourceName();
      // Adjust the states according to the current state.
      ResourceAssignment finalAssignment = _mappingCalculator.computeBestPossiblePartitionState(
          clusterData, is, resourceMap.get(resourceName), currentStateOutput);

      // Clean up the state mapping fields. Use the final assignment that is calculated by the
      // mapping calculator to replace them.
      is.getRecord().getMapFields().clear();
      for (Partition partition : finalAssignment.getMappedPartitions()) {
        Map<String, String> newStateMap = finalAssignment.getReplicaMap(partition);
        // if the final states cannot be generated, override the best possible state with empty map.
        is.setInstanceStateMap(partition.getPartitionName(),
            newStateMap == null ? Collections.emptyMap() : newStateMap);
      }
    }

    LOG.info("Finish computing new ideal states for resources: {}",
        resourceMap.keySet().toString());
    return newIdealStates;
  }

  // Coordinate baseline recalculation and partial rebalance according to the cluster changes.
  private Map<String, IdealState> computeBestPossibleStates(
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      final CurrentStateOutput currentStateOutput) throws HelixRebalanceException {
    getChangeDetector().updateSnapshots(clusterData);
    // Get all the changed items' information
    Map<HelixConstants.ChangeType, Set<String>> clusterChanges =
        getChangeDetector().getChangeTypes().stream()
            .collect(Collectors.toMap(changeType -> changeType, changeType -> {
              Set<String> itemKeys = new HashSet<>();
              itemKeys.addAll(getChangeDetector().getAdditionsByType(changeType));
              itemKeys.addAll(getChangeDetector().getChangesByType(changeType));
              itemKeys.addAll(getChangeDetector().getRemovalsByType(changeType));
              return itemKeys;
            }));

    if (clusterChanges.keySet().stream()
        .anyMatch(changeType -> GLOBAL_REBALANCE_REQUIRED_CHANGE_TYPES.contains(changeType))) {
      refreshBaseline(clusterData, clusterChanges, resourceMap, currentStateOutput);
      // Inject a cluster config change for large scale partial rebalance once the baseline changed.
      clusterChanges.putIfAbsent(HelixConstants.ChangeType.CLUSTER_CONFIG, Collections.emptySet());
    }

    Set<String> activeNodes = new HashSet<>(clusterData.getEnabledLiveInstances());
    ClusterConfig clusterConfig = clusterData.getClusterConfig();
    if (DelayedRebalanceUtil.isDelayRebalanceEnabled(clusterConfig)) {
      // If delayed rebalance is enabled, calculate using the delayed active instance list.
      Set<String> delayedActiveNodes = DelayedRebalanceUtil
          .getActiveInstances(clusterData.getAllInstances(), activeNodes,
              clusterData.getInstanceOfflineTimeMap(), clusterData.getLiveInstances().keySet(),
              clusterData.getInstanceConfigMap(), clusterConfig);
      // Schedule delayed rebalance in case no new cluster event happen before the delay time window
      // passes.
      delayedRebalanceSchedule(clusterData, delayedActiveNodes);
      activeNodes = delayedActiveNodes;
    }

    Map<String, ResourceAssignment> newAssignment =
        partialRebalance(clusterData, clusterChanges, resourceMap, activeNodes,
            currentStateOutput);

    Map<String, Map<String, Integer>> resourceStatePriorityMap = new HashMap<>();
    // Convert the assignments into IdealState for the following state mapping calculation.
    Map<String, IdealState> finalIdealStateMap = new HashMap<>();
    for (String resourceName : newAssignment.keySet()) {
      IdealState newIdeaState;
      try {
        IdealState currentIdealState = clusterData.getIdealState(resourceName);
        Map<String, Integer> statePriorityMap = clusterData
            .getStateModelDef(currentIdealState.getStateModelDefRef()).getStatePriorityMap();
        // Cache the priority map for delayed rebalance calculation which happens later.
        resourceStatePriorityMap.put(resourceName, statePriorityMap);
        // Create a new IdealState instance contains the new calculated assignment in the preference
        // list.
        newIdeaState = generateIdealStateWithAssignment(resourceName, currentIdealState,
            newAssignment.get(resourceName), statePriorityMap);
      } catch (Exception ex) {
        throw new HelixRebalanceException(
            "Fail to calculate the new IdealState for resource: " + resourceName,
            HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
      }
      finalIdealStateMap.put(resourceName, newIdeaState);
    }

    // Additional calculating required for the delayed rebalancing, user-defined preference list.
    applyRebalanceOverwrite(finalIdealStateMap, activeNodes, clusterData, resourceMap,
        clusterChanges, resourceStatePriorityMap);
    // Note the user-defined list will be applied as an overwritten to the final result, instead of
    // part of the rebalance algorithm.
    finalIdealStateMap.entrySet().stream().forEach(idealStateEntry -> applyUserDefinePreferenceList(
        clusterData.getResourceConfig(idealStateEntry.getKey()), idealStateEntry.getValue()));

    return finalIdealStateMap;
  }

  // TODO make the Baseline calculation async if complicated algorithm is used for the Baseline
  private void refreshBaseline(ResourceControllerDataProvider clusterData,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges, Map<String, Resource> resourceMap,
      final CurrentStateOutput currentStateOutput) throws HelixRebalanceException {
    LOG.info("Start calculating the new baseline.");
    Map<String, ResourceAssignment> currentBaseline =
        getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet());
    // For baseline calculation
    // 1. Ignore node status (disable/offline).
    // 2. Use the baseline as the previous best possible assignment since there is no "baseline" for
    // the baseline.
    Map<String, ResourceAssignment> newBaseline =
        calculateAssignment(clusterData, clusterChanges, resourceMap, clusterData.getAllInstances(),
            Collections.emptyMap(), currentBaseline);

    if (_assignmentMetadataStore != null) {
      try {
        _assignmentMetadataStore.persistBaseline(newBaseline);
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to persist the new baseline assignment.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    } else {
      LOG.debug("Assignment Metadata Store is empty. Skip persist the baseline assignment.");
    }

    LOG.info("Finish calculating the new baseline.");
  }

  private Map<String, ResourceAssignment> partialRebalance(
      ResourceControllerDataProvider clusterData,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges, Map<String, Resource> resourceMap,
      Set<String> activeNodes, final CurrentStateOutput currentStateOutput)
      throws HelixRebalanceException {
    LOG.info("Start calculating the new best possible assignment.");
    Map<String, ResourceAssignment> currentBaseline =
        getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet());
    Map<String, ResourceAssignment> currentBestPossibleAssignment =
        getBestPossibleAssignment(_assignmentMetadataStore, currentStateOutput,
            resourceMap.keySet());
    Map<String, ResourceAssignment> newAssignment =
        calculateAssignment(clusterData, clusterChanges, resourceMap, activeNodes, currentBaseline,
            currentBestPossibleAssignment);

    if (_assignmentMetadataStore != null) {
      try {
        // TODO Test to confirm if persisting the final assignment (with final partition states)
        // would be a better option.
        _assignmentMetadataStore.persistBestPossibleAssignment(newAssignment);
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to persist the new best possible assignment.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    } else {
      LOG.debug("Assignment Metadata Store is empty. Skip persist the baseline assignment.");
    }

    LOG.info("Finish calculating the new best possible assignment.");
    return newAssignment;
  }

  /**
   * Generate the cluster model based on the input and calculate the optimal assignment.
   * @param clusterData                the cluster data cache.
   * @param clusterChanges             the detected cluster changes.
   * @param resourceMap                the rebalancing resources.
   * @param activeNodes                the alive and enabled nodes.
   * @param baseline                   the baseline assignment for the algorithm as a reference.
   * @param prevBestPossibleAssignment the previous best possible assignment for the algorithm as a
   *                                   reference.
   * @return the new optimal assignment for the resources.
   */
  private Map<String, ResourceAssignment> calculateAssignment(
      ResourceControllerDataProvider clusterData,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges, Map<String, Resource> resourceMap,
      Set<String> activeNodes, Map<String, ResourceAssignment> baseline,
      Map<String, ResourceAssignment> prevBestPossibleAssignment) throws HelixRebalanceException {
    long startTime = System.currentTimeMillis();
    LOG.info("Start calculating for an assignment");
    ClusterModel clusterModel;
    try {
      clusterModel = ClusterModelProvider.generateClusterModel(clusterData, resourceMap,
          activeNodes, clusterChanges, baseline, prevBestPossibleAssignment);
    } catch (Exception ex) {
      throw new HelixRebalanceException("Failed to generate cluster model.",
          HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
    }

    OptimalAssignment optimalAssignment = _rebalanceAlgorithm.calculate(clusterModel);
    Map<String, ResourceAssignment> newAssignment =
        optimalAssignment.getOptimalResourceAssignment();

    LOG.info("Finish calculating. Time spent: {}ms.", System.currentTimeMillis() - startTime);
    return newAssignment;
  }

  private ResourceChangeDetector getChangeDetector() {
    if (CHANGE_DETECTOR_THREAD_LOCAL.get() == null) {
      CHANGE_DETECTOR_THREAD_LOCAL.set(new ResourceChangeDetector());
    }
    return CHANGE_DETECTOR_THREAD_LOCAL.get();
  }

  // Generate a new IdealState based on the input newAssignment.
  // The assignment will be propagate to the preference lists.
  // Note that we will recalculate the states based on the current state, so there is no need to
  // update the mapping fields in the IdealState output.
  private IdealState generateIdealStateWithAssignment(String resourceName,
      IdealState currentIdealState, ResourceAssignment newAssignment,
      Map<String, Integer> statePriorityMap) {
    IdealState newIdealState = new IdealState(resourceName);
    // Copy the simple fields
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    // Sort the preference list according to state priority.
    newIdealState.setPreferenceLists(getPreferenceLists(newAssignment, statePriorityMap));
    // Note the state mapping in the new assignment won't be directly propagate to the map fields.
    // The rebalancer will calculate for the final state mapping considering the current states.
    return newIdealState;
  }

  // Generate the preference lists from the state mapping based on state priority.
  private Map<String, List<String>> getPreferenceLists(ResourceAssignment newAssignment,
      Map<String, Integer> statePriorityMap) {
    Map<String, List<String>> preferenceList = new HashMap<>();
    for (Partition partition : newAssignment.getMappedPartitions()) {
      List<String> nodes = new ArrayList<>(newAssignment.getReplicaMap(partition).keySet());
      // To ensure backward compatibility, sort the preference list according to state priority.
      nodes.sort((node1, node2) -> {
        int statePriority1 =
            statePriorityMap.get(newAssignment.getReplicaMap(partition).get(node1));
        int statePriority2 =
            statePriorityMap.get(newAssignment.getReplicaMap(partition).get(node2));
        if (statePriority1 == statePriority2) {
          return node1.compareTo(node2);
        } else {
          return statePriority1 - statePriority2;
        }
      });
      preferenceList.put(partition.getPartitionName(), nodes);
    }
    return preferenceList;
  }

  private void validateInput(ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap) throws HelixRebalanceException {
    Set<String> nonCompatibleResources = resourceMap.entrySet().stream().filter(resourceEntry -> {
      IdealState is = clusterData.getIdealState(resourceEntry.getKey());
      return is == null || !is.getRebalanceMode().equals(IdealState.RebalanceMode.FULL_AUTO)
          || !getClass().getName().equals(is.getRebalancerClassName());
    }).map(Map.Entry::getKey).collect(Collectors.toSet());
    if (!nonCompatibleResources.isEmpty()) {
      throw new HelixRebalanceException(String.format(
          "Input contains invalid resource(s) that cannot be rebalanced by the WAGED rebalancer. %s",
          nonCompatibleResources.toString()), HelixRebalanceException.Type.INVALID_INPUT);
    }
  }

  /**
   * @param assignmentMetadataStore
   * @param currentStateOutput
   * @param resources
   * @return The current baseline assignment. If record does not exist in the
   * assignmentMetadataStore, return the current state assignment.
   * @throws HelixRebalanceException
   */
  private Map<String, ResourceAssignment> getBaselineAssignment(
      AssignmentMetadataStore assignmentMetadataStore, CurrentStateOutput currentStateOutput,
      Set<String> resources) throws HelixRebalanceException {
    Map<String, ResourceAssignment> currentBaseline = Collections.emptyMap();
    if (assignmentMetadataStore != null) {
      try {
        currentBaseline = assignmentMetadataStore.getBaseline();
      } catch (HelixException ex) {
        // Report error. and use empty mapping instead.
        LOG.error("Failed to get the current baseline assignment.", ex);
      } catch (Exception ex) {
        throw new HelixRebalanceException(
            "Failed to get the current baseline assignment because of unexpected error.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    }
    if (currentBaseline.isEmpty()) {
      LOG.warn(
          "The current baseline assignment record is empty. Use the current states instead.");
      currentBaseline = getCurrentStateAssingment(currentStateOutput, resources);
    }
    return currentBaseline;
  }

  /**
   * @param assignmentMetadataStore
   * @param currentStateOutput
   * @param resources
   * @return The current best possible assignment. If record does not exist in the
   * assignmentMetadataStore, return the current state assignment.
   * @throws HelixRebalanceException
   */
  private Map<String, ResourceAssignment> getBestPossibleAssignment(
      AssignmentMetadataStore assignmentMetadataStore, CurrentStateOutput currentStateOutput,
      Set<String> resources) throws HelixRebalanceException {
    Map<String, ResourceAssignment> currentBestAssignment = Collections.emptyMap();
    if (assignmentMetadataStore != null) {
      try {
        currentBestAssignment = assignmentMetadataStore.getBestPossibleAssignment();
      } catch (HelixException ex) {
        // Report error. and use empty mapping instead.
        LOG.error("Failed to get the current best possible assignment.", ex);
      } catch (Exception ex) {
        throw new HelixRebalanceException(
            "Failed to get the current best possible assignment because of unexpected error.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    }
    if (currentBestAssignment.isEmpty()) {
      LOG.warn(
          "The current best possible assignment record is empty. Use the current states instead.");
      currentBestAssignment = getCurrentStateAssingment(currentStateOutput, resources);
    }
    return currentBestAssignment;
  }

  private Map<String, ResourceAssignment> getCurrentStateAssingment(
      CurrentStateOutput currentStateOutput, Set<String> resourceSet) {
    Map<String, ResourceAssignment> currentStateAssignment = new HashMap<>();
    for (String resourceName : resourceSet) {
      Map<Partition, Map<String, String>> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName);
      if (!currentStateMap.isEmpty()) {
        ResourceAssignment newResourceAssignment = new ResourceAssignment(resourceName);
        currentStateMap.entrySet().stream().forEach(currentStateEntry -> {
          newResourceAssignment
              .addReplicaMap(currentStateEntry.getKey(), currentStateEntry.getValue());
        });
        currentStateAssignment.put(resourceName, newResourceAssignment);
      }
    }
    return currentStateAssignment;
  }

  /**
   * Schedule rebalance according to the delayed rebalance logic.
   * @param clusterData the current cluster data cache
   * @param delayedActiveNodes the active nodes set that is calculated with the delay time window
   */
  private void delayedRebalanceSchedule(ResourceControllerDataProvider clusterData,
      Set<String> delayedActiveNodes) {
    if (_manager != null) {
      // Schedule for the next delayed rebalance in case no cluster change event happens.
      ClusterConfig clusterConfig = clusterData.getClusterConfig();
      Set<String> offlineOrDisabledInstances = new HashSet<>(delayedActiveNodes);
      offlineOrDisabledInstances.removeAll(clusterData.getEnabledLiveInstances());
      for (IdealState idealState : clusterData.getIdealStates().values()) {
        DelayedRebalanceUtil.setRebalanceScheduler(idealState, offlineOrDisabledInstances,
            clusterData.getInstanceOfflineTimeMap(), clusterData.getLiveInstances().keySet(),
            clusterData.getInstanceConfigMap(), clusterConfig.getRebalanceDelayTime(),
            clusterConfig, _manager);
      }
    } else {
      LOG.warn("Skip schedule for the delayed rebalancer since HelixManager is not specified.");
    }
  }

  /**
   * Update the ideal states in the input according to the current active nodes.
   * Note that since the rebalance might be done with delayed logic, the ideal states calculated
   * might include inactive (although in the delayed time window) nodes. This overwrite is to adjust
   * the final mapping accordingly. So as to ensure the result is completely valid.
   *
   * @param idealStateMap            the calculated ideal states.
   * @param virtualActiveNodes       the instances list that were used to calculate the idealStateMap.
   * @param clusterData              the cluster data cache.
   * @param resourceMap              the rebalanaced resource map.
   * @param clusterChanges           the detected cluster changes that triggeres the rebalance.
   * @param resourceStatePriorityMap the state priority map for each resource.
   */
  private void applyRebalanceOverwrite(Map<String, IdealState> idealStateMap,
      Set<String> virtualActiveNodes, ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap, Map<HelixConstants.ChangeType, Set<String>> clusterChanges,
      Map<String, Map<String, Integer>> resourceStatePriorityMap) throws HelixRebalanceException {
    Set<String> activeInstances = clusterData.getEnabledLiveInstances();
    if (!virtualActiveNodes.equals(activeInstances)) {
      // Note that the calculation used the baseline as the input only. This is for minimizing unnecessary partition movement.
      Map<String, ResourceAssignment> activeAssignment =
          calculateAssignment(clusterData, clusterChanges, resourceMap, activeInstances,
              Collections.emptyMap(), _assignmentMetadataStore.getBaseline());
      for (String resourceName : idealStateMap.keySet()) {
        IdealState is = idealStateMap.get(resourceName);
        if (!activeAssignment.containsKey(resourceName)) {
          throw new HelixRebalanceException(
              "Failed to calculate the complete partition assignment with all active nodes. Cannot find the resource assignment for "
                  + resourceName, HelixRebalanceException.Type.FAILED_TO_CALCULATE);
        }
        IdealState currentIdealState = clusterData.getIdealState(resourceName);
        IdealState newActiveIdealState =
            generateIdealStateWithAssignment(resourceName, currentIdealState,
                activeAssignment.get(resourceName), resourceStatePriorityMap.get(resourceName));
        Map<String, List<String>> finalPreferenceLists = DelayedRebalanceUtil
            .getFinalDelayedMapping(newActiveIdealState.getPreferenceLists(),
                is.getPreferenceLists(), activeInstances, DelayedRebalanceUtil
                    .getMinActiveReplica(currentIdealState, currentIdealState.getReplicaCount(activeAssignment.size())));
        is.setPreferenceLists(finalPreferenceLists);
      }
    }
  }

  private void applyUserDefinePreferenceList(ResourceConfig resourceConfig, IdealState idealState) {
    if (resourceConfig != null) {
      Map<String, List<String>> userDefinedPreferenceList = resourceConfig.getPreferenceLists();
      if (!userDefinedPreferenceList.isEmpty()) {
        LOG.info("Using user defined preference list for partitions: " + userDefinedPreferenceList
            .keySet());
        for (String partition : userDefinedPreferenceList.keySet()) {
          idealState.setPreferenceList(partition, userDefinedPreferenceList.get(partition));
        }
      }
    }
  }
}
