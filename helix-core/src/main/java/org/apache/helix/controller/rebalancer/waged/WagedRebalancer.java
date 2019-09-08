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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
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
      Collections
          .unmodifiableSet(new HashSet<>(Arrays.asList(HelixConstants.ChangeType.RESOURCE_CONFIG,
              HelixConstants.ChangeType.CLUSTER_CONFIG,
              HelixConstants.ChangeType.INSTANCE_CONFIG)));
  // The cluster change detector is a stateful object.
  // Make it static to avoid unnecessary reinitialization.
  private static final ThreadLocal<ResourceChangeDetector> CHANGE_DETECTOR_THREAD_LOCAL =
      new ThreadLocal<>();
  private final MappingCalculator<ResourceControllerDataProvider> _mappingCalculator;

  // --------- The following fields are placeholders and need replacement. -----------//
  // TODO Shall we make the metadata store a static threadlocal object as well to avoid
  // reinitialization?
  private final AssignmentMetadataStore _assignmentMetadataStore;
  private final RebalanceAlgorithm _rebalanceAlgorithm;
  // ------------------------------------------------------------------------------------//

  public WagedRebalancer(HelixManager helixManager) {
    this(
        // TODO init the metadata store according to their requirement when integrate,
        // or change to final static method if possible.
        new AssignmentMetadataStore(helixManager),
        // TODO parse the cluster setting
        ConstraintBasedAlgorithmFactory.getInstance(),
        // Use DelayedAutoRebalancer as the mapping calculator for the final assignment output.
        // Mapping calculator will translate the best possible assignment into the applicable state
        // mapping based on the current states.
        // TODO abstract and separate the main mapping calculator logic from DelayedAutoRebalancer
        new DelayedAutoRebalancer());
  }

  private WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm, MappingCalculator mappingCalculator) {
    _assignmentMetadataStore = assignmentMetadataStore;
    _rebalanceAlgorithm = algorithm;
    _mappingCalculator = mappingCalculator;
  }

  @VisibleForTesting
  protected WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm) {
    this(assignmentMetadataStore, algorithm, new DelayedAutoRebalancer());

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
    LOG.info("Start computing new ideal states for resources: {}", resourceMap.keySet().toString());

    // Find the compatible resources: 1. FULL_AUTO 2. Configured to use the WAGED rebalancer
    resourceMap = resourceMap.entrySet().stream().filter(resourceEntry -> {
      IdealState is = clusterData.getIdealState(resourceEntry.getKey());
      return is != null && is.getRebalanceMode().equals(IdealState.RebalanceMode.FULL_AUTO)
          && getClass().getName().equals(is.getRebalancerClassName());
    }).collect(Collectors.toMap(resourceEntry -> resourceEntry.getKey(),
        resourceEntry -> resourceEntry.getValue()));

    if (resourceMap.isEmpty()) {
      LOG.warn("There is no valid resource to be rebalanced by {}",
          this.getClass().getSimpleName());
      return Collections.emptyMap();
    } else {
      LOG.info("Valid resources that will be rebalanced by {}: {}", this.getClass().getSimpleName(),
          resourceMap.keySet().toString());
    }

    // Calculate the target assignment based on the current cluster status.
    Map<String, IdealState> newIdealStates = computeBestPossibleStates(clusterData, resourceMap);

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
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap)
      throws HelixRebalanceException {
    getChangeDetector().updateSnapshots(clusterData);
    // Get all the modified and new items' information
    Map<HelixConstants.ChangeType, Set<String>> clusterChanges =
        getChangeDetector().getChangeTypes().stream()
            .collect(Collectors.toMap(changeType -> changeType, changeType -> {
              Set<String> itemKeys = new HashSet<>();
              itemKeys.addAll(getChangeDetector().getAdditionsByType(changeType));
              itemKeys.addAll(getChangeDetector().getChangesByType(changeType));
              return itemKeys;
            }));

    if (clusterChanges.keySet().stream()
        .anyMatch(changeType -> GLOBAL_REBALANCE_REQUIRED_CHANGE_TYPES.contains(changeType))) {
      refreshBaseline(clusterData, clusterChanges, resourceMap);
      // Inject a cluster config change for large scale partial rebalance once the baseline changed.
      clusterChanges.putIfAbsent(HelixConstants.ChangeType.CLUSTER_CONFIG, Collections.emptySet());
    }

    Map<String, ResourceAssignment> newAssignment =
        partialRebalance(clusterData, clusterChanges, resourceMap);

    // Convert the assignments into IdealState for the following state mapping calculation.
    Map<String, IdealState> finalIdealState = new HashMap<>();
    for (String resourceName : newAssignment.keySet()) {
      IdealState newIdeaState;
      try {
        IdealState currentIdealState = clusterData.getIdealState(resourceName);
        Map<String, Integer> statePriorityMap = clusterData
            .getStateModelDef(currentIdealState.getStateModelDefRef()).getStatePriorityMap();
        // Create a new IdealState instance contains the new calculated assignment in the preference
        // list.
        newIdeaState = generateIdealStateWithAssignment(resourceName, currentIdealState,
            newAssignment.get(resourceName), statePriorityMap);
      } catch (Exception ex) {
        throw new HelixRebalanceException(
            "Fail to calculate the new IdealState for resource: " + resourceName,
            HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
      }
      finalIdealState.put(resourceName, newIdeaState);
    }
    return finalIdealState;
  }

  // TODO make the Baseline calculation async if complicated algorithm is used for the Baseline
  private void refreshBaseline(ResourceControllerDataProvider clusterData,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges, Map<String, Resource> resourceMap)
      throws HelixRebalanceException {
    // For baseline calculation
    // 1. Ignore node status (disable/offline).
    // 2. Use the baseline as the previous best possible assignment since there is no "baseline" for
    // the baseline.
    LOG.info("Start calculating the new baseline.");
    Map<String, ResourceAssignment> currentBaseline;
    try {
      currentBaseline = _assignmentMetadataStore.getBaseline();
    } catch (Exception ex) {
      throw new HelixRebalanceException("Failed to get the current baseline assignment.",
          HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
    }
    Map<String, ResourceAssignment> baseline = calculateAssignment(clusterData, clusterChanges,
        resourceMap, clusterData.getAllInstances(), Collections.emptyMap(), currentBaseline);
    try {
      _assignmentMetadataStore.persistBaseline(baseline);
    } catch (Exception ex) {
      throw new HelixRebalanceException("Failed to persist the new baseline assignment.",
          HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
    }
    LOG.info("Finish calculating the new baseline.");
  }

  private Map<String, ResourceAssignment> partialRebalance(
      ResourceControllerDataProvider clusterData,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges, Map<String, Resource> resourceMap)
      throws HelixRebalanceException {
    LOG.info("Start calculating the new best possible assignment.");
    Set<String> activeInstances = clusterData.getEnabledLiveInstances();
    Map<String, ResourceAssignment> baseline;
    Map<String, ResourceAssignment> prevBestPossibleAssignment;
    try {
      baseline = _assignmentMetadataStore.getBaseline();
      prevBestPossibleAssignment = _assignmentMetadataStore.getBestPossibleAssignment();
    } catch (Exception ex) {
      throw new HelixRebalanceException("Failed to get the persisted assignment records.",
          HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
    }
    Map<String, ResourceAssignment> newAssignment = calculateAssignment(clusterData, clusterChanges,
        resourceMap, activeInstances, baseline, prevBestPossibleAssignment);
    try {
      // TODO Test to confirm if persisting the final assignment (with final partition states)
      // would be a better option.
      _assignmentMetadataStore.persistBestPossibleAssignment(newAssignment);
    } catch (Exception ex) {
      throw new HelixRebalanceException("Failed to persist the new best possible assignment.",
          HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
    }
    LOG.info("Finish calculating the new best possible assignment.");
    return newAssignment;
  }

  /**
   * Generate the cluster model based on the input and calculate the optimal assignment.
   * @param clusterData the cluster data cache.
   * @param clusterChanges the detected cluster changes.
   * @param resourceMap the rebalancing resources.
   * @param activeNodes the alive and enabled nodes.
   * @param baseline the baseline assignment for the algorithm as a reference.
   * @param prevBestPossibleAssignment the previous best possible assignment for the algorithm as a
   *          reference.
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
}
