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

import com.google.common.collect.ImmutableSet;
import org.apache.helix.HelixConstants;
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
import org.apache.helix.monitoring.metrics.MetricCollector;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.implementation.BaselineDivergenceGauge;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
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
      ImmutableSet.of(HelixConstants.ChangeType.RESOURCE_CONFIG,
          HelixConstants.ChangeType.IDEAL_STATE,
          HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG);
  private final ResourceChangeDetector _changeDetector;
  private final HelixManager _manager;
  private final MappingCalculator<ResourceControllerDataProvider> _mappingCalculator;
  private final AssignmentMetadataStore _assignmentMetadataStore;
  private final RebalanceAlgorithm _rebalanceAlgorithm;
  private MetricCollector _metricCollector;

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
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences,
      MetricCollector metricCollector) {
    this(constructAssignmentStore(helixManager),
        ConstraintBasedAlgorithmFactory.getInstance(preferences),
        // Use DelayedAutoRebalancer as the mapping calculator for the final assignment output.
        // Mapping calculator will translate the best possible assignment into the applicable state
        // mapping based on the current states.
        // TODO abstract and separate the main mapping calculator logic from DelayedAutoRebalancer
        new DelayedAutoRebalancer(),
        // Helix Manager is required for the rebalancer scheduler
        helixManager, metricCollector);
  }

  /**
   * This constructor will use null for HelixManager and MetricCollector. With null HelixManager,
   * the rebalancer will rebalance solely based on CurrentStates. With null MetricCollector, the
   * rebalancer will not emit JMX metrics.
   * @param assignmentMetadataStore
   * @param algorithm
   * @param mappingCalculator
   */
  protected WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm, MappingCalculator mappingCalculator) {
    this(assignmentMetadataStore, algorithm, mappingCalculator, null, null);
  }

  /**
   * This constructor will use null for HelixManager and MetricCollector. With null HelixManager,
   * the rebalancer will rebalance solely based on CurrentStates.
   * @param assignmentMetadataStore
   * @param algorithm
   * @param metricCollector
   */
  protected WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm, MetricCollector metricCollector) {
    this(assignmentMetadataStore, algorithm, new DelayedAutoRebalancer(), null, metricCollector);
  }

  private WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm, MappingCalculator mappingCalculator, HelixManager manager,
      MetricCollector metricCollector) {
    if (assignmentMetadataStore == null) {
      LOG.warn("Assignment Metadata Store is not configured properly."
          + " The rebalancer will not access the assignment store during the rebalance.");
    }
    _assignmentMetadataStore = assignmentMetadataStore;
    _rebalanceAlgorithm = algorithm;
    _mappingCalculator = mappingCalculator;
    _manager = manager;
    // If metricCollector is null, instantiate a version that does not register metrics in order to
    // allow rebalancer to proceed
    _metricCollector =
        metricCollector == null ? new WagedRebalancerMetricCollector() : metricCollector;
    _changeDetector = new ResourceChangeDetector(true);
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
      LOG.warn("There is no resource to be rebalanced by {}", this.getClass().getSimpleName());
      return Collections.emptyMap();
    }

    LOG.info("Start computing new ideal states for resources: {}", resourceMap.keySet().toString());
    validateInput(clusterData, resourceMap);

    Map<String, IdealState> newIdealStates;
    try {
      // Calculate the target assignment based on the current cluster status.
      newIdealStates = computeBestPossibleStates(clusterData, resourceMap, currentStateOutput);
    } catch (HelixRebalanceException ex) {
      LOG.error("Failed to calculate the new assignments.", ex);
      // Record the failure in metrics.
      CountMetric rebalanceFailureCount = _metricCollector.getMetric(
          WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceFailureCounter.name(),
          CountMetric.class);
      rebalanceFailureCount.increment(1L);

      HelixRebalanceException.Type failureType = ex.getFailureType();
      if (failureType.equals(HelixRebalanceException.Type.INVALID_REBALANCER_STATUS) || failureType
          .equals(HelixRebalanceException.Type.UNKNOWN_FAILURE)) {
        // If the failure is unknown or because of assignment store access failure, throw the
        // rebalance exception.
        throw ex;
      } else { // return the previously calculated assignment.
        LOG.warn(
            "Returning the last known-good best possible assignment from metadata store due to "
                + "rebalance failure of type: {}", failureType);
        // Note that don't return an assignment based on the current state if there is no previously
        // calculated result in this fallback logic.
        Map<String, ResourceAssignment> assignmentRecord =
            getBestPossibleAssignment(_assignmentMetadataStore, new CurrentStateOutput(),
                resourceMap.keySet());
        newIdealStates = convertResourceAssignment(clusterData, assignmentRecord);
      }
    }

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
  protected Map<String, IdealState> computeBestPossibleStates(
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
    // Filter for the items that have content changed.
    clusterChanges =
        clusterChanges.entrySet().stream().filter(changeEntry -> !changeEntry.getValue().isEmpty())
            .collect(Collectors
                .toMap(changeEntry -> changeEntry.getKey(), changeEntry -> changeEntry.getValue()));

    // Perform Global Baseline Calculation
    if (clusterChanges.keySet().stream()
        .anyMatch(GLOBAL_REBALANCE_REQUIRED_CHANGE_TYPES::contains)) {
      refreshBaseline(clusterData, clusterChanges, resourceMap, currentStateOutput);
    }

    Set<String> activeNodes = DelayedRebalanceUtil.getActiveNodes(clusterData.getAllInstances(),
        clusterData.getEnabledLiveInstances(), clusterData.getInstanceOfflineTimeMap(),
        clusterData.getLiveInstances().keySet(), clusterData.getInstanceConfigMap(),
        clusterData.getClusterConfig());

    // Schedule (or unschedule) delayed rebalance according to the delayed rebalance config.
    delayedRebalanceSchedule(clusterData, activeNodes, resourceMap.keySet());

    // Perform partial rebalance
    Map<String, ResourceAssignment> newAssignment =
        partialRebalance(clusterData, clusterChanges, resourceMap, activeNodes, currentStateOutput);

    Map<String, IdealState> finalIdealStateMap =
        convertResourceAssignment(clusterData, newAssignment);

    // The additional rebalance overwrite is required since the calculated mapping may contains
    // some delayed rebalanced assignments.
    if (!activeNodes.equals(clusterData.getEnabledLiveInstances())) {
      applyRebalanceOverwrite(finalIdealStateMap, clusterData, resourceMap, clusterChanges,
          getBaselineAssignment(_assignmentMetadataStore, currentStateOutput,
              resourceMap.keySet()));
    }
    // Replace the assignment if user-defined preference list is configured.
    // Note the user-defined list is intentionally applied to the final mapping after calculation.
    // This is to avoid persisting it into the assignment store, which impacts the long term
    // assignment evenness and partition movements.
    finalIdealStateMap.entrySet().stream()
        .forEach(idealStateEntry -> applyUserDefinedPreferenceList(
            clusterData.getResourceConfig(idealStateEntry.getKey()), idealStateEntry.getValue()));

    return finalIdealStateMap;
  }

  /**
   * Convert the resource assignment map into an IdealState map.
   */
  private Map<String, IdealState> convertResourceAssignment(
      ResourceControllerDataProvider clusterData, Map<String, ResourceAssignment> assignments)
      throws HelixRebalanceException {
    // Convert the assignments into IdealState for the following state mapping calculation.
    Map<String, IdealState> finalIdealStateMap = new HashMap<>();
    for (String resourceName : assignments.keySet()) {
      try {
        IdealState currentIdealState = clusterData.getIdealState(resourceName);
        Map<String, Integer> statePriorityMap =
            clusterData.getStateModelDef(currentIdealState.getStateModelDefRef())
                .getStatePriorityMap();
        // Create a new IdealState instance which contains the new calculated assignment in the
        // preference list.
        IdealState newIdealState = new IdealState(resourceName);
        // Copy the simple fields
        newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
        // Sort the preference list according to state priority.
        newIdealState.setPreferenceLists(
            getPreferenceLists(assignments.get(resourceName), statePriorityMap));
        // Note the state mapping in the new assignment won't directly propagate to the map fields.
        // The rebalancer will calculate for the final state mapping considering the current states.
        finalIdealStateMap.put(resourceName, newIdealState);
      } catch (Exception ex) {
        throw new HelixRebalanceException(
            "Failed to calculate the new IdealState for resource: " + resourceName,
            HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
      }
    }
    return finalIdealStateMap;
  }

  // TODO make the Baseline calculation async if complicated algorithm is used for the Baseline
  private void refreshBaseline(ResourceControllerDataProvider clusterData,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges, Map<String, Resource> resourceMap,
      final CurrentStateOutput currentStateOutput) throws HelixRebalanceException {
    LOG.info("Start calculating the new baseline.");
    CountMetric globalBaselineCalcCounter = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.GlobalBaselineCalcCounter.name(),
        CountMetric.class);
    globalBaselineCalcCounter.increment(1L);

    LatencyMetric globalBaselineCalcLatency = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.GlobalBaselineCalcLatencyGauge
            .name(),
        LatencyMetric.class);
    globalBaselineCalcLatency.startMeasuringLatency();
    // Read the baseline from metadata store
    Map<String, ResourceAssignment> currentBaseline =
        getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet());

    // For baseline calculation
    // 1. Ignore node status (disable/offline).
    // 2. Use the baseline as the previous best possible assignment since there is no "baseline" for
    // the baseline.
    Map<String, ResourceAssignment> newBaseline = calculateAssignment(clusterData, clusterChanges,
        resourceMap, clusterData.getAllInstances(), Collections.emptyMap(), currentBaseline);

    // Write the new baseline to metadata store
    if (_assignmentMetadataStore != null) {
      try {
        LatencyMetric writeLatency = _metricCollector.getMetric(
            WagedRebalancerMetricCollector.WagedRebalancerMetricNames.StateWriteLatencyGauge.name(),
            LatencyMetric.class);
        writeLatency.startMeasuringLatency();
        _assignmentMetadataStore.persistBaseline(newBaseline);
        writeLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to persist the new baseline assignment.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    } else {
      LOG.debug("Assignment Metadata Store is empty. Skip persist the baseline assignment.");
    }
    globalBaselineCalcLatency.endMeasuringLatency();
    LOG.info("Finish calculating the new baseline.");
  }

  private Map<String, ResourceAssignment> partialRebalance(
      ResourceControllerDataProvider clusterData,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges, Map<String, Resource> resourceMap,
      Set<String> activeNodes, final CurrentStateOutput currentStateOutput)
      throws HelixRebalanceException {
    LOG.info("Start calculating the new best possible assignment.");
    CountMetric partialRebalanceCounter = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceCounter.name(),
        CountMetric.class);
    partialRebalanceCounter.increment(1L);

    LatencyMetric partialRebalanceLatency = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceLatencyGauge
            .name(),
        LatencyMetric.class);
    partialRebalanceLatency.startMeasuringLatency();
    // TODO: Consider combining the metrics for both baseline/best possible?
    // Read the baseline from metadata store
    Map<String, ResourceAssignment> currentBaseline =
        getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet());

    // Read the best possible assignment from metadata store
    Map<String, ResourceAssignment> currentBestPossibleAssignment = getBestPossibleAssignment(
        _assignmentMetadataStore, currentStateOutput, resourceMap.keySet());

    // Compute the new assignment
    Map<String, ResourceAssignment> newAssignment = calculateAssignment(clusterData, clusterChanges,
        resourceMap, activeNodes, currentBaseline, currentBestPossibleAssignment);

    // Asynchronously report baseline divergence metric before persisting to metadata store,
    // just in case if persisting fails, we still have the metric.
    BaselineDivergenceGauge baselineDivergenceGauge = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.BaselineDivergenceGauge.name(),
        BaselineDivergenceGauge.class);
    baselineDivergenceGauge.asyncMeasureAndUpdateValue(clusterData.getAsyncTasksThreadPool(),
        currentBaseline, newAssignment);

    if (_assignmentMetadataStore != null) {
      try {
        LatencyMetric writeLatency = _metricCollector.getMetric(
            WagedRebalancerMetricCollector.WagedRebalancerMetricNames.StateWriteLatencyGauge.name(),
            LatencyMetric.class);
        writeLatency.startMeasuringLatency();
        // TODO Test to confirm if persisting the final assignment (with final partition states)
        // would be a better option.
        _assignmentMetadataStore.persistBestPossibleAssignment(newAssignment);
        writeLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to persist the new best possible assignment.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    } else {
      LOG.debug("Assignment Metadata Store is empty. Skip persist the baseline assignment.");
    }
    partialRebalanceLatency.endMeasuringLatency();
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

    LOG.info("Finish calculating an assignment. Took: {} ms.", System.currentTimeMillis() - startTime);

    return newAssignment;
  }

  private ResourceChangeDetector getChangeDetector() {
    return _changeDetector;
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
   *         assignmentMetadataStore, return the current state assignment.
   * @throws HelixRebalanceException
   */
  private Map<String, ResourceAssignment> getBaselineAssignment(
      AssignmentMetadataStore assignmentMetadataStore, CurrentStateOutput currentStateOutput,
      Set<String> resources) throws HelixRebalanceException {
    Map<String, ResourceAssignment> currentBaseline = Collections.emptyMap();
    if (assignmentMetadataStore != null) {
      try {
        LatencyMetric stateReadLatency = _metricCollector.getMetric(
            WagedRebalancerMetricCollector.WagedRebalancerMetricNames.StateReadLatencyGauge.name(),
            LatencyMetric.class);
        stateReadLatency.startMeasuringLatency();
        currentBaseline = assignmentMetadataStore.getBaseline();
        stateReadLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException(
            "Failed to get the current baseline assignment because of unexpected error.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    }
    if (currentBaseline.isEmpty()) {
      LOG.warn("The current baseline assignment record is empty. Use the current states instead.");
      currentBaseline = currentStateOutput.getAssignment(resources);
    }
    currentBaseline.keySet().retainAll(resources);
    return currentBaseline;
  }

  /**
   * @param assignmentMetadataStore
   * @param currentStateOutput
   * @param resources
   * @return The current best possible assignment. If record does not exist in the
   *         assignmentMetadataStore, return the current state assignment.
   * @throws HelixRebalanceException
   */
  private Map<String, ResourceAssignment> getBestPossibleAssignment(
      AssignmentMetadataStore assignmentMetadataStore, CurrentStateOutput currentStateOutput,
      Set<String> resources) throws HelixRebalanceException {
    Map<String, ResourceAssignment> currentBestAssignment = Collections.emptyMap();
    if (assignmentMetadataStore != null) {
      try {
        LatencyMetric stateReadLatency = _metricCollector.getMetric(
            WagedRebalancerMetricCollector.WagedRebalancerMetricNames.StateReadLatencyGauge.name(),
            LatencyMetric.class);
        stateReadLatency.startMeasuringLatency();
        currentBestAssignment = assignmentMetadataStore.getBestPossibleAssignment();
        stateReadLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException(
            "Failed to get the current best possible assignment because of unexpected error.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    }
    if (currentBestAssignment.isEmpty()) {
      LOG.warn(
          "The current best possible assignment record is empty. Use the current states instead.");
      currentBestAssignment = currentStateOutput.getAssignment(resources);
    }
    currentBestAssignment.keySet().retainAll(resources);
    return currentBestAssignment;
  }

  /**
   * Schedule rebalance according to the delayed rebalance logic.
   * @param clusterData the current cluster data cache
   * @param delayedActiveNodes the active nodes set that is calculated with the delay time window
   * @param resourceSet the rebalanced resourceSet
   */
  private void delayedRebalanceSchedule(ResourceControllerDataProvider clusterData,
      Set<String> delayedActiveNodes, Set<String> resourceSet) {
    if (_manager != null) {
      // Schedule for the next delayed rebalance in case no cluster change event happens.
      ClusterConfig clusterConfig = clusterData.getClusterConfig();
      boolean delayedRebalanceEnabled = DelayedRebalanceUtil.isDelayRebalanceEnabled(clusterConfig);
      Set<String> offlineOrDisabledInstances = new HashSet<>(delayedActiveNodes);
      offlineOrDisabledInstances.removeAll(clusterData.getEnabledLiveInstances());
      for (String resource : resourceSet) {
        DelayedRebalanceUtil.setRebalanceScheduler(resource, delayedRebalanceEnabled,
            offlineOrDisabledInstances, clusterData.getInstanceOfflineTimeMap(),
            clusterData.getLiveInstances().keySet(), clusterData.getInstanceConfigMap(),
            clusterConfig.getRebalanceDelayTime(), clusterConfig, _manager);
      }
    } else {
      LOG.warn("Skip scheduling a delayed rebalancer since HelixManager is not specified.");
    }
  }

  /**
   * Update the rebalanced ideal states according to the real active nodes.
   * Since the rebalancing might be done with the delayed logic, the rebalanced ideal states
   * might include inactive nodes.
   * This overwrite will adjust the final mapping, so as to ensure the result is completely valid.
   * @param idealStateMap the calculated ideal states.
   * @param clusterData the cluster data cache.
   * @param resourceMap the rebalanaced resource map.
   * @param clusterChanges the detected cluster changes that triggeres the rebalance.
   * @param baseline the baseline assignment
   */
  private void applyRebalanceOverwrite(Map<String, IdealState> idealStateMap,
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges,
      Map<String, ResourceAssignment> baseline) throws HelixRebalanceException {
    Set<String> enabledLiveInstances = clusterData.getEnabledLiveInstances();
    // Note that the calculation used the baseline as the input only. This is for minimizing
    // unnecessary partition movement.
    Map<String, IdealState> activeIdealStates = convertResourceAssignment(clusterData,
        calculateAssignment(clusterData, clusterChanges, resourceMap, enabledLiveInstances,
            Collections.emptyMap(), baseline));
    for (String resourceName : idealStateMap.keySet()) {
      // The new calculated ideal state before overwrite
      IdealState newIdealState = idealStateMap.get(resourceName);
      if (!activeIdealStates.containsKey(resourceName)) {
        throw new HelixRebalanceException(
            "Failed to calculate the complete partition assignment with all active nodes. Cannot find the resource assignment for "
                + resourceName, HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      }
      // The ideal state that is calculated based on the real alive/enabled instances list
      IdealState newActiveIdealState = activeIdealStates.get(resourceName);
      // The current ideal state that exists in the IdealState znode
      IdealState currentIdealState = clusterData.getIdealState(resourceName);
      int numReplica = currentIdealState.getReplicaCount(enabledLiveInstances.size());
      int minActiveReplica =
          DelayedRebalanceUtil.getMinActiveReplica(currentIdealState, numReplica);
      Map<String, List<String>> finalPreferenceLists = DelayedRebalanceUtil
          .getFinalDelayedMapping(newActiveIdealState.getPreferenceLists(),
              newIdealState.getPreferenceLists(), enabledLiveInstances,
              Math.min(minActiveReplica, numReplica));

      newIdealState.setPreferenceLists(finalPreferenceLists);
    }
  }

  private void applyUserDefinedPreferenceList(ResourceConfig resourceConfig,
      IdealState idealState) {
    if (resourceConfig != null) {
      Map<String, List<String>> userDefinedPreferenceList = resourceConfig.getPreferenceLists();
      if (!userDefinedPreferenceList.isEmpty()) {
        LOG.info("Using user defined preference list for partitions.");
        for (String partition : userDefinedPreferenceList.keySet()) {
          idealState.setPreferenceList(partition, userDefinedPreferenceList.get(partition));
        }
      }
    }
  }

  protected MetricCollector getMetricCollector() {
    return _metricCollector;
  }

  @Override
  protected void finalize()
      throws Throwable {
    super.finalize();
    close();
  }
}
