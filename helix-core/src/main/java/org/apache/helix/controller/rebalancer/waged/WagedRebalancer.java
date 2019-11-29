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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
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
import org.apache.helix.util.RebalanceUtil;
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
      ImmutableSet
          .of(HelixConstants.ChangeType.RESOURCE_CONFIG, HelixConstants.ChangeType.IDEAL_STATE,
              HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG);
  // To identify if the preference has been configured or not.
  private static final Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer>
      NOT_CONFIGURED_PREFERENCE = ImmutableMap
      .of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, -1,
          ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, -1);

  // To calculate the baseline asynchronously
  private final ExecutorService _baselineCalculateExecutor;
  private final ResourceChangeDetector _changeDetector;
  private final HelixManager _manager;
  private final MappingCalculator<ResourceControllerDataProvider> _mappingCalculator;
  private final AssignmentMetadataStore _assignmentMetadataStore;

  private final MetricCollector _metricCollector;
  private final CountMetric _rebalanceFailureCount;
  private final CountMetric _globalBaselineCalcCounter;
  private final LatencyMetric _globalBaselineCalcLatency;
  private final LatencyMetric _writeLatency;
  private final CountMetric _partialRebalanceCounter;
  private final LatencyMetric _partialRebalanceLatency;
  private final LatencyMetric _stateReadLatency;
  private final BaselineDivergenceGauge _baselineDivergenceGauge;

  private boolean _asyncBaselineCalculation;

  // Note, the rebalance algorithm field is mutable so it should not be directly referred except for
  // the public method computeNewIdealStates.
  private RebalanceAlgorithm _rebalanceAlgorithm;
  private Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> _preference =
      NOT_CONFIGURED_PREFERENCE;

  private static AssignmentMetadataStore constructAssignmentStore(String metadataStoreAddrs,
      String clusterName) {
    if (metadataStoreAddrs != null && clusterName != null) {
      return new AssignmentMetadataStore(metadataStoreAddrs, clusterName);
    }
    return null;
  }

  public WagedRebalancer(HelixManager helixManager,
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference,
      boolean isAsyncGlobalRebalance) {
    this(helixManager == null ? null
            : constructAssignmentStore(helixManager.getMetadataStoreConnectionString(),
                helixManager.getClusterName()), ConstraintBasedAlgorithmFactory.getInstance(preference),
        // Use DelayedAutoRebalancer as the mapping calculator for the final assignment output.
        // Mapping calculator will translate the best possible assignment into the applicable state
        // mapping based on the current states.
        // TODO abstract and separate the main mapping calculator logic from DelayedAutoRebalancer
        new DelayedAutoRebalancer(),
        // Helix Manager is required for the rebalancer scheduler
        helixManager,
        // If HelixManager is null, we just pass in null for MetricCollector so that a
        // non-functioning WagedRebalancerMetricCollector would be created in WagedRebalancer's
        // constructor. This is to handle two cases: 1. HelixManager is null for non-testing cases -
        // in this case, WagedRebalancer will not read/write to metadata store and just use
        // CurrentState-based rebalancing. 2. Tests that require instrumenting the rebalancer for
        // verifying whether the cluster has converged.
        helixManager == null ? null
            : new WagedRebalancerMetricCollector(helixManager.getClusterName()),
        isAsyncGlobalRebalance);
    _preference = ImmutableMap.copyOf(preference);
  }

  /**
   * This constructor will use null for HelixManager and MetricCollector. With null HelixManager,
   * the rebalancer will not schedule for a future delayed rebalance. With null MetricCollector, the
   * rebalancer will not emit JMX metrics.
   * @param assignmentMetadataStore
   * @param algorithm
   */
  protected WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm) {
    this(assignmentMetadataStore, algorithm, new DelayedAutoRebalancer(), null, null, false);
  }

  /**
   * This constructor will use null for HelixManager. With null HelixManager, the rebalancer will
   * not schedule for a future delayed rebalance.
   * @param assignmentMetadataStore
   * @param algorithm
   * @param metricCollector
   */
  protected WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm, MetricCollector metricCollector) {
    this(assignmentMetadataStore, algorithm, new DelayedAutoRebalancer(), null, metricCollector,
        false);
  }

  private WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm, MappingCalculator mappingCalculator, HelixManager manager,
      MetricCollector metricCollector, boolean asyncBaselineCalculation) {
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
    _rebalanceFailureCount = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceFailureCounter.name(),
        CountMetric.class);
    _globalBaselineCalcCounter = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.GlobalBaselineCalcCounter.name(),
        CountMetric.class);
    _globalBaselineCalcLatency = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.GlobalBaselineCalcLatencyGauge
            .name(),
        LatencyMetric.class);
    _partialRebalanceCounter = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceCounter.name(),
        CountMetric.class);
    _partialRebalanceLatency = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceLatencyGauge
            .name(),
        LatencyMetric.class);
    _writeLatency = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.StateWriteLatencyGauge.name(),
        LatencyMetric.class);
    _stateReadLatency = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.StateReadLatencyGauge.name(),
        LatencyMetric.class);
    _baselineDivergenceGauge = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.BaselineDivergenceGauge.name(),
        BaselineDivergenceGauge.class);

    _changeDetector = new ResourceChangeDetector(true);

    _baselineCalculateExecutor = Executors.newSingleThreadExecutor();
    _asyncBaselineCalculation = asyncBaselineCalculation;
  }

  // Update the rebalancer configuration if the new options are different from the current
  // configuration.
  public synchronized void updateRebalanceOptions(
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> newPreference,
      boolean asyncBaselineCalculation) {
    _asyncBaselineCalculation = asyncBaselineCalculation;
    // 1. if the preference was not configured during constructing, no need to update.
    // 2. if the preference equals to the new preference, no need to update.
    if (!_preference.equals(NOT_CONFIGURED_PREFERENCE) && !_preference.equals(newPreference)) {
      _rebalanceAlgorithm = ConstraintBasedAlgorithmFactory.getInstance(newPreference);
      _preference = ImmutableMap.copyOf(newPreference);
    }
  }

  // Release all the resources.
  public void close() {
    if (_baselineCalculateExecutor != null) {
      _baselineCalculateExecutor.shutdown();
    }
    if (_assignmentMetadataStore != null) {
      _assignmentMetadataStore.close();
    }
    _metricCollector.unregister();
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
      newIdealStates = computeBestPossibleStates(clusterData, resourceMap, currentStateOutput,
          _rebalanceAlgorithm);
    } catch (HelixRebalanceException ex) {
      LOG.error("Failed to calculate the new assignments.", ex);
      // Record the failure in metrics.
      _rebalanceFailureCount.increment(1L);

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
    newIdealStates.values().parallelStream().forEach(idealState -> {
      String resourceName = idealState.getResourceName();
      // Adjust the states according to the current state.
      ResourceAssignment finalAssignment = _mappingCalculator
          .computeBestPossiblePartitionState(clusterData, idealState, resourceMap.get(resourceName),
              currentStateOutput);

      // Clean up the state mapping fields. Use the final assignment that is calculated by the
      // mapping calculator to replace them.
      idealState.getRecord().getMapFields().clear();
      for (Partition partition : finalAssignment.getMappedPartitions()) {
        Map<String, String> newStateMap = finalAssignment.getReplicaMap(partition);
        // if the final states cannot be generated, override the best possible state with empty map.
        idealState.setInstanceStateMap(partition.getPartitionName(),
            newStateMap == null ? Collections.emptyMap() : newStateMap);
      }
    });
    LOG.info("Finish computing new ideal states for resources: {}",
        resourceMap.keySet().toString());
    return newIdealStates;
  }

  // Coordinate baseline recalculation and partial rebalance according to the cluster changes.
  private Map<String, IdealState> computeBestPossibleStates(
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      final CurrentStateOutput currentStateOutput, RebalanceAlgorithm algorithm)
      throws HelixRebalanceException {
    Set<String> activeNodes = DelayedRebalanceUtil
        .getActiveNodes(clusterData.getAllInstances(), clusterData.getEnabledLiveInstances(),
            clusterData.getInstanceOfflineTimeMap(), clusterData.getLiveInstances().keySet(),
            clusterData.getInstanceConfigMap(), clusterData.getClusterConfig());

    // Schedule (or unschedule) delayed rebalance according to the delayed rebalance config.
    delayedRebalanceSchedule(clusterData, activeNodes, resourceMap.keySet());

    Map<String, IdealState> newIdealStates = convertResourceAssignment(clusterData,
        computeBestPossibleAssignment(clusterData, resourceMap, activeNodes, currentStateOutput,
            algorithm));

    // The additional rebalance overwrite is required since the calculated mapping may contain
    // some delayed rebalanced assignments.
    if (!activeNodes.equals(clusterData.getEnabledLiveInstances())) {
      applyRebalanceOverwrite(newIdealStates, clusterData, resourceMap,
          getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet()),
          algorithm);
    }
    // Replace the assignment if user-defined preference list is configured.
    // Note the user-defined list is intentionally applied to the final mapping after calculation.
    // This is to avoid persisting it into the assignment store, which impacts the long term
    // assignment evenness and partition movements.
    newIdealStates.entrySet().stream().forEach(idealStateEntry -> applyUserDefinedPreferenceList(
        clusterData.getResourceConfig(idealStateEntry.getKey()), idealStateEntry.getValue()));

    return newIdealStates;
  }

  // Coordinate global rebalance and partial rebalance according to the cluster changes.
  protected Map<String, ResourceAssignment> computeBestPossibleAssignment(
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Set<String> activeNodes, final CurrentStateOutput currentStateOutput,
      RebalanceAlgorithm algorithm)
      throws HelixRebalanceException {
    // Perform global rebalance for a new baseline assignment
    globalRebalance(clusterData, resourceMap, currentStateOutput, algorithm);
    // Perform partial rebalance for a new best possible assignment
    Map<String, ResourceAssignment> newAssignment =
        partialRebalance(clusterData, resourceMap, activeNodes, currentStateOutput, algorithm);
    return newAssignment;
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

  /**
   * Global rebalance calculates for a new baseline assignment.
   * The new baseline assignment will be persisted and leveraged by the partial rebalance.
   * @param clusterData
   * @param resourceMap
   * @param currentStateOutput
   * @param algorithm
   * @throws HelixRebalanceException
   */
  private void globalRebalance(ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap, final CurrentStateOutput currentStateOutput,
      RebalanceAlgorithm algorithm)
      throws HelixRebalanceException {
    _changeDetector.updateSnapshots(clusterData);
    // Get all the changed items' information. Filter for the items that have content changed.
    final Map<HelixConstants.ChangeType, Set<String>> clusterChanges =
        _changeDetector.getAllChanges();

    if (clusterChanges.keySet().stream()
        .anyMatch(GLOBAL_REBALANCE_REQUIRED_CHANGE_TYPES::contains)) {
      // Build the cluster model for rebalance calculation.
      // Note, for a Baseline calculation,
      // 1. Ignore node status (disable/offline).
      // 2. Use the previous Baseline as the only parameter about the previous assignment.
      Map<String, ResourceAssignment> currentBaseline =
          getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet());
      ClusterModel clusterModel;
      try {
        clusterModel = ClusterModelProvider
            .generateClusterModelForBaseline(clusterData, resourceMap,
                clusterData.getAllInstances(), clusterChanges, currentBaseline);
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to generate cluster model for global rebalance.",
            HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
      }

      final boolean waitForGlobalRebalance = !_asyncBaselineCalculation;
      final String clusterName = clusterData.getClusterName();
      // Calculate the Baseline assignment for global rebalance.
      Future<Boolean> result = _baselineCalculateExecutor.submit(() -> {
        try {
          // Note that we should schedule a new partial rebalance if the following calculation does
          // not wait until the new baseline is calculated.
          // So set doSchedulePartialRebalance to be !waitForGlobalRebalance
          calculateAndUpdateBaseline(clusterModel, algorithm, !waitForGlobalRebalance, clusterName);
        } catch (HelixRebalanceException e) {
          LOG.error("Failed to calculate baseline assignment!", e);
          return false;
        }
        return true;
      });
      if (waitForGlobalRebalance) {
        try {
          if (!result.get()) {
            throw new HelixRebalanceException("Failed to calculate for the new Baseline.",
                HelixRebalanceException.Type.FAILED_TO_CALCULATE);
          }
        } catch (InterruptedException | ExecutionException e) {
          throw new HelixRebalanceException("Failed to execute new Baseline calculation.",
              HelixRebalanceException.Type.FAILED_TO_CALCULATE, e);
        }
      }
    }
  }

  /**
   * Calculate and update the Baseline assignment
   * @param clusterModel
   * @param doSchedulePartialRebalance True if the call should trigger a following partial rebalance
   *                                   so the new Baseline could be applied to cluster.
   * @param clusterName
   * @throws HelixRebalanceException
   */
  private void calculateAndUpdateBaseline(ClusterModel clusterModel, RebalanceAlgorithm algorithm,
      boolean doSchedulePartialRebalance, String clusterName)
      throws HelixRebalanceException {
    LOG.info("Start calculating the new baseline.");
    _globalBaselineCalcCounter.increment(1L);
    _globalBaselineCalcLatency.startMeasuringLatency();

    boolean isbaselineUpdated = false;
    Map<String, ResourceAssignment> newBaseline = calculateAssignment(clusterModel, algorithm);
    // Write the new baseline to metadata store
    if (_assignmentMetadataStore != null) {
      try {
        _writeLatency.startMeasuringLatency();
        isbaselineUpdated = _assignmentMetadataStore.persistBaseline(newBaseline);
        _writeLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to persist the new baseline assignment.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    } else {
      LOG.debug("Assignment Metadata Store is null. Skip persisting the baseline assignment.");
    }
    _globalBaselineCalcLatency.endMeasuringLatency();
    LOG.info("Finish calculating the new baseline.");

    if (isbaselineUpdated && doSchedulePartialRebalance) {
      LOG.info("Schedule a new rebalance after the new baseline calculated.");
      RebalanceUtil.scheduleOnDemandPipeline(clusterName, 0l, false);
    }
  }

  private Map<String, ResourceAssignment> partialRebalance(
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Set<String> activeNodes, final CurrentStateOutput currentStateOutput,
      RebalanceAlgorithm algorithm)
      throws HelixRebalanceException {
    LOG.info("Start calculating the new best possible assignment.");
    _partialRebalanceCounter.increment(1L);
    _partialRebalanceLatency.startMeasuringLatency();
    // TODO: Consider combining the metrics for both baseline/best possible?
    // Read the baseline from metadata store
    Map<String, ResourceAssignment> currentBaseline =
        getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet());

    // Read the best possible assignment from metadata store
    Map<String, ResourceAssignment> currentBestPossibleAssignment =
        getBestPossibleAssignment(_assignmentMetadataStore, currentStateOutput,
            resourceMap.keySet());
    ClusterModel clusterModel;
    try {
      clusterModel = ClusterModelProvider
          .generateClusterModelForPartialRebalance(clusterData, resourceMap, activeNodes,
              currentBaseline, currentBestPossibleAssignment);
    } catch (Exception ex) {
      throw new HelixRebalanceException("Failed to generate cluster model for partial rebalance.",
          HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
    }
    Map<String, ResourceAssignment> newAssignment = calculateAssignment(clusterModel, algorithm);

    // Asynchronously report baseline divergence metric before persisting to metadata store,
    // just in case if persisting fails, we still have the metric.
    // To avoid changes of the new assignment and make it safe when being used to measure baseline
    // divergence, use a deep copy of the new assignment.
    Map<String, ResourceAssignment> newAssignmentCopy = new HashMap<>();
    for (Map.Entry<String, ResourceAssignment> entry : newAssignment.entrySet()) {
      newAssignmentCopy.put(entry.getKey(), new ResourceAssignment(entry.getValue().getRecord()));
    }

    _baselineDivergenceGauge.asyncMeasureAndUpdateValue(clusterData.getAsyncTasksThreadPool(),
        currentBaseline, newAssignmentCopy);

    if (_assignmentMetadataStore != null) {
      try {
        _writeLatency.startMeasuringLatency();
        _assignmentMetadataStore.persistBestPossibleAssignment(newAssignment);
        _writeLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to persist the new best possible assignment.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    } else {
      LOG.debug("Assignment Metadata Store is null. Skip persisting the baseline assignment.");
    }
    _partialRebalanceLatency.endMeasuringLatency();
    LOG.info("Finish calculating the new best possible assignment.");
    return newAssignment;
  }

  /**
   * @param clusterModel the cluster model that contains all the cluster status for the purpose of
   *                     rebalancing.
   * @return the new optimal assignment for the resources.
   */
  private Map<String, ResourceAssignment> calculateAssignment(ClusterModel clusterModel,
      RebalanceAlgorithm algorithm) throws HelixRebalanceException {
    long startTime = System.currentTimeMillis();
    LOG.info("Start calculating for an assignment with algorithm {}",
        algorithm.getClass().getSimpleName());
    OptimalAssignment optimalAssignment = algorithm.calculate(clusterModel);
    Map<String, ResourceAssignment> newAssignment =
        optimalAssignment.getOptimalResourceAssignment();
    LOG.info("Finish calculating an assignment with algorithm {}. Took: {} ms.",
        algorithm.getClass().getSimpleName(), System.currentTimeMillis() - startTime);
    return newAssignment;
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
          || !WagedRebalancer.class.getName().equals(is.getRebalancerClassName());
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
        _stateReadLatency.startMeasuringLatency();
        currentBaseline = assignmentMetadataStore.getBaseline();
        _stateReadLatency.endMeasuringLatency();
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
  protected Map<String, ResourceAssignment> getBestPossibleAssignment(
      AssignmentMetadataStore assignmentMetadataStore, CurrentStateOutput currentStateOutput,
      Set<String> resources) throws HelixRebalanceException {
    Map<String, ResourceAssignment> currentBestAssignment = Collections.emptyMap();
    if (assignmentMetadataStore != null) {
      try {
        _stateReadLatency.startMeasuringLatency();
        currentBestAssignment = assignmentMetadataStore.getBestPossibleAssignment();
        _stateReadLatency.endMeasuringLatency();
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
        DelayedRebalanceUtil
            .setRebalanceScheduler(resource, delayedRebalanceEnabled, offlineOrDisabledInstances,
                clusterData.getInstanceOfflineTimeMap(), clusterData.getLiveInstances().keySet(),
                clusterData.getInstanceConfigMap(), clusterConfig.getRebalanceDelayTime(),
                clusterConfig, _manager);
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
   * @param baseline the baseline assignment.
   * @param algorithm the rebalance algorithm.
   */
  private void applyRebalanceOverwrite(Map<String, IdealState> idealStateMap,
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Map<String, ResourceAssignment> baseline, RebalanceAlgorithm algorithm)
      throws HelixRebalanceException {
    ClusterModel clusterModel;
    try {
      // Note this calculation uses the baseline as the best possible assignment input here.
      // This is for minimizing unnecessary partition movement.
      clusterModel = ClusterModelProvider
          .generateClusterModelFromExistingAssignment(clusterData, resourceMap, baseline);
    } catch (Exception ex) {
      throw new HelixRebalanceException(
          "Failed to generate cluster model for delayed rebalance overwrite.",
          HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
    }
    Map<String, IdealState> activeIdealStates =
        convertResourceAssignment(clusterData, calculateAssignment(clusterModel, algorithm));
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
      Set<String> enabledLiveInstances = clusterData.getEnabledLiveInstances();
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

  protected AssignmentMetadataStore getAssignmentMetadataStore() {
    return _assignmentMetadataStore;
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
