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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.StatefulRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.metrics.MetricCollector;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
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
public class WagedRebalancer implements StatefulRebalancer<ResourceControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(WagedRebalancer.class);

  // To identify if the preference has been configured or not.
  private static final Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer>
      NOT_CONFIGURED_PREFERENCE = ImmutableMap
      .of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, -1,
          ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, -1);
  // The default algorithm to use when there is no preference configured.
  private static final RebalanceAlgorithm DEFAULT_REBALANCE_ALGORITHM =
      ConstraintBasedAlgorithmFactory
          .getInstance(ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE);
  // These failure types should be propagated to caller of computeNewIdealStates()
  private static final List<HelixRebalanceException.Type> FAILURE_TYPES_TO_PROPAGATE =
      ImmutableList.of(HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, HelixRebalanceException.Type.UNKNOWN_FAILURE);

  private final HelixManager _manager;
  private final MappingCalculator<ResourceControllerDataProvider> _mappingCalculator;
  private final AssignmentMetadataStore _assignmentMetadataStore;

  private final MetricCollector _metricCollector;
  private final CountMetric _rebalanceFailureCount;
  private final LatencyMetric _writeLatency;
  private final CountMetric _emergencyRebalanceCounter;
  private final LatencyMetric _emergencyRebalanceLatency;
  private final CountMetric _rebalanceOverwriteCounter;
  private final LatencyMetric _rebalanceOverwriteLatency;
  private final AssignmentManager _assignmentManager;
  private final PartialRebalanceRunner _partialRebalanceRunner;
  private final GlobalRebalanceRunner _globalRebalanceRunner;

  // Note, the rebalance algorithm field is mutable so it should not be directly referred except for
  // the public method computeNewIdealStates.
  private RebalanceAlgorithm _rebalanceAlgorithm;
  private Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> _preference = NOT_CONFIGURED_PREFERENCE;

  private static AssignmentMetadataStore constructAssignmentStore(String metadataStoreAddrs,
      String clusterName) {
    if (metadataStoreAddrs != null && clusterName != null) {
      return new AssignmentMetadataStore(metadataStoreAddrs, clusterName);
    }
    return null;
  }

  public WagedRebalancer(HelixManager helixManager) {
    this(helixManager == null ? null
            : constructAssignmentStore(helixManager.getMetadataStoreConnectionString(),
                helixManager.getClusterName()),
        DEFAULT_REBALANCE_ALGORITHM,
        // Use DelayedAutoRebalancer as the mapping calculator for the final assignment output.
        // Mapping calculator will translate the best possible assignment into the applicable state
        // mapping based on the current states.
        // TODO abstract and separate the main mapping calculator logic from DelayedAutoRebalancer
        new DelayedAutoRebalancer(),
        // Helix Manager is required for the rebalancer scheduler
        helixManager,
        // If HelixManager is null, we just pass in a non-functioning WagedRebalancerMetricCollector
        // that will not be registered to MBean.
        // This is to handle two cases: 1. HelixManager is null for non-testing cases. In this case,
        // WagedRebalancer will not read/write to metadata store and just use CurrentState-based
        // rebalancing. 2. Tests that require instrumenting the rebalancer for verifying whether the
        // cluster has converged.
        helixManager == null ? new WagedRebalancerMetricCollector()
            : new WagedRebalancerMetricCollector(helixManager.getClusterName()),
        ClusterConfig.DEFAULT_GLOBAL_REBALANCE_ASYNC_MODE_ENABLED,
        ClusterConfig.DEFAULT_PARTIAL_REBALANCE_ASYNC_MODE_ENABLED);
    _preference = ImmutableMap.copyOf(ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE);
  }

  /**
   * This constructor will use null for HelixManager. With null HelixManager, the rebalancer will
   * not schedule for a future delayed rebalance.
   * @param assignmentMetadataStore
   * @param algorithm
   * @param metricCollectorOptional
   */
  protected WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm, Optional<MetricCollector> metricCollectorOptional) {
    this(assignmentMetadataStore, algorithm, new DelayedAutoRebalancer(), null,
        // If metricCollector is not provided, instantiate a version that does not register metrics
        // in order to allow rebalancer to proceed
        metricCollectorOptional.orElse(new WagedRebalancerMetricCollector()),
        false, false);
  }

  private WagedRebalancer(AssignmentMetadataStore assignmentMetadataStore,
      RebalanceAlgorithm algorithm, MappingCalculator mappingCalculator, HelixManager manager,
      MetricCollector metricCollector, boolean isAsyncGlobalRebalanceEnabled,
      boolean isAsyncPartialRebalanceEnabled) {
    if (assignmentMetadataStore == null) {
      LOG.warn("Assignment Metadata Store is not configured properly."
          + " The rebalancer will not access the assignment store during the rebalance.");
    }
    _assignmentMetadataStore = assignmentMetadataStore;
    _rebalanceAlgorithm = algorithm;
    _mappingCalculator = mappingCalculator;
    if (manager == null) {
      LOG.warn("HelixManager is not provided. The rebalancer is not going to schedule for a future "
          + "rebalance even when delayed rebalance is enabled.");
    }
    _manager = manager;

    _metricCollector = metricCollector;
    _rebalanceFailureCount = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceFailureCounter.name(),
        CountMetric.class);
    _emergencyRebalanceCounter = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.EmergencyRebalanceCounter.name(), CountMetric.class);
    _emergencyRebalanceLatency = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.EmergencyRebalanceLatencyGauge.name(),
        LatencyMetric.class);
    _rebalanceOverwriteCounter = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceOverwriteCounter.name(), CountMetric.class);
    _rebalanceOverwriteLatency = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceOverwriteLatencyGauge.name(),
        LatencyMetric.class);
    _writeLatency = _metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.StateWriteLatencyGauge.name(),
        LatencyMetric.class);
    _assignmentManager = new AssignmentManager(_metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.StateReadLatencyGauge.name(),
        LatencyMetric.class));

    _partialRebalanceRunner = new PartialRebalanceRunner(_assignmentManager, assignmentMetadataStore, metricCollector,
        _rebalanceFailureCount, isAsyncPartialRebalanceEnabled);
    _globalRebalanceRunner = new GlobalRebalanceRunner(_assignmentManager, assignmentMetadataStore, metricCollector,
        _writeLatency, _rebalanceFailureCount, isAsyncGlobalRebalanceEnabled);
  }

  // Update the global rebalance mode to be asynchronous or synchronous
  public void setGlobalRebalanceAsyncMode(boolean isAsyncGlobalRebalanceEnabled) {
    _globalRebalanceRunner.setGlobalRebalanceAsyncMode(isAsyncGlobalRebalanceEnabled);
  }

  // Update the partial rebalance mode to be asynchronous or synchronous
  public void setPartialRebalanceAsyncMode(boolean isAsyncPartialRebalanceEnabled) {
    _partialRebalanceRunner.setPartialRebalanceAsyncMode(isAsyncPartialRebalanceEnabled);
  }

  // Update the rebalancer preference if the new options are different from the current preference.
  public synchronized void updateRebalancePreference(
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> newPreference) {
    // 1. if the preference was not configured during constructing, no need to update.
    // 2. if the preference equals to the new preference, no need to update.
    if (!_preference.equals(NOT_CONFIGURED_PREFERENCE) && !_preference.equals(newPreference)) {
      _rebalanceAlgorithm = ConstraintBasedAlgorithmFactory.getInstance(newPreference);
      _preference = ImmutableMap.copyOf(newPreference);
    }
  }

  @Override
  public void reset() {
    if (_assignmentMetadataStore != null) {
      _assignmentMetadataStore.reset();
    }
    _globalRebalanceRunner.resetChangeDetector();
  }

  // TODO the rebalancer should reject any other computing request after being closed.
  @Override
  public void close() {
    _partialRebalanceRunner.close();
    _globalRebalanceRunner.close();
    if (_assignmentMetadataStore != null) {
      _assignmentMetadataStore.close();
    }
    _metricCollector.unregister();
  }

  @Override
  public Map<String, IdealState> computeNewIdealStates(ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap, final CurrentStateOutput currentStateOutput)
      throws HelixRebalanceException {
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
      if (failureTypesToPropagate().contains(failureType)) {
        // If the failure is unknown or because of assignment store access failure, throw the
        // rebalance exception.
        throw ex;
      } else {
        // return the previously calculated assignment.
        LOG.warn(
            "Returning the last known-good best possible assignment from metadata store due to "
                + "rebalance failure of type: {}", failureType);
        // Note that don't return an assignment based on the current state if there is no previously
        // calculated result in this fallback logic.
        Map<String, ResourceAssignment> assignmentRecord =
            _assignmentManager.getBestPossibleAssignment(_assignmentMetadataStore, new CurrentStateOutput(),
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

  // Coordinate global rebalance and partial rebalance according to the cluster changes.
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

    Map<String, ResourceAssignment> newBestPossibleAssignment =
        computeBestPossibleAssignment(clusterData, resourceMap, activeNodes, currentStateOutput, algorithm);
    Map<String, IdealState> newIdealStates = convertResourceAssignment(clusterData, newBestPossibleAssignment);

    // Replace the assignment if user-defined preference list is configured.
    // Note the user-defined list is intentionally applied to the final mapping after calculation.
    // This is to avoid persisting it into the assignment store, which impacts the long term
    // assignment evenness and partition movements.
    newIdealStates.forEach(
        (resourceName, idealState) -> applyUserDefinedPreferenceList(clusterData.getResourceConfig(resourceName),
            idealState));

    return newIdealStates;
  }

  // Coordinate global rebalance and partial rebalance according to the cluster changes.
  protected Map<String, ResourceAssignment> computeBestPossibleAssignment(
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Set<String> activeNodes, final CurrentStateOutput currentStateOutput,
      RebalanceAlgorithm algorithm)
      throws HelixRebalanceException {
    // Perform global rebalance for a new baseline assignment
    _globalRebalanceRunner.globalRebalance(clusterData, resourceMap, currentStateOutput, algorithm);
    // Perform emergency rebalance for a new best possible assignment
    return emergencyRebalance(clusterData, resourceMap, activeNodes, currentStateOutput, algorithm);
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

  protected List<HelixRebalanceException.Type> failureTypesToPropagate() {
    return FAILURE_TYPES_TO_PROPAGATE;
  }

  /**
   * Some partition may fail to meet minActiveReplica due to delayed rebalance, because some instances are offline yet
   * active. In this case, additional replicas have to be brought up -- until either the instance gets back, or timeout,
   * at which we have a more permanent resolution.
   * The term "overwrite" is inherited from historical approach, however, it's no longer technically an overwrite.
   * It's a formal rebalance process that goes through the algorithm and all constraints.
   * @param clusterData Cluster data cache
   * @param resourceMap The map of resource to calculate
   * @param activeNodes All active nodes (live nodes plus offline-yet-active nodes) while considering cluster's
   *                    delayed rebalance config
   * @param currentResourceAssignment The current resource assignment or the best possible assignment computed from last
   *                           emergency rebalance.
   * @param algorithm The rebalance algorithm
   * @return The resource assignment with delayed rebalance minActiveReplica
   */
  private Map<String, ResourceAssignment> handleDelayedRebalanceMinActiveReplica(
      ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap,
      Set<String> activeNodes,
      Map<String, ResourceAssignment> currentResourceAssignment,
      RebalanceAlgorithm algorithm) throws HelixRebalanceException {
    // the "real" live nodes at the time
    // TODO: this is a hacky way to filter our on operation instance. We should consider redesign `getEnabledLiveInstances()`.
    final Set<String> enabledLiveInstances = filterOutOnOperationInstances(clusterData.getInstanceConfigMap(), clusterData.getEnabledLiveInstances());
    if (activeNodes.equals(enabledLiveInstances) || !requireRebalanceOverwrite(clusterData, currentResourceAssignment)) {
      // no need for additional process, return the current resource assignment
      return currentResourceAssignment;
    }
    _rebalanceOverwriteCounter.increment(1L);
    _rebalanceOverwriteLatency.startMeasuringLatency();
    LOG.info("Start delayed rebalance overwrites in emergency rebalance.");
    try {
      // use the "real" live and enabled instances for calculation
      ClusterModel clusterModel = ClusterModelProvider.generateClusterModelForDelayedRebalanceOverwrites(
          clusterData, resourceMap, enabledLiveInstances, currentResourceAssignment);
      Map<String, ResourceAssignment> assignment = WagedRebalanceUtil.calculateAssignment(clusterModel, algorithm);
      // keep only the resource entries requiring changes for minActiveReplica
      assignment.keySet().retainAll(clusterModel.getAssignableReplicaMap().keySet());
      DelayedRebalanceUtil.mergeAssignments(assignment, currentResourceAssignment);
      return currentResourceAssignment;
    } catch (HelixRebalanceException e) {
      LOG.error("Failed to compute for delayed rebalance overwrites in cluster {}", clusterData.getClusterName());
      throw e;
    } catch (Exception e) {
      LOG.error("Failed to compute for delayed rebalance overwrites in cluster {}", clusterData.getClusterName());
      throw new HelixRebalanceException("Failed to compute for delayed rebalance overwrites in cluster "
          + clusterData.getClusterConfig(), HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, e);
    } finally {
      _rebalanceOverwriteLatency.endMeasuringLatency();
    }
  }

  private static Set<String> filterOutOnOperationInstances(Map<String, InstanceConfig> instanceConfigMap,
      Set<String> nodes) {
    return nodes.stream()
        .filter(
            instance -> !DelayedAutoRebalancer.INSTANCE_OPERATION_TO_EXCLUDE_FROM_ASSIGNMENT.contains(instanceConfigMap.get(instance).getInstanceOperation()))
        .collect(Collectors.toSet());
  }

  /**
   * Emergency rebalance is scheduled to quickly handle urgent cases like reassigning partitions from inactive nodes
   * and addressing for partitions failing to meet minActiveReplicas.
   * The scope of the computation here should be limited to handling urgency only and shouldn't be blocking.
   * @param clusterData Cluster data cache
   * @param resourceMap resource map
   * @param activeNodes All active nodes (live nodes plus offline-yet-active nodes) while considering cluster's
   *                    delayed rebalance config
   * @param currentStateOutput Current state output from pipeline
   * @param algorithm The rebalance algorithm
   * @return The new resource assignment
   */
  protected Map<String, ResourceAssignment> emergencyRebalance(
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Set<String> activeNodes, final CurrentStateOutput currentStateOutput,
      RebalanceAlgorithm algorithm)
      throws HelixRebalanceException {
    LOG.info("Start emergency rebalance.");
    _emergencyRebalanceCounter.increment(1L);
    _emergencyRebalanceLatency.startMeasuringLatency();

    Map<String, ResourceAssignment> currentBestPossibleAssignment =
        _assignmentManager.getBestPossibleAssignment(_assignmentMetadataStore, currentStateOutput,
            resourceMap.keySet());

    // Step 1: Check for permanent node down
    AtomicBoolean allNodesActive = new AtomicBoolean(true);
    currentBestPossibleAssignment.values().parallelStream().forEach((resourceAssignment -> {
      resourceAssignment.getMappedPartitions().parallelStream().forEach(partition -> {
        for (String instance : resourceAssignment.getReplicaMap(partition).keySet()) {
          if (!activeNodes.contains(instance)) {
            allNodesActive.set(false);
            break;
          }
        }
      });
    }));


    // Step 2: if there are permanent node downs, calculate for a new one best possible
    Map<String, ResourceAssignment> newAssignment;
    if (!allNodesActive.get()) {
      LOG.info("Emergency rebalance responding to permanent node down.");
      ClusterModel clusterModel;
      try {
        clusterModel =
            ClusterModelProvider.generateClusterModelForEmergencyRebalance(clusterData, resourceMap, activeNodes,
                currentBestPossibleAssignment);
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to generate cluster model for emergency rebalance.",
            HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
      }
      newAssignment = WagedRebalanceUtil.calculateAssignment(clusterModel, algorithm);
    } else {
      newAssignment = currentBestPossibleAssignment;
    }

    // Step 3: persist result to metadata store
    persistBestPossibleAssignment(newAssignment);

    // Step 4: handle delayed rebalance minActiveReplica
    // Note this result is one step branching from the main calculation and SHOULD NOT be persisted -- it is temporary,
    // and only apply during the delayed window of those offline yet active nodes, a definitive resolution will happen
    // once the node comes back of remain offline after the delayed window.
    Map<String, ResourceAssignment> assignmentWithDelayedRebalanceAdjust = newAssignment;
    if (_partialRebalanceRunner.isAsyncPartialRebalanceEnabled()) {
      assignmentWithDelayedRebalanceAdjust =
          handleDelayedRebalanceMinActiveReplica(clusterData, resourceMap, activeNodes, newAssignment, algorithm);
    }

    _emergencyRebalanceLatency.endMeasuringLatency();
    LOG.info("Finish emergency rebalance");

    _partialRebalanceRunner.partialRebalance(clusterData, resourceMap, activeNodes, currentStateOutput, algorithm);
    if (!_partialRebalanceRunner.isAsyncPartialRebalanceEnabled()) {
      newAssignment = _assignmentManager.getBestPossibleAssignment(_assignmentMetadataStore, currentStateOutput,
          resourceMap.keySet());
      persistBestPossibleAssignment(newAssignment);
      // delayed rebalance handling result is temporary, shouldn't be persisted
      assignmentWithDelayedRebalanceAdjust =
          handleDelayedRebalanceMinActiveReplica(clusterData, resourceMap, activeNodes, newAssignment, algorithm);
    }

    return assignmentWithDelayedRebalanceAdjust;
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
    Set<String> nonCompatibleResources = resourceMap.keySet().stream()
        .filter(resource -> !WagedValidationUtil.isWagedEnabled(clusterData.getIdealState(resource)))
        .collect(Collectors.toSet());

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
   * @return The current best possible assignment. If record does not exist in the
   *         assignmentMetadataStore, return the current state assignment.
   * @throws HelixRebalanceException
   */
  protected Map<String, ResourceAssignment> getBestPossibleAssignment(
      AssignmentMetadataStore assignmentMetadataStore, CurrentStateOutput currentStateOutput,
      Set<String> resources) throws HelixRebalanceException {
    return _assignmentManager.getBestPossibleAssignment(assignmentMetadataStore, currentStateOutput, resources);
  }

  private void persistBestPossibleAssignment(Map<String, ResourceAssignment> bestPossibleAssignment)
      throws HelixRebalanceException {
    if (_assignmentMetadataStore != null && _assignmentMetadataStore.isBestPossibleChanged(bestPossibleAssignment)) {
      try {
        _writeLatency.startMeasuringLatency();
        _assignmentMetadataStore.persistBestPossibleAssignment(bestPossibleAssignment);
        _writeLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to persist the new best possible assignment.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    } else {
      LOG.debug("Assignment Metadata Store is null. Skip persisting the best possible assignment.");
    }
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

  protected boolean requireRebalanceOverwrite(ResourceControllerDataProvider clusterData,
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    AtomicBoolean allMinActiveReplicaMet = new AtomicBoolean(true);
    bestPossibleAssignment.values().parallelStream().forEach((resourceAssignment -> {
      String resourceName = resourceAssignment.getResourceName();
      IdealState currentIdealState = clusterData.getIdealState(resourceName);
      Set<String> enabledLiveInstances =
          filterOutOnOperationInstances(clusterData.getInstanceConfigMap(), clusterData.getEnabledLiveInstances());
      int numReplica = currentIdealState.getReplicaCount(enabledLiveInstances.size());
      int minActiveReplica = DelayedRebalanceUtil.getMinActiveReplica(ResourceConfig
          .mergeIdealStateWithResourceConfig(clusterData.getResourceConfig(resourceName),
              currentIdealState), currentIdealState, numReplica);
      resourceAssignment.getMappedPartitions().parallelStream().forEach(partition -> {
        int enabledLivePlacementCounter = 0;
        for (String instance : resourceAssignment.getReplicaMap(partition).keySet()) {
          if (enabledLiveInstances.contains(instance)) {
            enabledLivePlacementCounter++;
          }
        }

        if (enabledLivePlacementCounter < Math.min(minActiveReplica, numReplica)) {
          allMinActiveReplicaMet.set(false);
        }
      });
    }));

    return !allMinActiveReplicaMet.get();
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

  protected ResourceChangeDetector getChangeDetector() {
    return _globalRebalanceRunner.getChangeDetector();
  }

  @Override
  protected void finalize()
      throws Throwable {
    super.finalize();
    close();
  }
}
