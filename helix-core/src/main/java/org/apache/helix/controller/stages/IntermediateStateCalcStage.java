package org.apache.helix.controller.stages;

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

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.*;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * For partition compute the Intermediate State (instance,state) pair based on the BestPossibleState
 * and CurrentState, with all constraints applied (such as state transition throttling).
 */
public class IntermediateStateCalcStage extends AbstractBaseStage {
  private static final Logger logger =
      LoggerFactory.getLogger(IntermediateStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE.name());
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());

    if (currentStateOutput == null || bestPossibleStateOutput == null || resourceMap == null
        || cache == null) {
      throw new StageException(String.format("Missing attributes in event: %s. "
              + "Requires CURRENT_STATE (%s) |BEST_POSSIBLE_STATE (%s) |RESOURCES (%s) |DataCache (%s)", event,
          currentStateOutput, bestPossibleStateOutput, resourceMap, cache));
    }

    IntermediateStateOutput intermediateStateOutput =
        compute(event, resourceMap, currentStateOutput, bestPossibleStateOutput);
    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), intermediateStateOutput);

    // Make sure no instance has more replicas/partitions assigned than maxPartitionPerInstance. If
    // it does, pause the rebalance and put the cluster on maintenance mode
    int maxPartitionPerInstance = cache.getClusterConfig().getMaxPartitionsPerInstance();
    if (maxPartitionPerInstance > 0) {
      validateMaxPartitionsPerInstance(event, cache, intermediateStateOutput,
          maxPartitionPerInstance);
    }
  }

  /**
   * Go through each resource, and based on BestPossibleState and CurrentState, compute
   * IntermediateState as close to BestPossibleState while maintaining throttling constraints (for
   * example, ensure that the number of possible pending state transitions does NOT go over the set
   * threshold).
   * @param event
   * @param resourceMap
   * @param currentStateOutput
   * @param bestPossibleStateOutput
   * @return
   */
  private IntermediateStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput) {
    IntermediateStateOutput output = new IntermediateStateOutput();
    ClusterDataCache dataCache = event.getAttribute(AttributeName.ClusterDataCache.name());

    StateTransitionThrottleController throttleController = new StateTransitionThrottleController(
        resourceMap.keySet(), dataCache.getClusterConfig(), dataCache.getLiveInstances().keySet());

    // Resource level prioritization based on the numerical (sortable) priority field.
    // If the resource priority field is null/not set, the resource will be treated as lowest
    // priority.
    List<ResourcePriority> prioritizedResourceList = new ArrayList<>();
    for (String resourceName : resourceMap.keySet()) {
      prioritizedResourceList.add(new ResourcePriority(resourceName, Integer.MIN_VALUE));
    }
    // If resourcePriorityField is null at the cluster level, all resources will be considered equal
    // in priority by keeping all priorities at MIN_VALUE
    if (dataCache.getClusterConfig().getResourcePriorityField() != null) {
      String priorityField = dataCache.getClusterConfig().getResourcePriorityField();
      for (ResourcePriority resourcePriority : prioritizedResourceList) {
        String resourceName = resourcePriority.getResourceName();

        // Will take the priority from ResourceConfig first
        // If ResourceConfig does not exist or does not have this field.
        // Try to load it from the resource's IdealState. Otherwise, keep it at the lowest priority
        if (dataCache.getResourceConfig(resourceName) != null
            && dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField) != null) {
          resourcePriority.setPriority(
              dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField));
        } else if (dataCache.getIdealState(resourceName) != null && dataCache
            .getIdealState(resourceName).getRecord().getSimpleField(priorityField) != null) {
          resourcePriority.setPriority(
              dataCache.getIdealState(resourceName).getRecord().getSimpleField(priorityField));
        }
      }
      Collections.sort(prioritizedResourceList, new ResourcePriorityComparator());
    }

    // Re-load ClusterStatusMonitor MBean
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());

    // Priority is applied in assignment computation because higher priority by looping in order of
    // decreasing priority
    for (ResourcePriority resourcePriority : prioritizedResourceList) {
      String resourceName = resourcePriority.getResourceName();
      Resource resource = resourceMap.get(resourceName);
      IdealState idealState = dataCache.getIdealState(resourceName);
      if (idealState == null) {
        // If IdealState is null, use an empty one
        logger.info("IdealState for resource {} does not exist; resource may not exist anymore",
            resourceName);
        idealState = new IdealState(resourceName);
        idealState.setStateModelDefRef(resource.getStateModelDefRef());
      }
      PartitionStateMap intermediatePartitionStateMap = computeIntermediatePartitionState(dataCache,
          clusterStatusMonitor, idealState, resourceMap.get(resourceName), currentStateOutput,
          bestPossibleStateOutput.getPartitionStateMap(resourceName),
          bestPossibleStateOutput.getPreferenceLists(resourceName), throttleController);
      output.setState(resourceName, intermediatePartitionStateMap);
    }
    return output;
  }

  /**
   * Go through every instance in the assignment and check that each instance does NOT have more
   * replicas for partitions assigned to it than maxPartitionsPerInstance. If the assignment
   * violates this, put the cluster on maintenance mode.
   * This logic could be integrated with compute() for IntermediateState calculation but appended
   * separately for visibility and testing. Additionally, performing validation after compute()
   * ensures that we have a full intermediate state mapping complete prior to validation.
   * @param event
   * @param cache
   * @param intermediateStateOutput
   * @param maxPartitionPerInstance
   */
  private void validateMaxPartitionsPerInstance(ClusterEvent event, ClusterDataCache cache,
      IntermediateStateOutput intermediateStateOutput, int maxPartitionPerInstance) {
    Map<String, PartitionStateMap> resourceStatesMap =
        intermediateStateOutput.getResourceStatesMap();
    Map<String, Integer> instancePartitionCounts = new HashMap<>();

    for (String resource : resourceStatesMap.keySet()) {
      IdealState idealState = cache.getIdealState(resource);
      if (idealState != null
          && idealState.getStateModelDefRef().equals(BuiltInStateModelDefinitions.Task.name())) {
        // Ignore task here. Task has its own throttling logic
        continue;
      }

      PartitionStateMap partitionStateMap = resourceStatesMap.get(resource);
      Map<Partition, Map<String, String>> stateMaps = partitionStateMap.getStateMap();
      for (Partition p : stateMaps.keySet()) {
        Map<String, String> stateMap = stateMaps.get(p);
        for (String instance : stateMap.keySet()) {
          // If this replica is in DROPPED state, do not count it in the partition count since it is
          // to be dropped
          String state = stateMap.get(instance);
          if (state.equals(HelixDefinedState.DROPPED.name())) {
            continue;
          }

          if (!instancePartitionCounts.containsKey(instance)) {
            instancePartitionCounts.put(instance, 0);
          }
          int partitionCount = instancePartitionCounts.get(instance); // Number of replicas (from
          // different partitions) held
          // in this instance
          partitionCount++;
          if (partitionCount > maxPartitionPerInstance) {
            HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
            String errMsg = String.format(
                "Problem: according to this assignment, instance %s contains more "
                    + "replicas/partitions than the maximum number allowed (%d). Pipeline will "
                    + "stop the rebalance and pause the cluster %s",
                instance, maxPartitionPerInstance, cache.getClusterName());
            if (manager != null) {
              manager.getClusterManagmentTool().enableMaintenanceMode(manager.getClusterName(),
                  true, errMsg);
            } else {
              logger.error(
                  "HelixManager is not set/null! Failed to pause this cluster/enable maintenance"
                      + " mode due to an instance being assigned more replicas/partitions than "
                      + "the limit.");
            }
            throw new HelixException(errMsg);
          }
          instancePartitionCounts.put(instance, partitionCount);
        }
      }
    }
  }

  /**
   * Compute intermediate partition states for a prioritized resource.
   * @param cache
   * @param clusterStatusMonitor
   * @param idealState
   * @param resource
   * @param currentStateOutput
   * @param bestPossiblePartitionStateMap
   * @param preferenceLists
   * @param throttleController
   * @return
   */
  private PartitionStateMap computeIntermediatePartitionState(ClusterDataCache cache,
      ClusterStatusMonitor clusterStatusMonitor, IdealState idealState, Resource resource,
      CurrentStateOutput currentStateOutput, PartitionStateMap bestPossiblePartitionStateMap,
      Map<String, List<String>> preferenceLists,
      StateTransitionThrottleController throttleController) {
    String resourceName = resource.getResourceName();
    logger.debug("Processing resource: {}", resourceName);

    // Throttling is applied only on FULL-AUTO mode
    if (!throttleController.isThrottleEnabled()
        || !IdealState.RebalanceMode.FULL_AUTO.equals(idealState.getRebalanceMode())
        || cache.isTaskCache()) {
      return bestPossiblePartitionStateMap;
    }

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    PartitionStateMap intermediatePartitionStateMap = new PartitionStateMap(resourceName);

    Set<Partition> partitionsNeedRecovery = new HashSet<>();
    Set<Partition> partitionsNeedLoadBalance = new HashSet<>();
    Set<Partition> partitionsWithErrorStateReplica = new HashSet<>();
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      Map<String, String> bestPossibleMap =
          bestPossiblePartitionStateMap.getPartitionMap(partition);
      List<String> preferenceList = preferenceLists.get(partition.getPartitionName());

      RebalanceType rebalanceType = getRebalanceType(cache, bestPossibleMap, preferenceList,
          stateModelDef, currentStateMap, idealState);

      // TODO: refine getRebalanceType to return more accurate rebalance types. So the following
      // logic doesn't need to check for more details.
      boolean isRebalanceNeeded = false;

      // Number of states required by StateModelDefinition are not satisfied, need recovery
      if (rebalanceType.equals(RebalanceType.RECOVERY_BALANCE)) {
        // Check whether partition is in ERROR state
        if (currentStateMap.values().contains(HelixDefinedState.ERROR.name())) {
          partitionsWithErrorStateReplica.add(partition);
        }
        // Check if recovery is needed for this partition
        if (!currentStateMap.equals(bestPossibleMap)) {
          partitionsNeedRecovery.add(partition);
          isRebalanceNeeded = true;
        }
      } else if (rebalanceType.equals(RebalanceType.LOAD_BALANCE)) {
        // Number of states required by StateModelDefinition are satisfied, but to achieve
        // BestPossibleState, need load balance
        partitionsNeedLoadBalance.add(partition);
        isRebalanceNeeded = true;
      }
      // Currently at BestPossibleState, no further action necessary
      if (!isRebalanceNeeded) {
        Map<String, String> intermediateMap = new HashMap<>(bestPossibleMap);
        intermediatePartitionStateMap.setState(partition, intermediateMap);
      }
    }

    if (!partitionsNeedRecovery.isEmpty()) {
      logger.info("Recovery balance needed for {} partitions: {}", resourceName,
          partitionsNeedRecovery);
    }
    if (!partitionsNeedLoadBalance.isEmpty()) {
      logger.info("Load balance needed for partitions: {}", resourceName,
          partitionsNeedLoadBalance);
    }
    if (!partitionsWithErrorStateReplica.isEmpty()) {
      logger.info("Partition currently has an ERROR replica in {} partitions: {}", resourceName,
          partitionsWithErrorStateReplica);
    }

    chargePendingTransition(resource, currentStateOutput, throttleController,
        partitionsNeedRecovery, partitionsNeedLoadBalance);

    // Perform recovery balance
    Set<Partition> recoveryThrottledPartitions =
        recoveryRebalance(resource, bestPossiblePartitionStateMap, throttleController,
            intermediatePartitionStateMap, partitionsNeedRecovery, currentStateOutput,
            cache.getStateModelDef(resource.getStateModelDefRef()).getTopState());

    // Perform load balance upon checking conditions below
    Set<Partition> loadbalanceThrottledPartitions = partitionsNeedLoadBalance;
    ClusterConfig clusterConfig = cache.getClusterConfig();

    // If the threshold (ErrorOrRecovery) is set, then use it, if not, then check if the old
    // threshold (Error) is set. If the old threshold is set, use it. If not, use the default value
    // for the new one. This is for backward-compatibility
    int threshold = 1; // Default threshold for ErrorOrRecoveryPartitionThresholdForLoadBalance
    int partitionCount = partitionsWithErrorStateReplica.size();
    if (clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance() != -1) {
      // ErrorOrRecovery is set
      threshold = clusterConfig.getErrorOrRecoveryPartitionThresholdForLoadBalance();
      partitionCount += partitionsNeedRecovery.size(); // Only add this count when the threshold is set
    } else {
      if (clusterConfig.getErrorPartitionThresholdForLoadBalance() != 0) {
        // 0 is the default value so the old threshold has been set
        threshold = clusterConfig.getErrorPartitionThresholdForLoadBalance();
      }
    }

    // Perform load balance only if the number of partitions in recovery and in error is less than
    // the threshold
    if (partitionCount < threshold) {
      loadbalanceThrottledPartitions = loadRebalance(resource, currentStateOutput,
          bestPossiblePartitionStateMap, throttleController, intermediatePartitionStateMap,
          partitionsNeedLoadBalance, currentStateOutput.getCurrentStateMap(resourceName));
    } else {
      // Only allow dropping of replicas to happen (dropping does NOT need to be throttled) and skip
      // load balance for this cycle
      for (Partition partition : partitionsNeedLoadBalance) {
        Map<String, String> currentStateMap =
            currentStateOutput.getCurrentStateMap(resourceName, partition);
        Map<String, String> bestPossibleMap =
            bestPossiblePartitionStateMap.getPartitionMap(partition);
        // Skip load balance by passing current state to intermediate state
        intermediatePartitionStateMap.setState(partition, currentStateMap);

        // Check if this partition only has downward state transitions; if so, allow state
        // transitions by setting it at bestPossibleState
        if (isLoadBalanceDownwardForAllReplicas(currentStateMap, bestPossibleMap, stateModelDef)) {
          intermediatePartitionStateMap.setState(partition, bestPossibleMap);
        }
      }
    }

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.updateRebalancerStats(resourceName, partitionsNeedRecovery.size(),
          partitionsNeedLoadBalance.size(), recoveryThrottledPartitions.size(),
          loadbalanceThrottledPartitions.size());
    }

    if (logger.isDebugEnabled()) {
      logPartitionMapState(resourceName, new HashSet<>(resource.getPartitions()),
          partitionsNeedRecovery, recoveryThrottledPartitions, partitionsNeedLoadBalance,
          loadbalanceThrottledPartitions, currentStateOutput, bestPossiblePartitionStateMap,
          intermediatePartitionStateMap);
    }

    logger.debug("End processing resource: {}", resourceName);
    return intermediatePartitionStateMap;
  }

  /**
   * Check for a partition, whether all transitions for its replicas are downward transitions. Note
   * that this function does NOT check for ERROR states.
   * @param currentStateMap
   * @param bestPossibleMap
   * @param stateModelDef
   * @return true if there are; false otherwise
   */
  private boolean isLoadBalanceDownwardForAllReplicas(Map<String, String> currentStateMap,
      Map<String, String> bestPossibleMap, StateModelDefinition stateModelDef) {
    Set<String> allInstances = new HashSet<>();
    allInstances.addAll(currentStateMap.keySet());
    allInstances.addAll(bestPossibleMap.keySet());
    Map<String, Integer> statePriorityMap = stateModelDef.getStatePriorityMap();

    for (String instance : allInstances) {
      String currentState = currentStateMap.get(instance);
      String bestPossibleState = bestPossibleMap.get(instance);
      if (currentState == null) {
        return false; // null -> state is upward
      }
      if (bestPossibleState != null) {
        // Compare priority values and return if an upward transition is found
        // Note that lower integer value implies higher priority
        if (statePriorityMap.get(currentState) > statePriorityMap.get(bestPossibleState)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Check and charge all pending transitions for throttling.
   */
  private void chargePendingTransition(Resource resource, CurrentStateOutput currentStateOutput,
      StateTransitionThrottleController throttleController, Set<Partition> partitionsNeedRecovery,
      Set<Partition> partitionsNeedLoadbalance) {
    String resourceName = resource.getResourceName();

    // check and charge pending transitions
    for (Partition partition : resource.getPartitions()) {
      // Maps instance to its current state
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      // Maps instance to its pending (next) state
      Map<String, String> pendingMap =
          currentStateOutput.getPendingStateMap(resourceName, partition);

      StateTransitionThrottleConfig.RebalanceType rebalanceType = RebalanceType.NONE;
      if (partitionsNeedRecovery.contains(partition)) {
        rebalanceType = StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE;
      } else if (partitionsNeedLoadbalance.contains(partition)) {
        rebalanceType = StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE;
      }

      if (pendingMap.size() > 0) {
        throttleController.chargeCluster(rebalanceType);
        throttleController.chargeResource(rebalanceType, resourceName);

        // charge each instance.
        for (String instance : pendingMap.keySet()) {
          String currentState = currentStateMap.get(instance);
          String pendingState = pendingMap.get(instance);
          if (pendingState != null && !pendingState.equals(currentState)) {
            throttleController.chargeInstance(rebalanceType, instance);
          }
        }
      }
    }
  }

  /**
   * Sort partitions according to partition priority {@link PartitionPriorityComparator}, and for
   * each partition, throttle state transitions if needed. Also populate
   * intermediatePartitionStateMap either with BestPossibleState (if no throttling is necessary) or
   * CurrentState (if throttled).
   * @param resource
   * @param bestPossiblePartitionStateMap
   * @param throttleController
   * @param intermediatePartitionStateMap
   * @param partitionsNeedRecovery
   * @param currentStateOutput
   * @param topState
   * @return a set of partitions that need recovery but did not get recovered due to throttling
   */
  private Set<Partition> recoveryRebalance(Resource resource,
      PartitionStateMap bestPossiblePartitionStateMap,
      StateTransitionThrottleController throttleController,
      PartitionStateMap intermediatePartitionStateMap, Set<Partition> partitionsNeedRecovery,
      CurrentStateOutput currentStateOutput, String topState) {
    String resourceName = resource.getResourceName();
    Set<Partition> partitionRecoveryBalanceThrottled = new HashSet<>();

    // Maps Partition -> Instance -> State
    Map<Partition, Map<String, String>> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName);
    List<Partition> partitionsNeedRecoveryPrioritized = new ArrayList<>(partitionsNeedRecovery);

    // TODO: Remove this sort by partition name when Java 1.8 is used
    // We want the result of the intermediate state calculation to be deterministic. We sort here by
    // partition name to ensure that the order is consistent for inputs fed into
    // PartitionPriorityComparator sort
    Collections.sort(partitionsNeedRecoveryPrioritized, new Comparator<Partition>() {
      @Override
      public int compare(Partition partition1, Partition partition2) {
        return partition1.getPartitionName().compareTo(partition2.getPartitionName());
      }
    });
    Collections.sort(partitionsNeedRecoveryPrioritized, new PartitionPriorityComparator(
        bestPossiblePartitionStateMap.getStateMap(), currentStateMap, topState, true));

    // For each partition, apply throttling if needed.
    for (Partition partition : partitionsNeedRecoveryPrioritized) {
      throttleStateTransitionsForPartition(throttleController, resourceName, partition,
          currentStateOutput, bestPossiblePartitionStateMap, partitionRecoveryBalanceThrottled,
          intermediatePartitionStateMap, RebalanceType.RECOVERY_BALANCE);
    }
    logger.info(String.format(
        "For resource %s: Num of partitions needing recovery: %d, Num of partitions needing recovery"
            + " but throttled (not recovered): %d",
        resourceName, partitionsNeedRecovery.size(), partitionRecoveryBalanceThrottled.size()));
    return partitionRecoveryBalanceThrottled;
  }

  /**
   * Sort partitions according to partition priority {@link PartitionPriorityComparator}, and for
   * each partition, throttle state transitions if needed. Also populate
   * intermediatePartitionStateMap either with BestPossibleState (if no throttling is necessary) or
   * CurrentState (if throttled).
   * @param resource
   * @param currentStateOutput
   * @param bestPossiblePartitionStateMap
   * @param throttleController
   * @param intermediatePartitionStateMap
   * @param partitionsNeedLoadbalance
   * @param currentStateMap
   * @return a set of partitions that need to be load-balanced but did not due to throttling
   */
  private Set<Partition> loadRebalance(Resource resource, CurrentStateOutput currentStateOutput,
      PartitionStateMap bestPossiblePartitionStateMap,
      StateTransitionThrottleController throttleController,
      PartitionStateMap intermediatePartitionStateMap, Set<Partition> partitionsNeedLoadbalance,
      Map<Partition, Map<String, String>> currentStateMap) {
    String resourceName = resource.getResourceName();
    Set<Partition> partitionsLoadbalanceThrottled = new HashSet<>();

    List<Partition> partitionsNeedLoadRebalancePrioritized =
        new ArrayList<>(partitionsNeedLoadbalance);

    // TODO: Remove this sort by partition name when Java 1.8 is used
    // We want the result of the intermediate state calculation to be deterministic. We sort here by
    // partition name to ensure that the order is consistent for inputs fed into
    // PartitionPriorityComparator sort
    Collections.sort(partitionsNeedLoadRebalancePrioritized, new Comparator<Partition>() {
      @Override
      public int compare(Partition partition1, Partition partition2) {
        return partition1.getPartitionName().compareTo(partition2.getPartitionName());
      }
    });
    Collections.sort(partitionsNeedLoadRebalancePrioritized, new PartitionPriorityComparator(
        bestPossiblePartitionStateMap.getStateMap(), currentStateMap, "", false));
    for (Partition partition : partitionsNeedLoadRebalancePrioritized) {
      throttleStateTransitionsForPartition(throttleController, resourceName, partition,
          currentStateOutput, bestPossiblePartitionStateMap, partitionsLoadbalanceThrottled,
          intermediatePartitionStateMap, RebalanceType.LOAD_BALANCE);
    }
    logger.info(String.format(
        "For resource %s: Num of partitions needing load-balance: %d, Num of partitions needing"
            + " load-balance but throttled (not load-balanced): %d",
        resourceName, partitionsNeedLoadbalance.size(), partitionsLoadbalanceThrottled.size()));
    return partitionsLoadbalanceThrottled;
  }

  /**
   * Check the status on throttling at every level (cluster, resource, instance) and set
   * intermediatePartitionStateMap accordingly per partition.
   * @param throttleController
   * @param resourceName
   * @param partition
   * @param currentStateOutput
   * @param bestPossiblePartitionStateMap
   * @param partitionsThrottled
   * @param intermediatePartitionStateMap
   * @param rebalanceType
   */
  private void throttleStateTransitionsForPartition(
      StateTransitionThrottleController throttleController, String resourceName,
      Partition partition, CurrentStateOutput currentStateOutput,
      PartitionStateMap bestPossiblePartitionStateMap, Set<Partition> partitionsThrottled,
      PartitionStateMap intermediatePartitionStateMap, RebalanceType rebalanceType) {

    Map<String, String> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName, partition);
    Map<String, String> bestPossibleMap = bestPossiblePartitionStateMap.getPartitionMap(partition);
    Set<String> allInstances = new HashSet<>(currentStateMap.keySet());
    allInstances.addAll(bestPossibleMap.keySet());
    Map<String, String> intermediateMap = new HashMap<>();

    boolean hasReachedThrottlingLimit = false;
    if (throttleController.shouldThrottleForResource(rebalanceType, resourceName)) {
      hasReachedThrottlingLimit = true;
      if (logger.isDebugEnabled()) {
        logger.debug("Throttled on partition: {} in resource: {}", partition.getPartitionName(),
            resourceName);
      }
    } else {
      // throttle if any of the instances are not able to accept state transitions
      for (String instance : allInstances) {
        String currentState = currentStateMap.get(instance);
        String bestPossibleState = bestPossibleMap.get(instance);
        if (bestPossibleState != null && !bestPossibleState.equals(currentState)) {
          if (throttleController.shouldThrottleForInstance(rebalanceType, instance)) {
            hasReachedThrottlingLimit = true;
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Throttled because of instance: {} for partition: {} in resource: {}" + instance,
                  partition.getPartitionName(), resourceName);
            }
            break;
          }
        }
      }
    }
    if (!hasReachedThrottlingLimit) {
      // This implies that there is room for more pending states. Find
      // instances with a replica whose current state is different from BestPossibleState and
      // "charge" for it, and bestPossibleStates will become intermediate states
      intermediateMap.putAll(bestPossibleMap);
      for (String instance : allInstances) {
        String currentState = currentStateMap.get(instance);
        String bestPossibleState = bestPossibleMap.get(instance);
        if (bestPossibleState != null && !bestPossibleState.equals(currentState)) {
          throttleController.chargeInstance(rebalanceType, instance);
        }
      }
      throttleController.chargeCluster(rebalanceType);
      throttleController.chargeResource(rebalanceType, resourceName);
    } else {
      // No more room for more pending states; current states will just become intermediate states
      // Add this partition to a set of throttled partitions
      intermediateMap.putAll(currentStateMap);
      partitionsThrottled.add(partition);
    }
    intermediatePartitionStateMap.setState(partition, intermediateMap);
  }

  /**
   * For a partiton, given its preferenceList, bestPossibleState, and currentState, determine which
   * type of rebalance is needed to model IdealState's states defined by the state model definition.
   * @return RebalanceType needed to bring the replicas to idea states
   *         RECOVERY_BALANCE - not all required states (replicas) are available through all
   *         replicas
   *         NONE - current state matches the ideal state
   *         LOAD_BALANCE - although all replicas required exist, Helix needs to optimize the
   *         allocation
   */
  private RebalanceType getRebalanceType(ClusterDataCache cache,
      Map<String, String> bestPossibleMap, List<String> preferenceList,
      StateModelDefinition stateModelDef, Map<String, String> currentStateMap,
      IdealState idealState) {
    if (preferenceList == null) {
      preferenceList = Collections.emptyList();
    }

    int replica = idealState.getReplicaCount(preferenceList.size());
    Set<String> activeList = new HashSet<>(preferenceList);
    activeList.retainAll(cache.getEnabledLiveInstances());

    // For each state, check that this partition currently has the required number of that state as
    // required by StateModelDefinition.
    LinkedHashMap<String, Integer> expectedStateCountMap =
        stateModelDef.getStateCountMap(activeList.size(), replica); // StateModelDefinition's counts
    Map<String, Integer> currentStateCounts = StateModelDefinition.getStateCounts(currentStateMap); // Current
    // counts
    // Go through each state and compare counts
    for (String state : expectedStateCountMap.keySet()) {
      Integer expectedCount = expectedStateCountMap.get(state);
      Integer currentCount = currentStateCounts.get(state);
      expectedCount = expectedCount == null ? 0 : expectedCount;
      currentCount = currentCount == null ? 0 : currentCount;

      // If counts do not match up, this partition requires recovery
      if (currentCount < expectedCount) {
        // Recovery is not needed in cases where this partition just started, was dropped, or is in
        // error
        if (!state.equals(HelixDefinedState.DROPPED.name())
            && !state.equals(HelixDefinedState.ERROR.name())
            && !state.equals(stateModelDef.getInitialState())) {
          return RebalanceType.RECOVERY_BALANCE;
        }
      }
    }
    // No recovery needed, all expected replicas exist
    // Check if this partition is actually in the BestPossibleState
    if (currentStateMap.equals(bestPossibleMap)) {
      return RebalanceType.NONE; // No further action required
    } else {
      return RebalanceType.LOAD_BALANCE; // Required state counts are satisfied, but in order to
      // achieve BestPossibleState, load balance may be required
      // to shift replicas around
    }
  }

  /**
   * Log rebalancer metadata for debugging purposes.
   * @param resource
   * @param allPartitions
   * @param recoveryPartitions
   * @param recoveryThrottledPartitions
   * @param loadbalancePartitions
   * @param loadbalanceThrottledPartitions
   * @param currentStateOutput
   * @param bestPossibleStateMap
   * @param intermediateStateMap
   */
  private void logPartitionMapState(String resource, Set<Partition> allPartitions,
      Set<Partition> recoveryPartitions, Set<Partition> recoveryThrottledPartitions,
      Set<Partition> loadbalancePartitions, Set<Partition> loadbalanceThrottledPartitions,
      CurrentStateOutput currentStateOutput, PartitionStateMap bestPossibleStateMap,
      PartitionStateMap intermediateStateMap) {

    if (logger.isDebugEnabled()) {
      logger.debug("Partitions need recovery: {}\nPartitions get throttled on recovery: {}",
          recoveryPartitions, recoveryThrottledPartitions);
      logger.debug("Partitions need loadbalance: {}\nPartitions get throttled on load-balance: {}",
          loadbalancePartitions, loadbalanceThrottledPartitions);
    }

    for (Partition partition : allPartitions) {
      if (logger.isDebugEnabled()) {
        logger.debug(partition + ": Best possible map: {}",
            bestPossibleStateMap.getPartitionMap(partition));
        logger.debug(partition + ": Current State: {}",
            currentStateOutput.getCurrentStateMap(resource, partition));
        logger.debug(partition + ": Pending state: {}",
            currentStateOutput.getPendingMessageMap(resource, partition));
        logger.debug(partition + ": Intermediate state: {}",
            intermediateStateMap.getPartitionMap(partition));
      }
    }
  }

  /**
   * POJO that maps resource name to its priority represented by an integer.
   */
  private static class ResourcePriority {
    private String _resourceName;
    private int _priority;

    ResourcePriority(String resourceName, Integer priority) {
      _resourceName = resourceName;
      _priority = priority;
    }

    public int compareTo(ResourcePriority resourcePriority) {
      return Integer.compare(_priority, resourcePriority._priority);
    }

    public String getResourceName() {
      return _resourceName;
    }

    public void setPriority(String priority) {
      try {
        _priority = Integer.parseInt(priority);
      } catch (Exception e) {
        logger.warn(
            String.format("Invalid priority field %s for resource %s", priority, _resourceName));
      }
    }
  }

  private static class ResourcePriorityComparator implements Comparator<ResourcePriority> {
    @Override
    public int compare(ResourcePriority priority1, ResourcePriority priority2) {
      return priority2.compareTo(priority1);
    }
  }

  // Compare partitions according following standard:
  // 1) Partition without top state always is the highest priority.
  // 2) For partition with top-state, the more number of active replica it has, the less priority.
  private class PartitionPriorityComparator implements Comparator<Partition> {
    private Map<Partition, Map<String, String>> _bestPossibleMap;
    private Map<Partition, Map<String, String>> _currentStateMap;
    private String _topState;
    private boolean _recoveryRebalance;

    PartitionPriorityComparator(Map<Partition, Map<String, String>> bestPossibleMap,
        Map<Partition, Map<String, String>> currentStateMap, String topState,
        boolean recoveryRebalance) {
      _bestPossibleMap = bestPossibleMap;
      _currentStateMap = currentStateMap;
      _topState = topState;
      _recoveryRebalance = recoveryRebalance;
    }

    @Override
    public int compare(Partition p1, Partition p2) {
      if (_recoveryRebalance) {
        int missTopState1 = getMissTopStateIndex(p1);
        int missTopState2 = getMissTopStateIndex(p2);
        // Highest priority for the partition without top state
        if (missTopState1 != missTopState2) {
          return Integer.compare(missTopState1, missTopState2);
        }
        // Higher priority for the partition with fewer active replicas
        int currentActiveReplicas1 = getCurrentActiveReplicas(p1);
        int currentActiveReplicas2 = getCurrentActiveReplicas(p2);
        return Integer.compare(currentActiveReplicas1, currentActiveReplicas2);
      }
      // Higher priority for the partition with fewer replicas with states matching with IdealState
      int idealStateMatched1 = getIdealStateMatched(p1);
      int idealStateMatched2 = getIdealStateMatched(p2);
      return Integer.compare(idealStateMatched1, idealStateMatched2);
    }

    private int getMissTopStateIndex(Partition partition) {
      // 0 if no replicas in top-state, 1 if it has at least one replica in top-state.
      if (!_currentStateMap.containsKey(partition)
          || !_currentStateMap.get(partition).values().contains(_topState)) {
        return 0;
      }
      return 1;
    }

    private int getCurrentActiveReplicas(Partition partition) {
      int currentActiveReplicas = 0;
      if (!_currentStateMap.containsKey(partition)) {
        return currentActiveReplicas;
      }
      // Initialize state -> number of this state map
      Map<String, Integer> stateCountMap = new HashMap<>();
      for (String state : _bestPossibleMap.get(partition).values()) {
        if (!stateCountMap.containsKey(state)) {
          stateCountMap.put(state, 0);
        }
        stateCountMap.put(state, stateCountMap.get(state) + 1);
      }
      // Search the state map
      for (String state : _currentStateMap.get(partition).values()) {
        if (stateCountMap.containsKey(state) && stateCountMap.get(state) > 0) {
          currentActiveReplicas++;
          stateCountMap.put(state, stateCountMap.get(state) - 1);
        }
      }
      return currentActiveReplicas;
    }

    private int getIdealStateMatched(Partition partition) {
      int matchedState = 0;
      if (!_currentStateMap.containsKey(partition)) {
        return matchedState;
      }
      for (String instance : _bestPossibleMap.get(partition).keySet()) {
        if (_bestPossibleMap.get(partition).get(instance)
            .equals(_currentStateMap.get(partition).get(instance))) {
          matchedState++;
        }
      }
      return matchedState;
    }
  }
}