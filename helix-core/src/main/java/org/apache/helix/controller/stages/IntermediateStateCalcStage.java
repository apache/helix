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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.api.config.StateTransitionThrottleConfig.RebalanceType;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.log4j.Logger;

/**
 * For partition compute the Intermediate State (instance,state) pair based on the BestPossible
 * State and Current State, with all constraints applied (such as state transition throttling).
 */
public class IntermediateStateCalcStage extends AbstractBaseStage {
  private static final Logger logger = Logger.getLogger(IntermediateStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    logger.info("START Intermediate.process()");

    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());

    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());

    if (currentStateOutput == null || bestPossibleStateOutput == null || resourceMap == null
        || cache == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|BEST_POSSIBLE_STATE|RESOURCES|DataCache");
    }

    IntermediateStateOutput intermediateStateOutput =
        compute(event, resourceMap, currentStateOutput, bestPossibleStateOutput);
    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), intermediateStateOutput);

    // Check whether any instance in the cluster could be assigned more partitions than allowed, if yes, pause the rebalancer.
    int maxPartitionPerInstance = cache.getClusterConfig().getMaxPartitionsPerInstance();
    if (maxPartitionPerInstance > 0) {
      validateMaxPartitionsPerInstance(event, cache, intermediateStateOutput,
          maxPartitionPerInstance);
    }

    long endTime = System.currentTimeMillis();
    logger.info(
        "END ImmediateStateCalcStage.process() for cluster " + cache.getClusterName() + ". took: "
            + (endTime - startTime) + " ms");
  }

  private IntermediateStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleStateOutput) {
    // for each resource
    // get the best possible state and current state
    // try to bring immediate state close to best possible state until
    // the possible pending state transition numbers reach the set throttle number.
    IntermediateStateOutput output = new IntermediateStateOutput();
    ClusterDataCache dataCache = event.getAttribute(AttributeName.ClusterDataCache.name());

    StateTransitionThrottleController throttleController =
        new StateTransitionThrottleController(resourceMap.keySet(), dataCache.getClusterConfig(),
            dataCache.getLiveInstances().keySet());

    // Resource level prioritization with numerical sortable field.
    // If no value has been set, it will be treated as lowest priority.
    List<ResourcePriority> prioritizedResourceList = new ArrayList<ResourcePriority>();
    for (String resourceName : resourceMap.keySet()) {
      prioritizedResourceList.add(new ResourcePriority(resourceName, Integer.MIN_VALUE));
    }
    // Not have resource level prioritization if user did not set the field name
    if (dataCache.getClusterConfig().getResourcePriorityField() != null) {
      String priorityField = dataCache.getClusterConfig().getResourcePriorityField();

      for (ResourcePriority resourcePriority : prioritizedResourceList) {
        String resourceName = resourcePriority.getResourceName();

        // Will take the priority from ResourceConfig first
        // If ResourceConfig does not exist or does not have this field.
        // Try to fetch it from ideal state. Otherwise will treated as lowest priority
        if (dataCache.getResourceConfig(resourceName) != null
            && dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField) != null) {
          resourcePriority.setPriority(
              dataCache.getResourceConfig(resourceName).getSimpleConfig(priorityField));
        } else if (dataCache.getIdealState(resourceName) != null
            && dataCache.getIdealState(resourceName).getRecord().getSimpleField(priorityField)
            != null) {

          resourcePriority.setPriority(
              dataCache.getIdealState(resourceName).getRecord().getSimpleField(priorityField));
        }
      }

      Collections.sort(prioritizedResourceList, new ResourcePriortiyComparator());
    }

    // Update cluster status monitor mbean
    ClusterStatusMonitor clusterStatusMonitor = event.getAttribute(AttributeName.clusterStatusMonitor.name());

    for (ResourcePriority resourcePriority : prioritizedResourceList) {
      String resourceName = resourcePriority.getResourceName();
      Resource resource = resourceMap.get(resourceName);
      IdealState idealState = dataCache.getIdealState(resourceName);

      if (idealState == null) {
        // if ideal state is deleted, use an empty one
        logger.info("resource:" + resourceName + " does not exist anymore");
        idealState = new IdealState(resourceName);
        idealState.setStateModelDefRef(resource.getStateModelDefRef());
      }

      PartitionStateMap intermediatePartitionStateMap =
          computeIntermediatePartitionState(dataCache, clusterStatusMonitor, idealState,
              resourceMap.get(resourceName), currentStateOutput,
              bestPossibleStateOutput.getPartitionStateMap(resourceName),
              bestPossibleStateOutput.getPreferenceLists(resourceName), throttleController);
      output.setState(resourceName, intermediatePartitionStateMap);
    }
    return output;
  }

  private void validateMaxPartitionsPerInstance(ClusterEvent event, ClusterDataCache cache,
      IntermediateStateOutput intermediateStateOutput, int maxPartitionPerInstance) {
    Map<String, PartitionStateMap> resourceStatesMap =
        intermediateStateOutput.getResourceStatesMap();
    Map<String, Integer> instancePartitionCounts = new HashMap<>();

    for (String resource : resourceStatesMap.keySet()) {
      IdealState idealState = cache.getIdealState(resource);
      if (idealState != null && idealState.getStateModelDefRef()
          .equals(BuiltInStateModelDefinitions.Task.name())) {
        // ignore task here. Task has its own throttling logic
        continue;
      }

      PartitionStateMap partitionStateMap = resourceStatesMap.get(resource);
      Map<Partition, Map<String, String>> stateMaps = partitionStateMap.getStateMap();
      for (Partition p : stateMaps.keySet()) {
        Map<String, String> stateMap = stateMaps.get(p);
        for (String instance : stateMap.keySet()) {
          //ignore replica to be dropped.
          String state = stateMap.get(instance);
          if (state.equals(HelixDefinedState.DROPPED.name())) {
            continue;
          }

          if (!instancePartitionCounts.containsKey(instance)) {
            instancePartitionCounts.put(instance, 0);
          }
          int partitionCount = instancePartitionCounts.get(instance);
          partitionCount++;
          if (partitionCount > maxPartitionPerInstance) {
            HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
            String errMsg = String.format(
                "Partition count to be assigned to instance %s is greater than %d. Stop rebalance and pause the cluster %s",
                instance, maxPartitionPerInstance, cache.getClusterName());
            if (manager != null) {
              manager.getClusterManagmentTool()
                  .enableCluster(manager.getClusterName(), false, errMsg);
            } else {
              logger.error("Failed to pause cluster, HelixManager is not set!");
            }
            throw new HelixException(errMsg);
          }
          instancePartitionCounts.put(instance, partitionCount);
        }
      }
    }
  }

  private PartitionStateMap computeIntermediatePartitionState(ClusterDataCache cache,
      ClusterStatusMonitor clusterStatusMonitor, IdealState idealState, Resource resource,
      CurrentStateOutput currentStateOutput, PartitionStateMap bestPossiblePartitionStateMap,
      Map<String, List<String>> preferenceLists,
      StateTransitionThrottleController throttleController) {
    String resourceName = resource.getResourceName();
    logger.debug("Processing resource:" + resourceName);

    if (!throttleController.isThrottleEnabled() || !IdealState.RebalanceMode.FULL_AUTO
        .equals(idealState.getRebalanceMode())) {
      // We only apply throttling on FULL-AUTO now.
      return bestPossiblePartitionStateMap;
    }

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);

    PartitionStateMap intermediatePartitionStateMap = new PartitionStateMap(resourceName);

    Set<Partition> partitionsNeedRecovery = new HashSet<Partition>();
    Set<Partition> partitionsNeedLoadbalance = new HashSet<Partition>();
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      Map<String, String> bestPossibleMap =
          bestPossiblePartitionStateMap.getPartitionMap(partition);
      List<String> preferenceList = preferenceLists.get(partition.getPartitionName());

      RebalanceType rebalanceType =
          getRebalanceType(cache, bestPossibleMap, preferenceList, stateModelDef, currentStateMap,
              idealState);
      if (rebalanceType.equals(RebalanceType.RECOVERY_BALANCE)) {
        partitionsNeedRecovery.add(partition);
      } else if (rebalanceType.equals(RebalanceType.LOAD_BALANCE)){
        partitionsNeedLoadbalance.add(partition);
      } else {
        // no rebalance needed.
        Map<String, String> intermediateMap = new HashMap<String, String>(bestPossibleMap);
        intermediatePartitionStateMap.setState(partition, intermediateMap);
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "recovery balance needed for " + resource + " partitions: " + partitionsNeedRecovery);
      logger.debug(
          "load balance needed for " + resource + " partitions: " + partitionsNeedLoadbalance);
    }

    chargePendingTransition(resource, currentStateOutput, throttleController,
        partitionsNeedRecovery, partitionsNeedLoadbalance);

    // perform recovery rebalance
    int recoveryRebalanceThrottledCount =
    recoveryRebalance(resource, bestPossiblePartitionStateMap, throttleController,
        intermediatePartitionStateMap, partitionsNeedRecovery, currentStateOutput,
        cache.getStateModelDef(resource.getStateModelDefRef()).getTopState());

    int loadRebalanceThrottledCount = partitionsNeedLoadbalance.size();
    if (partitionsNeedRecovery.isEmpty()) {
      // perform load balance only if no partition need recovery rebalance.
      // TODO: to set a minimal threshold for allowing load-rebalance.
      loadRebalanceThrottledCount =
          loadRebalance(resource, currentStateOutput, bestPossiblePartitionStateMap,
              throttleController, intermediatePartitionStateMap, partitionsNeedLoadbalance,
              currentStateOutput.getCurrentStateMap(resourceName));
    } else {
      for (Partition p : partitionsNeedLoadbalance) {
        Map<String, String> currentStateMap =
            currentStateOutput.getCurrentStateMap(resourceName, p);
        intermediatePartitionStateMap.setState(p, currentStateMap);
      }
    }

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.updateRebalancerStats(resourceName, partitionsNeedRecovery.size(),
          partitionsNeedLoadbalance.size(), recoveryRebalanceThrottledCount,
          loadRebalanceThrottledCount);
    }

    logger.debug("End processing resource:" + resourceName);
    return intermediatePartitionStateMap;
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
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
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
        for (String ins : pendingMap.keySet()) {
          String currentState = currentStateMap.get(ins);
          String pendingState = pendingMap.get(ins);
          if (pendingState != null && !pendingState.equals(currentState)) {
            throttleController.chargeInstance(rebalanceType, ins);
          }
        }
      }
    }
  }

  /**
   *  Perform any recovery balance if needed, fill intermediatePartitionStateMap
   *  if recover rebalance is needed.
   *  return the number of partitions needs recoveryRebalance but get throttled
   */

  public int recoveryRebalance(Resource resource, PartitionStateMap bestPossiblePartitionStateMap,
      StateTransitionThrottleController throttleController,
      PartitionStateMap intermediatePartitionStateMap, Set<Partition> partitionsNeedRecovery,
      CurrentStateOutput currentStateOutput, String topState) {
    Set<Partition> partitionRecoveryBalanceThrottled = new HashSet<Partition>();

    Map<Partition, Map<String, String>> currentStateMap =
        currentStateOutput.getCurrentStateMap(resource.getResourceName());
    List<Partition> partitionsNeedRecoveryPrioritized =
        new ArrayList<Partition>(partitionsNeedRecovery);

    // TODO : Due to currently using JAVA 1.6, the original order of partitions list is not
    // determinable, sort the list by partition name and remove the code after bump to JAVA 1.8
    Collections.sort(partitionsNeedRecoveryPrioritized, new Comparator<Partition>() {
      @Override
      public int compare(Partition o1, Partition o2) {
        return o1.getPartitionName().compareTo(o2.getPartitionName());
      }
    });

    Collections.sort(partitionsNeedRecoveryPrioritized,
        new PartitionPriorityComparator(bestPossiblePartitionStateMap.getStateMap(),
            currentStateMap, topState, true));

    for (Partition partition : partitionsNeedRecoveryPrioritized) {
      throtteStateTransitions(throttleController, resource.getResourceName(), partition,
          currentStateOutput, bestPossiblePartitionStateMap, partitionRecoveryBalanceThrottled,
          intermediatePartitionStateMap, RebalanceType.RECOVERY_BALANCE);
    }

    logger.debug(String
        .format("needRecovery: %d, recoverybalanceThrottled: %d", partitionsNeedRecovery.size(),
            partitionRecoveryBalanceThrottled.size()));
    return partitionRecoveryBalanceThrottled.size();
  }

  /* return the number of partitions needs loadRebalance but get throttled */
  private int loadRebalance(Resource resource, CurrentStateOutput currentStateOutput,
      PartitionStateMap bestPossiblePartitionStateMap,
      StateTransitionThrottleController throttleController,
      PartitionStateMap intermediatePartitionStateMap, Set<Partition> partitionsNeedLoadbalance,
      Map<Partition, Map<String, String>> currentStateMaps) {
    String resourceName = resource.getResourceName();
    Set<Partition> partitionsLoadbalanceThrottled = new HashSet<Partition>();

    List<Partition> partitionsNeedLoadRebalancePrioritized =
        new ArrayList<Partition>(partitionsNeedLoadbalance);

    // TODO : Due to currently using JAVA 1.6, the original order of partitions list is not
    // determinable, sort the list by partition name and remove the code after bump to JAVA 1.8
    Collections.sort(partitionsNeedLoadRebalancePrioritized, new Comparator<Partition>() {
      @Override
      public int compare(Partition o1, Partition o2) {
        return o1.getPartitionName().compareTo(o2.getPartitionName());
      }
    });

    Collections.sort(partitionsNeedLoadRebalancePrioritized,
        new PartitionPriorityComparator(bestPossiblePartitionStateMap.getStateMap(),
            currentStateMaps, "", false));

    for (Partition partition : partitionsNeedLoadRebalancePrioritized) {
      throtteStateTransitions(throttleController, resourceName, partition, currentStateOutput,
          bestPossiblePartitionStateMap, partitionsLoadbalanceThrottled,
          intermediatePartitionStateMap, RebalanceType.LOAD_BALANCE);
    }

    logger.info(String
        .format("loadbalanceNeeded: %d, loadbalanceThrottled: %d", partitionsNeedLoadbalance.size(),
            partitionsLoadbalanceThrottled.size()));

    if (logger.isDebugEnabled()) {
      logger.debug("recovery balance throttled for " + resource + " partitions: "
          + partitionsLoadbalanceThrottled);
    }

    return partitionsLoadbalanceThrottled.size();
  }

  private void throtteStateTransitions(StateTransitionThrottleController throttleController,
      String resourceName, Partition partition, CurrentStateOutput currentStateOutput,
      PartitionStateMap bestPossiblePartitionStateMap, Set<Partition> partitionsThrottled,
      PartitionStateMap intermediatePartitionStateMap, RebalanceType rebalanceType) {

    Map<String, String> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName, partition);
    Map<String, String> bestPossibleMap = bestPossiblePartitionStateMap.getPartitionMap(partition);
    Set<String> allInstances = new HashSet<String>(currentStateMap.keySet());
    allInstances.addAll(bestPossibleMap.keySet());
    Map<String, String> intermediateMap = new HashMap<String, String>();

    boolean throttled = false;
    if (throttleController.throttleforResource(rebalanceType, resourceName)) {
      throttled = true;
      logger
          .debug("Throttled on resource for " + resourceName + " " + partition.getPartitionName());
    } else {
      // throttle if any of the instance can not handle the state transition
      for (String ins : allInstances) {
        String currentState = currentStateMap.get(ins);
        String bestPossibleState = bestPossibleMap.get(ins);
        if (bestPossibleState != null && !bestPossibleState.equals(currentState)) {
          if (throttleController.throttleForInstance(rebalanceType, ins)) {
            throttled = true;
            logger.debug(
                "Throttled because instance " + ins + " for " + resourceName + " " + partition
                    .getPartitionName());
          }
        }
      }
    }

    if (!throttled) {
      intermediateMap.putAll(bestPossibleMap);
      for (String ins : allInstances) {
        String currentState = currentStateMap.get(ins);
        String bestPossibleState = bestPossibleMap.get(ins);
        if (bestPossibleState != null && !bestPossibleState.equals(currentState)) {
          throttleController.chargeInstance(rebalanceType, ins);
        }
      }
      throttleController.chargeCluster(rebalanceType);
      throttleController.chargeResource(rebalanceType, resourceName);
    } else {
      intermediateMap.putAll(currentStateMap);
      partitionsThrottled.add(partition);
    }
    intermediatePartitionStateMap.setState(partition, intermediateMap);
  }

  /**
   * Given preferenceList, bestPossibleState and currentState, determine which type of rebalance is
   * needed.
   */
  private RebalanceType getRebalanceType(ClusterDataCache cache,
      Map<String, String> bestPossibleMap, List<String> preferenceList,
      StateModelDefinition stateModelDef, Map<String, String> currentStateMap, IdealState idealState) {
    if (currentStateMap.equals(bestPossibleMap)) {
      return RebalanceType.NONE;
    }

    if (preferenceList == null) {
      preferenceList = Collections.emptyList();
    }

    int replica = idealState.getReplicaCount(preferenceList.size());
    Set<String> activeList = new HashSet<String>(preferenceList);
    activeList.retainAll(cache.getEnabledLiveInstances());

    LinkedHashMap<String, Integer> bestPossileStateCountMap =
        getBestPossibleStateCountMap(stateModelDef, activeList.size(), replica);
    Map<String, Integer> currentStateCounts = getStateCounts(currentStateMap);

    for (String state : bestPossileStateCountMap.keySet()) {
      Integer bestPossibleCount = bestPossileStateCountMap.get(state);
      Integer currentCount = currentStateCounts.get(state);
      bestPossibleCount = bestPossibleCount == null? 0 : bestPossibleCount;
      currentCount = currentCount == null? 0 : currentCount;

      if (currentCount < bestPossibleCount) {
        if (!state.equals(HelixDefinedState.DROPPED.name()) &&
            !state.equals(HelixDefinedState.ERROR.name()) &&
            !state.equals(stateModelDef.getInitialState())) {
          return RebalanceType.RECOVERY_BALANCE;
        }
      }
    }
    return RebalanceType.LOAD_BALANCE;
  }

  private LinkedHashMap<String, Integer> getBestPossibleStateCountMap(
      StateModelDefinition stateModelDef, int candidateNodeNum, int totalReplicas) {
    LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<String, Integer>();
    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();

    int replicas = totalReplicas;
    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      if (candidateNodeNum <= 0) {
        break;
      }
      if ("N".equals(num)) {
        stateCountMap.put(state, candidateNodeNum);
        replicas -= candidateNodeNum;
        break;
      } else if ("R".equals(num)) {
        // wait until we get the counts for all other states
        continue;
      } else {
        int stateCount = -1;
        try {
          stateCount = Integer.parseInt(num);
        } catch (Exception e) {
        }

        if (stateCount > 0) {
          int count = stateCount <= candidateNodeNum ? stateCount : candidateNodeNum;
          candidateNodeNum -= count;
          stateCountMap.put(state, count);
          replicas -= count;
        }
      }
    }

    // get state count for R
    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      if ("R".equals(num)) {
        if (candidateNodeNum > 0 && replicas > 0) {
          stateCountMap.put(state, replicas < candidateNodeNum ? replicas : candidateNodeNum);
        }
        // should have at most one state using R
        break;
      }
    }
    return stateCountMap;
  }

  /* given instance->state map, return the state counts */
  private Map<String, Integer> getStateCounts(Map<String, String> stateMap) {
    Map<String, Integer> stateCounts = new HashMap<String, Integer>();
    for (String state : stateMap.values()) {
      if (!stateCounts.containsKey(state)) {
        stateCounts.put(state, 0);
      }
      stateCounts.put(state, stateCounts.get(state) + 1);
    }
    return stateCounts;
  }

  private void logParitionMapState(String resource, Set<Partition> allPartitions,
      Set<Partition> recoveryPartitions, Set<Partition> recoveryThrottledPartitions,
      Set<Partition> loadbalancePartitions, Set<Partition> loadbalanceThrottledPartitions,
      CurrentStateOutput currentStateOutput,
      PartitionStateMap bestPossibleStateMap,
      PartitionStateMap intermediateStateMap) {

    logger.debug("Partitions need recovery: " + recoveryPartitions
        + "\nPartitions get throttled on recovery: " + recoveryThrottledPartitions);
    logger.debug("Partitions need loadbalance: " + loadbalancePartitions
        + "\nPartitions get throttled on load-balance: " + loadbalanceThrottledPartitions);

    for (Partition partition : allPartitions) {
      if (recoveryPartitions.contains(partition)) {
        logger
            .debug("recovery balance needed for " + resource + " " + partition.getPartitionName());
        if (recoveryThrottledPartitions.contains(partition)) {
          logger.debug("Recovery balance throttled on resource for " + resource + " " + partition
              .getPartitionName());
        }
      } else if (loadbalancePartitions.contains(partition)) {
        logger.debug("load balance needed for " + resource + " " + partition.getPartitionName());
        if (loadbalanceThrottledPartitions.contains(partition)) {
          logger.debug("Load balance throttled on resource for " + resource + " " + partition
              .getPartitionName());
        }
      } else {
        logger.debug("no balance needed for " + resource + " " + partition.getPartitionName());
      }

      logger.debug(
          partition + ": Best possible map: " + bestPossibleStateMap.getPartitionMap(partition));
      logger.debug(partition + ": Current State: " + currentStateOutput
          .getCurrentStateMap(resource, partition));
      logger.debug(partition + ": Pending state: " + currentStateOutput
          .getPendingMessageMap(resource, partition));
      logger.debug(
          partition + ": Intermediate state: " + intermediateStateMap.getPartitionMap(partition));
    }
  }

  private static class ResourcePriortiyComparator implements Comparator<ResourcePriority> {
    @Override public int compare(ResourcePriority r1, ResourcePriority r2) {
      return r2.compareTo(r1);
    }
  }

  private static class ResourcePriority {
    private String _resourceName;
    private Integer _priority;

    public ResourcePriority(String resourceName, Integer priority) {
      _resourceName = resourceName;
      _priority = priority;
    }

    public int compareTo(ResourcePriority resourcePriority) {
      return this._priority.compareTo(resourcePriority._priority);
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

  //  Compare partitions according following standard:
  //  1) Partition without top state always is the highest priority.
  //  2) For partition with top-state, the more number of active replica it has, the less priority.
  private class PartitionPriorityComparator implements Comparator<Partition> {
    private Map<Partition, Map<String, String>> _bestPossibleMap;
    private Map<Partition, Map<String, String>> _currentStateMap;
    private String _topState;
    private boolean _recoveryRebalance;

    public PartitionPriorityComparator(Map<Partition, Map<String, String>> bestPossibleMap,
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
        Integer missTopState1 = getMissTopStateIndex(p1);
        Integer missTopState2 = getMissTopStateIndex(p2);

        // Highest priority for the partition without top state
        if (!missTopState1.equals(missTopState2)) {
          return missTopState1.compareTo(missTopState2);
        }

        Integer currentActiveReplicas1 = getCurrentActiveReplicas(p1);
        Integer currentActiveReplicas2 = getCurrentActiveReplicas(p2);
        return currentActiveReplicas1.compareTo(currentActiveReplicas2);
      }

      // Higher priority for the partition, which has less number of active replicas
      Integer idealStateMatched1 = getIdealStateMatched(p1);
      Integer idealStateMatched2 = getIdealStateMatched(p2);

      return idealStateMatched1.compareTo(idealStateMatched2);
    }

    private Integer getMissTopStateIndex(Partition partition) {
      // 0 if no replica in top-state, 1 if it has at least one replica in top-state.
      if (!_currentStateMap.containsKey(partition) || !_currentStateMap.get(partition).values()
          .contains(_topState)) {
        return 0;
      }
      return 1;
    }

    private Integer getCurrentActiveReplicas(Partition partition) {
      Integer currentActiveReplicas = 0;
      if (!_currentStateMap.containsKey(partition)) {
        return currentActiveReplicas;
      }

      // Initialize state -> number of this state map
      Map<String, Integer> stateCountMap = new HashMap<String, Integer>();
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

    private Integer getIdealStateMatched(Partition partition) {
      Integer matchedState = 0;
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
