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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * For partition compute the Intermediate State (instance,state) pair based on the BestPossible
 * State and Current State, with all constraints applied (such as state transition throttling).
 */
public class IntermediateStateCalcStage extends AbstractBaseStage {
  private static final Logger logger = Logger.getLogger(IntermediateStateCalcStage.class.getName());

  @Override public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    logger.info("START Intermediate.process()");

    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());

    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    if (currentStateOutput == null || bestPossibleStateOutput == null || resourceMap == null
        || cache == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|BEST_POSSIBLE_STATE|RESOURCES|DataCache");
    }

    IntermediateStateOutput immediateStateOutput =
        compute(cache, resourceMap, currentStateOutput, bestPossibleStateOutput);

    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), immediateStateOutput);

    long endTime = System.currentTimeMillis();
    logger.info("END ImmediateStateCalcStage.process(). took: " + (endTime - startTime) + " ms");
  }

  private IntermediateStateOutput compute(ClusterDataCache dataCache,
      Map<String, Resource> resourceMap, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleStateOutput) {
    // for each resource
    // get the best possible state and current state
    // try to bring immediate state close to best possible state until
    // the possible pending state transition numbers reach the set throttle number.
    IntermediateStateOutput output = new IntermediateStateOutput();

    StateTransitionThrottleController throttleController =
        new StateTransitionThrottleController(resourceMap.keySet(), dataCache.getClusterConfig(),
            dataCache.getLiveInstances().keySet());

    for (String resourceName : resourceMap.keySet()) {
      PartitionStateMap intermediatePartitionStateMap =
          computeIntermediatePartitionState(dataCache, dataCache.getIdealState(resourceName),
              resourceMap.get(resourceName), currentStateOutput,
              bestPossibleStateOutput.getPartitionStateMap(resourceName), throttleController);
      output.setState(resourceName, intermediatePartitionStateMap);
    }
    return output;
  }

  public PartitionStateMap computeIntermediatePartitionState(ClusterDataCache cache,
      IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput,
      PartitionStateMap bestPossiblePartitionStateMap,
      StateTransitionThrottleController throttleController) {
    String resourceName = resource.getResourceName();
    logger.info("Processing resource:" + resourceName);

    if (!throttleController.isThrottleEnabled()) {
      logger.info("None of any type of transition throttling is set for resource " + resourceName
          + " skip computing intermediate partition state.");
      return bestPossiblePartitionStateMap;
    }

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);

    boolean pendingRecoveryRebalance = false;

    // check and charge pending transitions
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      Map<String, String> pendingMap =
          currentStateOutput.getPendingStateMap(resourceName, partition);
      Map<String, String> bestPossibleMap =
          bestPossiblePartitionStateMap.getPartitionMap(partition);

      StateTransitionThrottleConfig.RebalanceType rebalanceType;
      if (needRecoveryRebalance(bestPossibleMap, stateModelDef, currentStateMap)) {
        rebalanceType = StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE;
        pendingRecoveryRebalance = true;
      } else {
        rebalanceType = StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE;
      }

      if (pendingMap.size() > 0) {
        throttleController.chargeCluster(rebalanceType);
        throttleController.chargeResource(rebalanceType, resourceName);
      }

      Set<String> allInstances = new HashSet<String>(currentStateMap.keySet());
      allInstances.addAll(pendingMap.keySet());

      for (String ins : allInstances) {
        String currentState = currentStateMap.get(ins);
        String pendingState = pendingMap.get(ins);
        if (pendingState != null && !pendingState.equals(currentState)) {
          throttleController.chargeInstance(rebalanceType, ins);
        }
      }
    }

    PartitionStateMap output = new PartitionStateMap(resourceName);

    int recoveryNeededCount = 0, recoveryThrottledCount = 0;
    int loadbalanceNeededCount = 0, loadbalanceThrottledCount = 0;

    Set<Partition> partitionsNeedRecovery = new HashSet<Partition>();
    Set<Partition> partitionsNeedLoadbalance = new HashSet<Partition>();
    Set<Partition> partitionsRecoveryThrotted = new HashSet<Partition>();
    Set<Partition> partitionsLoadbalanceThrottled = new HashSet<Partition>();

    // check recovery rebalance
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, partition);
      Map<String, String> bestPossibleMap =
          bestPossiblePartitionStateMap.getPartitionMap(partition);
      Map<String, String> intermediateMap = new HashMap<String, String>();

      if (currentStateMap.equals(bestPossibleMap)) {
        // no rebalance needed.
        intermediateMap.putAll(bestPossibleMap);
      } else if (needRecoveryRebalance(bestPossibleMap, stateModelDef, currentStateMap)) {
        //TODO: add throttling on recovery balance
        recoveryNeededCount++;
        intermediateMap.putAll(bestPossibleMap);
        pendingRecoveryRebalance = true;
        partitionsNeedRecovery.add(partition);
      } else {
        partitionsNeedLoadbalance.add(partition);
      }
      output.setState(partition, intermediateMap);
    }

    // perform load balance only if no partition need recovery rebalance.
    loadbalanceNeededCount = partitionsNeedLoadbalance.size();
    if (!pendingRecoveryRebalance) {
      for (Partition partition : partitionsNeedLoadbalance) {
        Map<String, String> currentStateMap =
            currentStateOutput.getCurrentStateMap(resourceName, partition);
        Map<String, String> bestPossibleMap =
            bestPossiblePartitionStateMap.getPartitionMap(partition);
        Map<String, String> intermediateMap = new HashMap<String, String>();

        Set<String> allInstances = new HashSet<String>(currentStateMap.keySet());
        allInstances.addAll(bestPossibleMap.keySet());

        boolean throttled = false;
        if (throttleController
            .throttleforResource(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
                resourceName)) {
          throttled = true;
          logger.debug("Load balance throttled on resource for " + resourceName + " " + partition
                  .getPartitionName());
        } else {
          // throttle the load balance if any of the instance can not handle the state transition
          // TODO: may need finer grained control here.
          for (String ins : allInstances) {
            String currentState = currentStateMap.get(ins);
            String bestPossibleState = bestPossibleMap.get(ins);
            if (bestPossibleState != null && !bestPossibleState.equals(currentState)) {
              if (throttleController
                  .throttleForInstance(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
                      ins)) {
                throttled = true;
                logger.debug(
                    "Load balance throttled because instance " + ins + " for " + resourceName + " "
                        + partition.getPartitionName());
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
              throttleController
                  .chargeInstance(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE, ins);
            }
          }

          throttleController
              .chargeCluster(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE);
          throttleController
              .chargeResource(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
                  resourceName);
        } else {
          intermediateMap.putAll(currentStateMap);
          loadbalanceThrottledCount++;
          partitionsLoadbalanceThrottled.add(partition);
        }
        output.setState(partition, intermediateMap);
      }
    }

    logger.info(String.format(
        "RecoveryNeeded: %d, RecoveryThrottled: %d, loadbalanceNeeded: %d, loadbalanceThrottled: %d",
        recoveryNeededCount, recoveryThrottledCount, loadbalanceNeededCount,
        loadbalanceThrottledCount));

    if (logger.isDebugEnabled()) {
      logParitionMapState(resourceName, new HashSet(resource.getPartitions()),
          partitionsNeedRecovery, partitionsRecoveryThrotted, partitionsNeedLoadbalance,
          partitionsLoadbalanceThrottled, currentStateOutput, bestPossiblePartitionStateMap,
          output);
    }

    logger.info("End processing resource:" + resourceName);

    return output;
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

  private boolean needRecoveryRebalance(Map<String, String> bestPossibleMap,
      StateModelDefinition stateModelDef, Map<String, String> currentStateMap) {
    boolean recoveryBalanceNeeded = false;
    List<String> states = stateModelDef.getStatesPriorityList();
    Map<String, Long> bestPossibleStateCounts = getStateCounts(bestPossibleMap);
    Map<String, Long> currentStateCounts = getStateCounts(currentStateMap);

    for (String state : states) {
      Long bestPossibleCount = bestPossibleStateCounts.get(state);
      Long currentCount = currentStateCounts.get(state);

      if (bestPossibleCount == null && currentCount == null) {
        continue;
      } else if (bestPossibleCount == null || currentCount == null ||
          !bestPossibleCount.equals(currentCount)) {
        if (!state.equals(HelixDefinedState.DROPPED.name()) &&
            !state.equals(HelixDefinedState.ERROR.name()) &&
            !state.equals(stateModelDef.getInitialState())) {
          recoveryBalanceNeeded = true;
          break;
        }
      }
    }

    return recoveryBalanceNeeded;
  }

  /* given instance->state map, return the state counts */
  private Map<String, Long> getStateCounts(Map<String, String> stateMap) {
    Map<String, Long> stateCounts = new HashMap<String, Long>();
    for (String state : stateMap.values()) {
      if (!stateCounts.containsKey(state)) {
        stateCounts.put(state, 0L);
      }
      stateCounts.put(state, stateCounts.get(state) + 1);
    }
    return stateCounts;
  }
}
