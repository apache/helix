package org.apache.helix.controller.rebalancer;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;


/**
 * This is a abstract rebalancer that defines some default behaviors for Helix rebalancer
 * as well as all utility functions that will be used by all specific rebalancers.
 */
public abstract class AbstractRebalancer implements Rebalancer, MappingCalculator {
  // These should be final, but are initialized in init rather than a constructor
  protected HelixManager _manager;
  protected RebalanceStrategy _rebalanceStrategy;

  private static final Logger LOG = Logger.getLogger(AbstractRebalancer.class);

  @Override
  public void init(HelixManager manager) {
    this._manager = manager;
    this._rebalanceStrategy = null;
  }

  @Override
  public abstract IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData);

  /**
   * Compute the best state for all partitions.
   * This is the default implementation, subclasses should re-implement
   * this method if its logic to generate bestpossible map for each partition is different from the default one here.
   *
   * @param cache
   * @param idealState
   * @param resource
   * @param currentStateOutput
   *          Provides the current state and pending state transitions for all partitions
   * @return
   */
  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache cache, IdealState idealState,
      Resource resource, CurrentStateOutput currentStateOutput) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + resource.getResourceName());
    }
    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    ResourceAssignment partitionMapping = new ResourceAssignment(resource.getResourceName());
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);
      Set<String> disabledInstancesForPartition =
          cache.getDisabledInstancesForPartition(resource.getResourceName(), partition.toString());
      List<String> preferenceList =
          getPreferenceList(partition, idealState, Collections.unmodifiableSet(cache.getLiveInstances().keySet()));
      Map<String, String> bestStateForPartition =
          computeBestPossibleStateForPartition(stateModelDef, preferenceList, currentStateMap,
              cache.getLiveInstances().keySet(), disabledInstancesForPartition, idealState.isEnabled());
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  protected Map<String, Map<String, String>> currentMapping(CurrentStateOutput currentStateOutput, String resourceName,
      List<String> partitions, Map<String, Integer> stateCountMap) {

    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();

    for (String partition : partitions) {
      Map<String, String> curStateMap = currentStateOutput.getCurrentStateMap(resourceName, new Partition(partition));
      map.put(partition, new HashMap<String, String>());
      for (String node : curStateMap.keySet()) {
        String state = curStateMap.get(node);
        map.get(partition).put(node, state);
      }

      Map<String, String> pendingStateMap =
          currentStateOutput.getPendingStateMap(resourceName, new Partition(partition));
      for (String node : pendingStateMap.keySet()) {
        String state = pendingStateMap.get(node);
        map.get(partition).put(node, state);
      }
    }
    return map;
  }

  protected RebalanceStrategy getRebalanceStrategy(String rebalanceStrategyName, List<String> partitions,
      String resourceName, LinkedHashMap<String, Integer> stateCountMap, int maxPartition) {
    RebalanceStrategy rebalanceStrategy;
    if (rebalanceStrategyName == null || rebalanceStrategyName.equalsIgnoreCase(
        RebalanceStrategy.DEFAULT_REBALANCE_STRATEGY)) {
      rebalanceStrategy = new AutoRebalanceStrategy(resourceName, partitions, stateCountMap, maxPartition);
    } else {
      try {
        rebalanceStrategy =
            RebalanceStrategy.class.cast(HelixUtil.loadClass(getClass(), rebalanceStrategyName).newInstance());
        rebalanceStrategy.init(resourceName, partitions, stateCountMap, maxPartition);
      } catch (ClassNotFoundException ex) {
        throw new HelixException("Exception while invoking custom rebalance strategy class: " + rebalanceStrategyName,
            ex);
      } catch (InstantiationException ex) {
        throw new HelixException("Exception while invoking custom rebalance strategy class: " + rebalanceStrategyName,
            ex);
      } catch (IllegalAccessException ex) {
        throw new HelixException("Exception while invoking custom rebalance strategy class: " + rebalanceStrategyName,
            ex);
      }
    }

    return rebalanceStrategy;
  }

  /**
   * compute best possible state for resource
   * @param stateModelDef
   * @param instancePreferenceList
   * @param currentStateMap
   *          : instance->state for each partition
   * @param liveInstances
   * @param disabledInstancesForPartition
   * @param isResourceEnabled
   * @return
   */
  public Map<String, String> computeBestPossibleStateForPartition(StateModelDefinition stateModelDef,
      List<String> instancePreferenceList, Map<String, String> currentStateMap, Set<String> liveInstances,
      Set<String> disabledInstancesForPartition, boolean isResourceEnabled) {
    Map<String, String> bestPossibleStateMap = new HashMap<String, String>();

    if (currentStateMap == null) {
      currentStateMap = new HashMap<String, String>();
    }

    for (String instance : currentStateMap.keySet()) {
      if (!instancePreferenceList.contains(instance)) {
        // The partition is dropped from preference list.
        // Transit to DROPPED no matter the instance is disabled or not.
        bestPossibleStateMap.put(instance, HelixDefinedState.DROPPED.toString());
      } else {
        // if disabled and not in ERROR state, transit to initial-state (e.g. OFFLINE)
        if (disabledInstancesForPartition.contains(instance) || !isResourceEnabled) {
          if (!currentStateMap.containsKey(instance) || !currentStateMap.get(instance)
              .equals(HelixDefinedState.ERROR.name())) {
            bestPossibleStateMap.put(instance, stateModelDef.getInitialState());
          }
        }
      }
    }

    // if the ideal state is deleted, instancePreferenceList will be empty and
    // we should drop all resources.
    if (instancePreferenceList.isEmpty()) {
      return bestPossibleStateMap;
    }

    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    boolean assigned[] = new boolean[instancePreferenceList.size()];

    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      int stateCount = -1;
      if ("N".equals(num)) {
        Set<String> liveAndEnabled = new HashSet<String>(liveInstances);
        liveAndEnabled.removeAll(disabledInstancesForPartition);
        stateCount = isResourceEnabled ? liveAndEnabled.size() : 0;
      } else if ("R".equals(num)) {
        stateCount = instancePreferenceList.size();
      } else {
        try {
          stateCount = Integer.parseInt(num);
        } catch (Exception e) {
          LOG.error("Invalid count for state:" + state + " ,count=" + num);
        }
      }
      if (stateCount > -1) {
        int count = 0;
        for (int i = 0; i < instancePreferenceList.size(); i++) {
          String instanceName = instancePreferenceList.get(i);

          boolean notInErrorState = !currentStateMap.containsKey(instanceName) || !currentStateMap.get(instanceName)
              .equals(HelixDefinedState.ERROR.toString());

          boolean enabled = !disabledInstancesForPartition.contains(instanceName) && isResourceEnabled;

          if (liveInstances.contains(instanceName) && !assigned[i] && notInErrorState && enabled) {
            bestPossibleStateMap.put(instanceName, state);
            count = count + 1;
            assigned[i] = true;
            if (count == stateCount) {
              break;
            }
          }
        }
      }
    }

    // In recovery from e.g. Offline to Master, we'd prefer promote it to Slave, and at the
    // same time promote another Slave to Master. After this is finished, switch the state of the two nodes.
    // This way, the system has higher availability because usually it takes longer time to
    // transit Offline->Slave than Slave->Master.
    // Therefore, we don't transit lower level state directly to top state, we try to
    // always only promote second level state to top state.

    // For each pair of states where current state is Offline and best possible is Master,
    // find the next pair where current is Slave and best possible is Slave, too
    // switch the best possible state of the 2 instances

    Set<String> secondTopStates = stateModelDef.getSecondTopStates();
    String topState = statesPriorityList.get(0);

    for (String instance : bestPossibleStateMap.keySet()) {
      if (!currentStateMap.containsKey(instance)) {
        continue;
      }
      String currentState = currentStateMap.get(instance);
      String bestPossibleState = bestPossibleStateMap.get(instance);

      if (!currentState.equals(topState) && !secondTopStates.contains(currentState) && bestPossibleState.equals(
          topState)) {

        // find alternative instance for Master
        for (String alternative : instancePreferenceList) {
          if (currentStateMap.containsKey(alternative) && secondTopStates.contains(currentStateMap.get(alternative))
              && secondTopStates.contains(bestPossibleStateMap.get(alternative))) {
            // switch states
            String tempState = bestPossibleStateMap.get(instance);
            bestPossibleStateMap.put(instance, bestPossibleStateMap.get(alternative));
            bestPossibleStateMap.put(alternative, tempState);
            break;
          }
        }
      }
    }

    return bestPossibleStateMap;
  }

  public static List<String> getPreferenceList(Partition partition, IdealState idealState,
      Set<String> eligibleInstances) {
    List<String> preferenceList = idealState.getPreferenceList(partition.getPartitionName());

    if (preferenceList.size() == 1 && IdealState.IdealStateConstants.ANY_LIVEINSTANCE.toString()
        .equals(preferenceList.get(0))) {
      List<String> sortedElibigleInstanceList = new ArrayList<String>(eligibleInstances);
      Collections.sort(sortedElibigleInstanceList);
      return sortedElibigleInstanceList;
    } else {
      return preferenceList;
    }
  }
}
