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
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache cache,
      IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput) {
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
      List<String> preferenceList = getPreferenceList(partition, idealState,
          Collections.unmodifiableSet(cache.getLiveInstances().keySet()));
      Map<String, String> bestStateForPartition =
          computeAutoBestStateForPartition(cache, stateModelDef, preferenceList, currentStateMap,
              disabledInstancesForPartition, idealState.isEnabled());
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  protected Map<String, Map<String, String>> currentMapping(CurrentStateOutput currentStateOutput,
      String resourceName, List<String> partitions, Map<String, Integer> stateCountMap) {

    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();

    for (String partition : partitions) {
      Map<String, String> curStateMap =
          currentStateOutput.getCurrentStateMap(resourceName, new Partition(partition));
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

  protected RebalanceStrategy getRebalanceStrategy(String rebalanceStrategyName,
      List<String> partitions, String resourceName, LinkedHashMap<String, Integer> stateCountMap,
      int maxPartition) {
    RebalanceStrategy rebalanceStrategy;
    if (rebalanceStrategyName == null || rebalanceStrategyName
        .equalsIgnoreCase(RebalanceStrategy.DEFAULT_REBALANCE_STRATEGY)) {
      rebalanceStrategy =
          new AutoRebalanceStrategy(resourceName, partitions, stateCountMap, maxPartition);
    } else {
      try {
        rebalanceStrategy = RebalanceStrategy.class
            .cast(HelixUtil.loadClass(getClass(), rebalanceStrategyName).newInstance());
        rebalanceStrategy.init(resourceName, partitions, stateCountMap, maxPartition);
      } catch (ClassNotFoundException ex) {
        throw new HelixException(
            "Exception while invoking custom rebalance strategy class: " + rebalanceStrategyName,
            ex);
      } catch (InstantiationException ex) {
        throw new HelixException(
            "Exception while invoking custom rebalance strategy class: " + rebalanceStrategyName,
            ex);
      } catch (IllegalAccessException ex) {
        throw new HelixException(
            "Exception while invoking custom rebalance strategy class: " + rebalanceStrategyName,
            ex);
      }
    }

    return rebalanceStrategy;
  }

  /**
   * compute best state for resource in AUTO ideal state mode
   * @param cache
   * @param stateModelDef
   * @param instancePreferenceList
   * @param currentStateMap
   *          : instance->state for each partition
   * @param disabledInstancesForPartition
   * @param isResourceEnabled
   * @return
   */
  public Map<String, String> computeAutoBestStateForPartition(ClusterDataCache cache,
      StateModelDefinition stateModelDef, List<String> instancePreferenceList,
      Map<String, String> currentStateMap, Set<String> disabledInstancesForPartition,
      boolean isResourceEnabled) {
    Map<String, String> instanceStateMap = new HashMap<String, String>();

    if (currentStateMap != null) {
      for (String instance : currentStateMap.keySet()) {
        if (instancePreferenceList == null || !instancePreferenceList.contains(instance)) {
          // The partition is dropped from preference list.
          // Transit to DROPPED no matter the instance is disabled or not.
          instanceStateMap.put(instance, HelixDefinedState.DROPPED.toString());
        } else {
          // if disabled and not in ERROR state, transit to initial-state (e.g. OFFLINE)
          if (disabledInstancesForPartition.contains(instance) || !isResourceEnabled) {
            if (currentStateMap.get(instance) == null || !currentStateMap.get(instance)
                .equals(HelixDefinedState.ERROR.name())) {
              instanceStateMap.put(instance, stateModelDef.getInitialState());
            }
          }
        }
      }
    }

    // if the ideal state is deleted, instancePreferenceList will be empty and
    // we should drop all resources.
    if (instancePreferenceList == null) {
      return instanceStateMap;
    }

    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    boolean assigned[] = new boolean[instancePreferenceList.size()];

    Map<String, LiveInstance> liveInstancesMap = cache.getLiveInstances();

    for (String state : statesPriorityList) {
      String num = stateModelDef.getNumInstancesPerState(state);
      int stateCount = -1;
      if ("N".equals(num)) {
        Set<String> liveAndEnabled = new HashSet<String>(liveInstancesMap.keySet());
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

          boolean notInErrorState =
              currentStateMap == null || currentStateMap.get(instanceName) == null
                  || !currentStateMap.get(instanceName).equals(HelixDefinedState.ERROR.toString());

          boolean enabled =
              !disabledInstancesForPartition.contains(instanceName) && isResourceEnabled;
          if (liveInstancesMap.containsKey(instanceName) && !assigned[i] && notInErrorState
              && enabled) {
            instanceStateMap.put(instanceName, state);
            count = count + 1;
            assigned[i] = true;
            if (count == stateCount) {
              break;
            }
          }
        }
      }
    }
    return instanceStateMap;
  }

  public static List<String> getPreferenceList(Partition partition, IdealState idealState,
      Set<String> eligibleInstances) {
    List<String> listField = idealState.getPreferenceList(partition.getPartitionName());

    if (listField != null && listField.size() == 1
        && IdealState.IdealStateConstants.ANY_LIVEINSTANCE.toString().equals(listField.get(0))) {
      List<String> prefList = new ArrayList<String>(eligibleInstances);
      Collections.sort(prefList);
      return prefList;
    } else {
      return listField;
    }
  }
}
