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
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a abstract rebalancer that defines some default behaviors for Helix rebalancer
 * as well as all utility functions that will be used by all specific rebalancers.
 */
public abstract class AbstractRebalancer implements Rebalancer, MappingCalculator {
  // These should be final, but are initialized in init rather than a constructor
  protected HelixManager _manager;
  protected RebalanceStrategy _rebalanceStrategy;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRebalancer.class);

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
      Set<String> disabledInstancesForPartition =
          cache.getDisabledInstancesForPartition(resource.getResourceName(), partition.toString());
      List<String> preferenceList = getPreferenceList(partition, idealState,
          Collections.unmodifiableSet(cache.getLiveInstances().keySet()));
      Map<String, String> bestStateForPartition =
          computeBestPossibleStateForPartition(cache.getLiveInstances().keySet(), stateModelDef,
              preferenceList, currentStateOutput, disabledInstancesForPartition, idealState,
              cache.getClusterConfig(), partition);
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  /**
   * Looking for cached ideal mapping for this resource, if it is already there, do not recompute it
   * again. The cached mapping will be cleared in ClusterDataCache if there is anything changed in
   * cluster state that can cause the potential changes in ideal state. This will avoid flip-flop
   * issue we saw in AutoRebalanceStrategy, and also improve the performance by avoiding recompute
   * IS everytime.
   */
  protected IdealState getCachedIdealState(String resourceName, ClusterDataCache clusterData) {
    ZNRecord cachedIdealMapping = clusterData.getCachedIdealMapping(resourceName);
    if (cachedIdealMapping != null) {
      return new IdealState(cachedIdealMapping);
    }

    return null;
  }

  protected Map<String, Map<String, String>> currentMapping(CurrentStateOutput currentStateOutput,
      String resourceName, List<String> partitions, Map<String, Integer> stateCountMap) {

    Map<String, Map<String, String>> map = new HashMap<>();

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

  protected Map<String, String> computeBestPossibleStateForPartition(Set<String> liveInstances,
      StateModelDefinition stateModelDef, List<String> preferenceList,
      CurrentStateOutput currentStateOutput, Set<String> disabledInstancesForPartition,
      IdealState idealState, ClusterConfig clusterConfig, Partition partition) {

    Map<String, String> currentStateMap =
        currentStateOutput.getCurrentStateMap(idealState.getResourceName(), partition);

    if (currentStateMap == null) {
      currentStateMap = Collections.emptyMap();
    }

    // (1) If the partition is removed from IS or the IS is deleted.
    // Transit to DROPPED no matter the instance is disabled or not.
    if (preferenceList == null) {
      return computeBestPossibleMapForDroppedResource(currentStateMap);
    }

    // (2) If resource disabled altogether, transit to initial-state (e.g. OFFLINE) if it's not in ERROR.
    if (!idealState.isEnabled()) {
      return computeBestPossibleMapForDisabledResource(currentStateMap, stateModelDef);
    }

    return computeBestPossibleMap(preferenceList, stateModelDef, currentStateMap, liveInstances,
        disabledInstancesForPartition);
  }

  protected Map<String, String> computeBestPossibleMapForDroppedResource(Map<String, String> currentStateMap) {
    Map<String, String> bestPossibleStateMap = new HashMap<String, String>();
    for (String instance : currentStateMap.keySet()) {
      bestPossibleStateMap.put(instance, HelixDefinedState.DROPPED.toString());
    }
    return bestPossibleStateMap;
  }

  protected Map<String, String> computeBestPossibleMapForDisabledResource(Map<String, String> currentStateMap
      , StateModelDefinition stateModelDef) {
    Map<String, String> bestPossibleStateMap = new HashMap<String, String>();
    for (String instance : currentStateMap.keySet()) {
      if (!HelixDefinedState.ERROR.name().equals(currentStateMap.get(instance))) {
        bestPossibleStateMap.put(instance, stateModelDef.getInitialState());
      }
    }
    return bestPossibleStateMap;
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

  public static int getStateCount(String state, StateModelDefinition stateModelDef, int liveAndEnabledSize,
      int preferenceListSize) {
    String num = stateModelDef.getNumInstancesPerState(state);
    int stateCount = -1;
    if ("N".equals(num)) {
      stateCount = liveAndEnabledSize;
    } else if ("R".equals(num)) {
      stateCount = preferenceListSize;
    } else {
      try {
        stateCount = Integer.parseInt(num);
      } catch (Exception e) {
        LOG.error("Invalid count for state:" + state + " ,count=" + num);
      }
    }

    return stateCount;
  }

  /**
   * This method generates the instance->state mapping for a partition based on its {@param preferenceList}
   * and {@param StateModelDefinition}.
   * {@param preferenceList} could be different for different rebalancer(e.g. DelayedAutoRebalancer).
   * This method also makes sure corner cases like disabled or ERROR instances are correctly handled.
   * {@param currentStateMap} must be non-null.
   */
  protected Map<String, String> computeBestPossibleMap(List<String> preferenceList, StateModelDefinition stateModelDef,
      Map<String, String> currentStateMap, Set<String> liveInstances, Set<String> disabledInstancesForPartition) {

    Map<String, String> bestPossibleStateMap = new HashMap<>();

    // (1) Instances that have current state but not in preference list, drop, no matter it's disabled or not.
    for (String instance : currentStateMap.keySet()) {
      if (!preferenceList.contains(instance)) {
        bestPossibleStateMap.put(instance, HelixDefinedState.DROPPED.name());
      }
    }

    // (2) Set initial-state to certain instances that are disabled and in preference list.
    // Be careful with the conditions.
    for (String instance : preferenceList) {
      if (disabledInstancesForPartition.contains(instance)) {
        if (currentStateMap.containsKey(instance)) {
          if (!currentStateMap.get(instance).equals(HelixDefinedState.ERROR.name())) {
            bestPossibleStateMap.put(instance, stateModelDef.getInitialState());
          }
        } else {
          if (liveInstances.contains(instance)) {
            bestPossibleStateMap.put(instance, stateModelDef.getInitialState());
          }
        }
      }
    }

    // (3) Assign normal states to instances.
    // When we choose the top-state (e.g. MASTER) replica for a partition, we prefer to choose it from
    // these replicas which are already in the secondary states (e.g, SLAVE) instead of in lower-state.
    // This is because a replica in secondary state will take shorter time to transition to the top-state,
    // which could minimize the impact to the application's availability.
    // To achieve that, we sort the preferenceList based on CurrentState, by treating top-state and second-states with
    // same priority and rely on the fact that Collections.sort() is stable.
    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    Set<String> assigned = new HashSet<>();
    Set<String> liveAndEnabled = new HashSet<>(liveInstances);
    liveAndEnabled.removeAll(disabledInstancesForPartition);

    for (String state : statesPriorityList) {
      // Use the the specially ordered preferenceList for choosing instance for top state.
      if (state.equals(statesPriorityList.get(0))) {
        List<String> preferenceListForTopState = new ArrayList<>(preferenceList);
        Collections.sort(preferenceListForTopState,
            new TopStatePreferenceListComparator(currentStateMap, stateModelDef));
        preferenceList = preferenceListForTopState;
      }

      int stateCount =
          getStateCount(state, stateModelDef, liveAndEnabled.size(), preferenceList.size());
      for (String instance : preferenceList) {
        if (stateCount <= 0) {
          break;
        }
        if (!assigned.contains(instance) && liveAndEnabled.contains(instance)) {
          if (HelixDefinedState.ERROR.toString().equals(currentStateMap.get(instance))) {
            bestPossibleStateMap.put(instance, HelixDefinedState.ERROR.toString());
          } else {
            bestPossibleStateMap.put(instance, state);
            stateCount--;
          }
          assigned.add(instance);
        }
      }
    }

    return bestPossibleStateMap;
  }

  /**
   * Sorter for nodes that sorts according to the CurrentState of the partition. There are only two priorities:
   * (1) Top-state and second states have priority 0. (2) Other states(or no state) have priority 1.
   */
  protected static class TopStatePreferenceListComparator implements Comparator<String> {
    protected final Map<String, String> _currentStateMap;
    protected final StateModelDefinition _stateModelDef;

    public TopStatePreferenceListComparator(Map<String, String> currentStateMap, StateModelDefinition stateModelDef) {
      _currentStateMap = currentStateMap;
      _stateModelDef = stateModelDef;
    }

    @Override
    public int compare(String ins1, String ins2) {
      String state1 = _currentStateMap.get(ins1);
      String state2 = _currentStateMap.get(ins2);

      String topState = _stateModelDef.getStatesPriorityList().get(0);
      Set<String> preferredStates = new HashSet<String>(_stateModelDef.getSecondTopStates());
      preferredStates.add(topState);

      int p1 = 1;
      int p2 = 1;

      if (state1 != null && preferredStates.contains(state1)) {
        p1 = 0;
      }
      if (state2 != null && preferredStates.contains(state2)) {
        p2 = 0;
      }

      return p1 - p2;
    }
  }

  /**
   * Sorter for nodes that sorts according to the CurrentState of the partition, based on the state priority defined
   * in the state model definition.
   * If the CurrentState doesn't exist, treat it as having lowest priority(Integer.MAX_VALUE).
   */
  protected static class PreferenceListNodeComparator implements Comparator<String> {
    protected final Map<String, String> _currentStateMap;
    protected final StateModelDefinition _stateModelDef;
    protected final List<String> _preferenceList;

    public PreferenceListNodeComparator(Map<String, String> currentStateMap,
        StateModelDefinition stateModelDef, List<String> preferenceList) {
      _currentStateMap = currentStateMap;
      _stateModelDef = stateModelDef;
      _preferenceList = preferenceList;
    }

    @Override
    public int compare(String ins1, String ins2) {
      // condition :
      // 1. both in preference list, keep the order in preference list
      // 2. one them in preference list, the one in preference list has higher priority
      // 3. none of them in preference list, sort by state.
      if (_preferenceList.contains(ins1) && _preferenceList.contains(ins2)) {
        return _preferenceList.indexOf(ins1) - _preferenceList.indexOf(ins2);
      } else if (_preferenceList.contains(ins1)) {
        return -1;
      } else if (_preferenceList.contains(ins2)) {
        return 1;
      }
      Integer p1 = Integer.MAX_VALUE;
      Integer p2 = Integer.MAX_VALUE;

      Map<String, Integer> statesPriorityMap = _stateModelDef.getStatePriorityMap();
      String state1 = _currentStateMap.get(ins1);
      String state2 = _currentStateMap.get(ins2);
      if (state1 != null && statesPriorityMap.containsKey(state1)) {
        p1 = statesPriorityMap.get(state1);
      }
      if (state2 != null && statesPriorityMap.containsKey(state2)) {
        p2 = statesPriorityMap.get(state2);
      }

      return p1.compareTo(p2);
    }
  }
}
