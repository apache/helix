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
import java.util.Optional;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.api.rebalancer.constraint.AbnormalStateResolver;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a abstract rebalancer that defines some default behaviors for Helix rebalancer
 * as well as all utility functions that will be used by all specific rebalancers.
 */
public abstract class AbstractRebalancer<T extends BaseControllerDataProvider> implements Rebalancer<T>,
    MappingCalculator<T> {
  // These should be final, but are initialized in init rather than a constructor
  protected HelixManager _manager;
  protected RebalanceStrategy<T> _rebalanceStrategy;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRebalancer.class);

  @Override
  public void init(HelixManager manager) {
    this._manager = manager;
    this._rebalanceStrategy = null;
  }

  @Override
  public abstract IdealState computeNewIdealState(
      String resourceName, IdealState currentIdealState, CurrentStateOutput currentStateOutput,
      T clusterData);

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
  public ResourceAssignment computeBestPossiblePartitionState(
      T cache, IdealState idealState, Resource resource,
      CurrentStateOutput currentStateOutput) {
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
              cache.getClusterConfig(), partition,
              cache.getAbnormalStateResolver(stateModelDefName));
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  /**
   * Looking for cached ideal mapping for this resource, if it is already there, do not recompute it
   * again. The cached mapping will be cleared in ResourceControllerDataProvider if there is
   * anything changed in cluster state that can cause the potential changes in ideal state.
   * This will avoid flip-flop issue we saw in AutoRebalanceStrategy, and also improve the
   * performance by avoiding recompute IS everytime.
   */
  protected IdealState getCachedIdealState(String resourceName, ResourceControllerDataProvider clusterData) {
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

  protected RebalanceStrategy<T> getRebalanceStrategy(
      String rebalanceStrategyName, List<String> partitions, String resourceName,
      LinkedHashMap<String, Integer> stateCountMap, int maxPartition) {
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
   * Compute best state for partition in AUTO ideal state mode.
   * @param liveInstances
   * @param stateModelDef
   * @param preferenceList
   * @param currentStateOutput instance->state for each partition
   * @param disabledInstancesForPartition
   * @param idealState
   * @param clusterConfig
   * @param partition
   * @param resolver
   * @return
   */
  protected Map<String, String> computeBestPossibleStateForPartition(Set<String> liveInstances,
      StateModelDefinition stateModelDef, List<String> preferenceList,
      CurrentStateOutput currentStateOutput, Set<String> disabledInstancesForPartition,
      IdealState idealState, ClusterConfig clusterConfig, Partition partition,
      AbnormalStateResolver resolver) {
    Optional<Map<String, String>> optionalOverwrittenStates =
        computeStatesOverwriteForPartition(stateModelDef, preferenceList, currentStateOutput,
            idealState, partition, resolver);
    if (optionalOverwrittenStates.isPresent()) {
      return optionalOverwrittenStates.get();
    }

    Map<String, String> currentStateMap = new HashMap<>(
        currentStateOutput.getCurrentStateMap(idealState.getResourceName(), partition));
    return computeBestPossibleMap(preferenceList, stateModelDef, currentStateMap, liveInstances,
        disabledInstancesForPartition);
  }

  /**
   * Compute if an overwritten is necessary for the partition assignment in case that the proposed
   * assignment is not valid or empty.
   * @param stateModelDef
   * @param preferenceList
   * @param currentStateOutput
   * @param idealState
   * @param partition
   * @param resolver
   * @return An optional object which contains the assignment map if overwritten is necessary.
   * Otherwise return Optional.empty().
   */
  protected Optional<Map<String, String>> computeStatesOverwriteForPartition(
      final StateModelDefinition stateModelDef, final List<String> preferenceList,
      final CurrentStateOutput currentStateOutput, IdealState idealState, final Partition partition,
      final AbnormalStateResolver resolver) {
    String resourceName = idealState.getResourceName();
    Map<String, String> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName, partition);

    // (1) If the partition is removed from IS or the IS is deleted.
    // Transit to DROPPED no matter the instance is disabled or not.
    if (preferenceList == null) {
      return Optional.of(computeBestPossibleMapForDroppedResource(currentStateMap));
    }

    // (2) If resource disabled altogether, transit to initial-state (e.g. OFFLINE) if it's not in ERROR.
    if (!idealState.isEnabled()) {
      return Optional.of(computeBestPossibleMapForDisabledResource(currentStateMap, stateModelDef));
    }

    // (3) If the current states are not valid, fix the invalid part first.
    if (!resolver.isCurrentStatesValid(currentStateOutput, resourceName, partition, stateModelDef)) {
      Map<String, String> recoveryAssignment = resolver
          .computeRecoveryAssignment(currentStateOutput, resourceName, partition, stateModelDef,
              preferenceList);
      if (recoveryAssignment == null || !recoveryAssignment.keySet()
          .equals(currentStateMap.keySet())) {
        throw new HelixException(String.format(
            "Invalid recovery assignment %s since it changed the current partition placement %s",
            recoveryAssignment, currentStateMap));
      }
      return Optional.of(recoveryAssignment);
    }

    return Optional.empty();
  }

  protected Map<String, String> computeBestPossibleMapForDroppedResource(
      final Map<String, String> currentStateMap) {
    Map<String, String> bestPossibleStateMap = new HashMap<>();
    for (String instance : currentStateMap.keySet()) {
      bestPossibleStateMap.put(instance, HelixDefinedState.DROPPED.toString());
    }
    return bestPossibleStateMap;
  }

  protected Map<String, String> computeBestPossibleMapForDisabledResource(
      final Map<String, String> currentStateMap, StateModelDefinition stateModelDef) {
    Map<String, String> bestPossibleStateMap = new HashMap<>();
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

    // Sort the instances based on replicas' state priority in the current state
    List<String> sortedPreferenceList = new ArrayList<>(preferenceList);
    sortedPreferenceList.sort(new StatePriorityComparator(currentStateMap, stateModelDef));

    // Assign the state to the instances that appear in the preference list.
    for (String state : statesPriorityList) {
      int stateCount =
          getStateCount(state, stateModelDef, liveAndEnabled.size(), preferenceList.size());
      for (String instance : preferenceList) {
        if (stateCount <= 0) {
          break; // continue assigning for the next state
        }
        if (assigned.contains(instance) || !liveAndEnabled.contains(instance)) {
          continue; // continue checking for the next available instance
        }
        String proposedInstance = instance;
        // Additional check and alternate the assignment for reducing top state handoff.
        if (state.equals(stateModelDef.getTopState()) && !stateModelDef.getSecondTopStates()
            .contains(currentStateMap.getOrDefault(instance, stateModelDef.getInitialState()))) {
          // If the desired state is the top state, but the instance cannot be transited to the
          // top state in one hop, try to keep the top state on current host or a host with a closer
          // state.
          for (String currentStatePrioritizedInstance : sortedPreferenceList) {
            if (!assigned.contains(currentStatePrioritizedInstance) && liveAndEnabled
                .contains(currentStatePrioritizedInstance)) {
              proposedInstance = currentStatePrioritizedInstance;
              break;
            }
          }
          // Note that if all the current top state instances are not assignable, then we fallback
          // to the default logic that assigning the state according to preference list order.
        }
        // Assign the desired state to the proposed instance
        if (HelixDefinedState.ERROR.toString().equals(currentStateMap.get(proposedInstance))) {
          bestPossibleStateMap.put(proposedInstance, HelixDefinedState.ERROR.toString());
        } else {
          bestPossibleStateMap.put(proposedInstance, state);
          stateCount--;
        }
        assigned.add(proposedInstance);
      }
    }
    return bestPossibleStateMap;
  }

  /**
   * Sorter for nodes that sorts according to the CurrentState of the partition.
   */
  protected static class StatePriorityComparator implements Comparator<String> {
    private final Map<String, String> _currentStateMap;
    private final StateModelDefinition _stateModelDef;

    public StatePriorityComparator(Map<String, String> currentStateMap,
        StateModelDefinition stateModelDef) {
      _currentStateMap = currentStateMap;
      _stateModelDef = stateModelDef;
    }

    @Override
    public int compare(String ins1, String ins2) {
      String state1 = _currentStateMap.get(ins1);
      String state2 = _currentStateMap.get(ins2);
      int p1 = state1 == null ? Integer.MAX_VALUE : _stateModelDef.getStatePriorityMap().get(state1);
      int p2 = state2 == null ? Integer.MAX_VALUE : _stateModelDef.getStatePriorityMap().get(state2);
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

  // This is for a backward compatible workaround to fix
  // https://github.com/apache/helix/issues/940.
  // TODO: remove the workaround once we are able to apply the simple fix without majorly
  // TODO: impacting user's clusters.
  protected List<String> getStablePartitionList(ResourceControllerDataProvider clusterData,
      IdealState currentIdealState) {
    List<String> partitions =
        clusterData.getStablePartitionList(currentIdealState.getResourceName());
    if (partitions == null) {
      Set<String> currentPartitionSet = currentIdealState.getPartitionSet();
      // In theory, the cached stable partition list must have contains all items in the current
      // partition set. Add one more check to avoid any intermediate change that modifies the list.
      LOG.warn("The current partition set {} has not been cached in the stable partition list. "
              + "Use the IdealState partition set directly.", currentPartitionSet.toString());
      partitions = new ArrayList<>(currentPartitionSet);
    }
    return partitions;
  }
}
