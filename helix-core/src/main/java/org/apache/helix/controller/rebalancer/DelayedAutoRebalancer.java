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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * This is the Full-Auto Rebalancer that is featured with delayed partition movement.
 */
public class DelayedAutoRebalancer extends AbstractRebalancer {
  private static final Logger LOG = Logger.getLogger(DelayedAutoRebalancer.class);
  private static RebalanceScheduler _scheduledRebalancer = new RebalanceScheduler();

  @Override
  public IdealState computeNewIdealState(String resourceName,
      IdealState currentIdealState, CurrentStateOutput currentStateOutput,
      ClusterDataCache clusterData) {
    List<String> partitions = new ArrayList<String>(currentIdealState.getPartitionSet());
    if (partitions.size() == 0) {
      LOG.info("Partition count is 0 for resource " + resourceName
          + ", stop calculate ideal mapping for the resource.");
      return generateNewIdealState(resourceName, currentIdealState,
          emptyMapping(currentIdealState));
    }
    ClusterConfig clusterConfig = clusterData.getClusterConfig();
    boolean delayRebalanceEnabled = isDelayRebalanceEnabled(currentIdealState, clusterConfig);

    Set<String> liveEnabledNodes;
    Set<String> allNodes;

    String instanceTag = currentIdealState.getInstanceGroupTag();
    if (instanceTag != null) {
      liveEnabledNodes = clusterData.getEnabledLiveInstancesWithTag(instanceTag);
      allNodes = clusterData.getInstancesWithTag(instanceTag);

      if (LOG.isInfoEnabled()) {
        LOG.info(String.format(
            "Found the following participants with tag %s for %s: "
                + "instances: %s, liveEnabledInstances: %s",
            currentIdealState.getInstanceGroupTag(), resourceName, allNodes, liveEnabledNodes));
      }
    } else {
      liveEnabledNodes = clusterData.getEnabledLiveInstances();
      allNodes = clusterData.getAllInstances();
    }

    Set<String> activeNodes = liveEnabledNodes;
    if (delayRebalanceEnabled) {
      long delay = getRebalanceDelay(currentIdealState, clusterConfig);
      activeNodes = getActiveInstances(allNodes, currentIdealState, liveEnabledNodes,
          clusterData.getInstanceOfflineTimeMap(), clusterData.getLiveInstances().keySet(),
          clusterData.getInstanceConfigMap(), delay, clusterConfig);

      Set<String> offlineOrDisabledInstances = new HashSet<String>(activeNodes);
      offlineOrDisabledInstances.removeAll(liveEnabledNodes);
      setRebalanceScheduler(currentIdealState, offlineOrDisabledInstances,
          clusterData.getInstanceOfflineTimeMap(), clusterData.getLiveInstances().keySet(),
          clusterData.getInstanceConfigMap(), delay, clusterConfig);
    }

    if (allNodes.isEmpty() || activeNodes.isEmpty()) {
      LOG.error(String.format(
          "No instances or active instances available for resource %s, "
              + "allInstances: %s, liveInstances: %s, activeInstances: %s",
          resourceName, allNodes, liveEnabledNodes, activeNodes));
      return generateNewIdealState(resourceName, currentIdealState,
          emptyMapping(currentIdealState));
    }

    StateModelDefinition stateModelDef =
        clusterData.getStateModelDef(currentIdealState.getStateModelDefRef());

    int replicaCount = currentIdealState.getReplicaCount(activeNodes.size());
    if (replicaCount == 0) {
      LOG.error("Replica count is 0 for resource " + resourceName
          + ", stop calculate ideal mapping for the resource.");
      return generateNewIdealState(resourceName, currentIdealState,
          emptyMapping(currentIdealState));
    }

    LinkedHashMap<String, Integer> stateCountMap =
        stateModelDef.getStateCountMap(activeNodes.size(), replicaCount);
    Map<String, Map<String, String>> currentMapping =
        currentMapping(currentStateOutput, resourceName, partitions, stateCountMap);

    int maxPartition = currentIdealState.getMaxPartitionsPerInstance();
    _rebalanceStrategy =
        getRebalanceStrategy(currentIdealState.getRebalanceStrategy(), partitions, resourceName,
            stateCountMap, maxPartition);

    // sort node lists to ensure consistent preferred assignments
    List<String> allNodeList = new ArrayList<String>(allNodes);
    List<String> liveEnabledNodeList = new ArrayList<String>(liveEnabledNodes);
    Collections.sort(allNodeList);
    Collections.sort(liveEnabledNodeList);

    ZNRecord newIdealMapping = _rebalanceStrategy
        .computePartitionAssignment(allNodeList, liveEnabledNodeList, currentMapping, clusterData);
    ZNRecord finalMapping = newIdealMapping;

    if (isDelayRebalanceEnabled(currentIdealState, clusterConfig)) {
      List<String> activeNodeList = new ArrayList<String>(activeNodes);
      Collections.sort(activeNodeList);
      int minActiveReplicas = getMinActiveReplica(currentIdealState, replicaCount);

      ZNRecord newActiveMapping = _rebalanceStrategy
          .computePartitionAssignment(allNodeList, activeNodeList, currentMapping, clusterData);
      finalMapping =
          getFinalDelayedMapping(currentIdealState, newIdealMapping, newActiveMapping, liveEnabledNodes,
              replicaCount, minActiveReplicas);
      LOG.debug("newActiveMapping: " + newActiveMapping);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("currentMapping: " + currentMapping);
      LOG.debug("stateCountMap: " + stateCountMap);
      LOG.debug("liveEnabledNodes: " + liveEnabledNodes);
      LOG.debug("ActiveNodes: " + activeNodes);
      LOG.debug("allNodes: " + allNodes);
      LOG.debug("maxPartition: " + maxPartition);
      LOG.debug("newIdealMapping: " + newIdealMapping);
      LOG.debug("finalMapping: " + finalMapping);
    }

    return generateNewIdealState(resourceName, currentIdealState, finalMapping);
  }

  private IdealState generateNewIdealState(String resourceName, IdealState currentIdealState,
      ZNRecord newMapping) {
    IdealState newIdealState = new IdealState(resourceName);
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    newIdealState.setRebalanceMode(currentIdealState.getRebalanceMode());
    newIdealState.getRecord().setListFields(newMapping.getListFields());

    return newIdealState;
  }

  /* get all active instances (live instances plus offline-yet-active instances */
  private Set<String> getActiveInstances(Set<String> allNodes, IdealState idealState,
      Set<String> liveEnabledNodes, Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap, long delay, ClusterConfig clusterConfig) {
    Set<String> activeInstances = new HashSet<String>(liveEnabledNodes);

    if (!isDelayRebalanceEnabled(idealState, clusterConfig)) {
      return activeInstances;
    }

    Set<String> offlineOrDisabledInstances = new HashSet<String>(allNodes);
    offlineOrDisabledInstances.removeAll(liveEnabledNodes);

    long currentTime = System.currentTimeMillis();
    for (String ins : offlineOrDisabledInstances) {
      long inactiveTime = getInactiveTime(ins, liveNodes, instanceOfflineTimeMap.get(ins), delay,
          instanceConfigMap.get(ins));
      InstanceConfig instanceConfig = instanceConfigMap.get(ins);
      if (inactiveTime > currentTime && instanceConfig != null && instanceConfig
          .isDelayRebalanceEnabled()) {
        activeInstances.add(ins);
      }
    }

    return activeInstances;
  }

  /* Set a rebalance scheduler for the closest future rebalance time. */
  private void setRebalanceScheduler(IdealState idealState, Set<String> offlineOrDisabledInstances,
      Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap,  long delay,
      ClusterConfig clusterConfig) {
    String resourceName = idealState.getResourceName();
    if (!isDelayRebalanceEnabled(idealState, clusterConfig)) {
      _scheduledRebalancer.removeScheduledRebalance(resourceName);
      return;
    }

    long currentTime = System.currentTimeMillis();
    long nextRebalanceTime = Long.MAX_VALUE;
    // calculate the closest future rebalance time
    for (String ins : offlineOrDisabledInstances) {
      long inactiveTime = getInactiveTime(ins, liveNodes, instanceOfflineTimeMap.get(ins), delay,
          instanceConfigMap.get(ins));
      if (inactiveTime != -1 && inactiveTime > currentTime && inactiveTime < nextRebalanceTime) {
        nextRebalanceTime = inactiveTime;
      }
    }

    if (nextRebalanceTime == Long.MAX_VALUE) {
      long startTime = _scheduledRebalancer.removeScheduledRebalance(resourceName);
      LOG.debug(String
          .format("Remove exist rebalance timer for resource %s at %d\n", resourceName, startTime));
    } else {
      long currentScheduledTime = _scheduledRebalancer.getRebalanceTime(resourceName);
      if (currentScheduledTime < 0 || currentScheduledTime > nextRebalanceTime) {
        _scheduledRebalancer.scheduleRebalance(_manager, resourceName, nextRebalanceTime);
        LOG.debug(String
            .format("Set next rebalance time for resource %s at time %d\n", resourceName,
                nextRebalanceTime));
      }
    }
  }

  /**
   * The time when an offline or disabled instance should be treated as inactive. return -1 if it is
   * inactive now.
   *
   * @return
   */
  private long getInactiveTime(String instance, Set<String> liveInstances, Long offlineTime,
      long delay, InstanceConfig instanceConfig) {
    long inactiveTime = Long.MAX_VALUE;

    // check the time instance went offline.
    if (!liveInstances.contains(instance)) {
      if (offlineTime != null && offlineTime > 0 && offlineTime + delay < inactiveTime) {
        inactiveTime = offlineTime + delay;
      }
    }

    // check the time instance got disabled.
    if (!instanceConfig.getInstanceEnabled()) {
      long disabledTime = instanceConfig.getInstanceEnabledTime();
      if (disabledTime > 0 && disabledTime + delay < inactiveTime) {
        inactiveTime = disabledTime + delay;
      }
    }

    if (inactiveTime == Long.MAX_VALUE) {
      return -1;
    }

    return inactiveTime;
  }

  private long getRebalanceDelay(IdealState idealState, ClusterConfig clusterConfig) {
    long delayTime = idealState.getRebalanceDelay();
    if (delayTime < 0) {
      delayTime = clusterConfig.getRebalanceDelayTime();
    }
    return delayTime;
  }

  private boolean isDelayRebalanceEnabled(IdealState idealState, ClusterConfig clusterConfig) {
    long delay = getRebalanceDelay(idealState, clusterConfig);
    return (delay > 0 && idealState.isDelayRebalanceEnabled() && clusterConfig
        .isDelayRebalaceEnabled());
  }

  private ZNRecord getFinalDelayedMapping(IdealState idealState, ZNRecord newIdealMapping,
      ZNRecord newActiveMapping, Set<String> liveInstances, int numReplica, int minActiveReplica) {
    if (minActiveReplica >= numReplica) {
      return newIdealMapping;
    }
    ZNRecord finalMapping = new ZNRecord(idealState.getResourceName());
    for (String partition : idealState.getPartitionSet()) {
      List<String> idealList = newIdealMapping.getListField(partition);
      List<String> activeList = newActiveMapping.getListField(partition);

      List<String> liveList = new ArrayList<String>();
      int activeReplica = 0;
      for (String ins : activeList) {
        if (liveInstances.contains(ins)) {
          activeReplica++;
          liveList.add(ins);
        }
      }

      if (activeReplica >= minActiveReplica) {
        finalMapping.setListField(partition, activeList);
      } else {
        List<String> candidates = new ArrayList<String>(idealList);
        candidates.removeAll(activeList);
        for (String liveIns : candidates) {
          liveList.add(liveIns);
          if (liveList.size() >= minActiveReplica) {
            break;
          }
        }
        finalMapping.setListField(partition, liveList);
      }
    }
    return finalMapping;
  }

  private ZNRecord emptyMapping(IdealState idealState) {
    ZNRecord emptyMapping = new ZNRecord(idealState.getResourceName());
    for (String partition : idealState.getPartitionSet()) {
      emptyMapping.setListField(partition, new ArrayList<String>());
    }
    return emptyMapping;
  }

  /**
   * Compute the best state for all partitions.
   * This is the default implementation, subclasses should re-implement
   * this method if its logic to generate bestpossible map for each partition is different from the default one here.
   *
   * @param cache
   * @param idealState
   * @param resource
   * @param currentStateOutput Provides the current state and pending state transitions for all partitions
   * @return
   */
  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache cache,
      IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing resource:" + resource.getResourceName());
    }

    Set<String> allNodes = cache.getEnabledInstances();
    Set<String> liveNodes = cache.getLiveInstances().keySet();

    ClusterConfig clusterConfig = cache.getClusterConfig();
    long delayTime = getRebalanceDelay(idealState, clusterConfig);
    Set<String> activeNodes = getActiveInstances(allNodes, idealState, liveNodes,
        cache.getInstanceOfflineTimeMap(), cache.getLiveInstances().keySet(),
        cache.getInstanceConfigMap(), delayTime, clusterConfig);

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    ResourceAssignment partitionMapping = new ResourceAssignment(resource.getResourceName());
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);
      Set<String> disabledInstancesForPartition =
          cache.getDisabledInstancesForPartition(resource.getResourceName(), partition.toString());
      List<String> preferenceList = getPreferenceList(partition, idealState, activeNodes);
      Map<String, String> bestStateForPartition =
          computeBestPossibleStateForPartition(liveNodes, stateModelDef, preferenceList, currentStateMap,
              disabledInstancesForPartition, idealState.isEnabled());

      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  private int getMinActiveReplica(IdealState idealState, int replicaCount) {
    int minActiveReplicas = idealState.getMinActiveReplicas();
    if (minActiveReplicas < 0) {
      minActiveReplicas = replicaCount;
    }
    return minActiveReplicas;
  }

  /**
   * compute best state for resource in AUTO ideal state mode
   * @param liveInstances
   * @param stateModelDef
   * @param preferenceList
   * @param currentStateMap
   *          : instance->state for each partition
   * @param disabledInstancesForPartition
   * @param isResourceEnabled
   * @return
   */
  @Override
  protected Map<String, String> computeBestPossibleStateForPartition(Set<String> liveInstances,
      StateModelDefinition stateModelDef, List<String> preferenceList,
      Map<String, String> currentStateMap, Set<String> disabledInstancesForPartition,
      boolean isResourceEnabled) {

    if (currentStateMap == null) {
      currentStateMap = Collections.emptyMap();
    }

    // (1) If the partition is removed from IS or the IS is deleted.
    // Transit to DROPPED no matter the instance is disabled or not.
    if (preferenceList == null) {
      return computeBestPossibleMapForDroppedResource(currentStateMap);
    }

    // (2) If resource disabled altogether, transit to initial-state (e.g. OFFLINE) if it's not in ERROR.
    if (!isResourceEnabled) {
      return computeBestPossibleMapForDisabledResource(currentStateMap, stateModelDef);
    }

    // Instances not in preference list but still have active replica, retain to avoid zero replica during movement
    List<String> instancesToMove = new ArrayList<String>(currentStateMap.keySet());
    instancesToMove.removeAll(preferenceList);

    Set<String> instancesToDrop = new HashSet<String>();
    Iterator<String> it = instancesToMove.iterator();
    while (it.hasNext()) {
      String instance = it.next();
      String state = currentStateMap.get(instance);
      if (disabledInstancesForPartition.contains(instance) || state == null
          || state.equals(HelixDefinedState.ERROR.name())
          || state.equals(stateModelDef.getInitialState())
          || disabledInstancesForPartition.contains(instance)) {
        it.remove();
        instancesToDrop.add(instance); // These instances should be set to DROPPED after we get bestPossibleStateMap;
      }
    }

    // Sort the instancesToMove by their current partition state.
    // Reason: because the states are assigned to instances in the order appeared in preferenceList, if we have
    // [node1:Slave, node2:Master], we want to keep it that way, instead of assigning Master to node1.
    Collections.sort(instancesToMove, new PreferenceListNodeComparator(currentStateMap, stateModelDef));
    List<String> combinedPreferenceList = new ArrayList<String>(preferenceList);
    combinedPreferenceList.addAll(instancesToMove);

    // Assign states to instances with the combined preference list.
    Map<String, String> bestPossibleStateMap = computeBestPossibleMap(combinedPreferenceList, stateModelDef,
        currentStateMap, liveInstances, disabledInstancesForPartition);

    for (String instance : instancesToDrop) {
      bestPossibleStateMap.put(instance, HelixDefinedState.DROPPED.name());
    }

    // The eventual goal is to have all instances in preferenceList all in the target states as specified in bestPossibleStateMap.
    Map<String, String> targetInstanceMap = new HashMap<String, String>(bestPossibleStateMap);
    targetInstanceMap.keySet().retainAll(preferenceList);

    // Once currentStateMap contains all required target instances and states, the
    // load-balance finishes, we should drop all partitions from previous assigned instances.
    if (currentStateMap.entrySet().containsAll(targetInstanceMap.entrySet())) {
      for (String instance : currentStateMap.keySet()) {
        if (!preferenceList.contains(instance)) {
          String state = currentStateMap.get(instance);
          if (state != null) {
            bestPossibleStateMap.put(instance, HelixDefinedState.DROPPED.toString());
          }
        }
      }
    }

    return bestPossibleStateMap;
  }


  /**
   * Sorter for nodes that sorts according to the CurrentState of the partition, based on the state priority defined
   * in the state model definition.
   * If the CurrentState doesn't exist, treat it as having lowest priority(Integer.MAX_VALUE).
   */
  private static class PreferenceListNodeComparator implements Comparator<String> {
    protected final Map<String, String> _currentStateMap;
    protected final StateModelDefinition _stateModelDef;

    public PreferenceListNodeComparator(Map<String, String> currentStateMap, StateModelDefinition stateModelDef) {
      _currentStateMap = currentStateMap;
      _stateModelDef = stateModelDef;
    }

    @Override
    public int compare(String ins1, String ins2) {
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
