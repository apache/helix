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

import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is the Full-Auto Rebalancer that is featured with delayed partition movement.
 */
public class DelayedAutoRebalancer extends AbstractRebalancer {
  private static final Logger LOG = Logger.getLogger(DelayedAutoRebalancer.class);
  private static RebalanceScheduler _scheduledRebalancer = new RebalanceScheduler();

  @Override public IdealState computeNewIdealState(String resourceName,
      IdealState currentIdealState, CurrentStateOutput currentStateOutput,
      ClusterDataCache clusterData) {

    List<String> partitions = new ArrayList<String>(currentIdealState.getPartitionSet());
    if (partitions.size() == 0) {
      LOG.info("Partition count is 0 for resource " + resourceName
          + ", stop calculate ideal mapping for the resource.");
      return generateNewIdealState(resourceName, currentIdealState,
          emptyMapping(currentIdealState));
    }

    Set<String> liveNodes;
    Set<String> allNodes;

    String instanceTag = currentIdealState.getInstanceGroupTag();
    if (instanceTag != null) {
      liveNodes = clusterData.getEnabledLiveInstancesWithTag(instanceTag);
      allNodes = clusterData.getAllInstancesWithTag(instanceTag);

      if (!liveNodes.isEmpty()) {
        // live nodes exist that have this tag
        if (LOG.isInfoEnabled()) {
          LOG.info(String.format("Found the following participants with tag %s for %s: %s",
              currentIdealState.getInstanceGroupTag(), resourceName,
              Arrays.toString(liveNodes.toArray())));
        }
      }
    } else {
      liveNodes = clusterData.getEnabledLiveInstances();
      allNodes = clusterData.getEnabledInstances();
    }

    ClusterConfig clusterConfig = clusterData.getClusterConfig();
    long delayTime = getRebalanceDelay(currentIdealState, clusterConfig);
    Set<String> activeNodes = getActiveInstances(currentIdealState, allNodes, liveNodes,
        clusterData.getInstanceOfflineTimeMap(), delayTime, clusterConfig);
    setRebalanceScheduler(currentIdealState, activeNodes, clusterData.getInstanceOfflineTimeMap(),
        delayTime, clusterConfig);

    if (allNodes.isEmpty() || activeNodes.isEmpty()) {
      LOG.error(String.format(
          "No instances or active instances available for resource %s, allNodes: %s, liveNodes: %s, activeInstances: %s",
          resourceName, Arrays.toString(allNodes.toArray()), Arrays.toString(liveNodes.toArray()),
          Arrays.toString(activeNodes.toArray())));
      return generateNewIdealState(resourceName, currentIdealState,
          emptyMapping(currentIdealState));
    }

    StateModelDefinition stateModelDef =
        clusterData.getStateModelDef(currentIdealState.getStateModelDefRef());

    int replicaCount = getReplicaCount(currentIdealState, activeNodes);
    if (replicaCount == 0) {
      LOG.error("Replica count is 0 for resource " + resourceName
          + ", stop calculate ideal mapping for the resource.");
      return generateNewIdealState(resourceName, currentIdealState,
          emptyMapping(currentIdealState));
    }

    LinkedHashMap<String, Integer> stateCountMap =
        StateModelDefinition.getStateCountMap(stateModelDef, activeNodes.size(), replicaCount);
    Map<String, Map<String, String>> currentMapping =
        currentMapping(currentStateOutput, resourceName, partitions, stateCountMap);

    int maxPartition = currentIdealState.getMaxPartitionsPerInstance();
    _rebalanceStrategy =
        getRebalanceStrategy(currentIdealState.getRebalanceStrategy(), partitions, resourceName,
            stateCountMap, maxPartition);

    // sort node lists to ensure consistent preferred assignments
    List<String> allNodeList = new ArrayList<String>(allNodes);
    List<String> liveNodeList = new ArrayList<String>(liveNodes);
    Collections.sort(allNodeList);
    Collections.sort(liveNodeList);

    ZNRecord newIdealMapping = _rebalanceStrategy
        .computePartitionAssignment(allNodeList, liveNodeList, currentMapping, clusterData);
    ZNRecord finalMapping = newIdealMapping;

    if (!isDelayRebalanceDisabled(currentIdealState, clusterConfig)) {
      List<String> activeNodeList = new ArrayList<String>(activeNodes);
      Collections.sort(activeNodeList);
      int minActiveReplicas = getMinActiveReplica(currentIdealState, replicaCount);

      ZNRecord newActiveMapping = _rebalanceStrategy
          .computePartitionAssignment(allNodeList, activeNodeList, currentMapping, clusterData);
      finalMapping =
          getFinalDelayedMapping(currentIdealState, newIdealMapping, newActiveMapping, liveNodes,
              replicaCount, minActiveReplicas);
      LOG.debug("newActiveMapping: " + newActiveMapping);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("currentMapping: " + currentMapping);
      LOG.debug("stateCountMap: " + stateCountMap);
      LOG.debug("liveNodes: " + liveNodes);
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
  private Set<String> getActiveInstances(IdealState idealState, Set<String> allNodes,
      Set<String> liveNodes, Map<String, Long> instanceOfflineTimeMap, long delayTime,
      ClusterConfig clusterConfig) {
    Set<String> activeInstances = new HashSet<String>(liveNodes);

    if (isDelayRebalanceDisabled(idealState, clusterConfig)) {
      return activeInstances;
    }

    Set<String> offlineInstances = new HashSet<String>(allNodes);
    offlineInstances.removeAll(liveNodes);

    long currentTime = System.currentTimeMillis();
    for (String ins : offlineInstances) {
      Long offlineTime = instanceOfflineTimeMap.get(ins);
      if (offlineTime != null && offlineTime > 0) {
        if (delayTime > 0 && offlineTime + delayTime > currentTime) {
          activeInstances.add(ins);
        }
      }
    }

    return activeInstances;
  }

  /* Set a rebalance scheduler for the closest future rebalance time. */
  private void setRebalanceScheduler(IdealState idealState, Set<String> activeInstances,
      Map<String, Long> instanceOfflineTimeMap, long delayTime, ClusterConfig clusterConfig) {
    String resourceName = idealState.getResourceName();
    if (isDelayRebalanceDisabled(idealState, clusterConfig)) {
      _scheduledRebalancer.removeScheduledRebalance(resourceName);
      return;
    }

    long nextRebalanceTime = Long.MAX_VALUE;
    for (String ins : activeInstances) {
      Long offlineTime = instanceOfflineTimeMap.get(ins);
      if (offlineTime != null && offlineTime > 0) {
        // calculate the closest future rebalance time
        if (offlineTime + delayTime < nextRebalanceTime) {
          long rebalanceTime = offlineTime + delayTime;
          if (rebalanceTime < nextRebalanceTime) {
            nextRebalanceTime = rebalanceTime;
          }
        }
      }
    }

    if (nextRebalanceTime == Long.MAX_VALUE) {
      long startTime = _scheduledRebalancer.removeScheduledRebalance(resourceName);
      LOG.debug(String
          .format("Remove exist rebalance timer for resource %s at %d\n", resourceName, startTime));
    } else {
      _scheduledRebalancer.scheduleRebalance(_manager, resourceName, nextRebalanceTime);
      LOG.debug(String.format("Set next rebalance time for resource %s at time %d\n", resourceName,
          nextRebalanceTime));
    }
  }

  private long getRebalanceDelay(IdealState idealState, ClusterConfig clusterConfig) {
    long delayTime = idealState.getRebalanceDelay();
    if (delayTime < 0) {
      delayTime = clusterConfig.getRebalanceDelayTime();
    }
    return delayTime;
  }

  private boolean isDelayRebalanceDisabled(IdealState idealState, ClusterConfig clusterConfig) {
    long delayTime = getRebalanceDelay(idealState, clusterConfig);
    return (delayTime < 0 || idealState.isDelayRebalanceDisabled() || clusterConfig
        .isDelayRebalaceDisabled());
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
   * This is the default ConstraintBasedAssignment implementation, subclasses should re-implement
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
    Set<String> liveNodes = cache.getEnabledLiveInstances();
    Set<String> offlineNodes = cache.getAllInstances();
    offlineNodes.removeAll(cache.getLiveInstances().keySet());

    ClusterConfig clusterConfig = cache.getClusterConfig();
    long delayTime = getRebalanceDelay(idealState, clusterConfig);
    Set<String> activeNodes =
        getActiveInstances(idealState, allNodes, liveNodes, cache.getInstanceOfflineTimeMap(),
            delayTime, clusterConfig);

    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    ResourceAssignment partitionMapping = new ResourceAssignment(resource.getResourceName());
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap =
          currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);
      Set<String> disabledInstancesForPartition =
          cache.getDisabledInstancesForPartition(partition.toString());
      List<String> preferenceList =
          ConstraintBasedAssignment.getPreferenceList(partition, idealState, activeNodes);
      Map<String, String> bestStateForPartition = ConstraintBasedAssignment
          .computeAutoBestStateForPartition(cache, stateModelDef, preferenceList, currentStateMap,
              disabledInstancesForPartition, idealState.isEnabled());

      if (preferenceList == null) {
        LOG.info(String.format(
            "No preferenceList defined for partition %s, resource %s, skip computing best possible mapping!",
            partition.getPartitionName(), idealState.getResourceName()));
        continue;
      }

      for (String ins : preferenceList) {
        if (offlineNodes.contains(ins) && !bestStateForPartition.containsKey(ins)) {
          bestStateForPartition.put(ins, stateModelDef.getInitialState());
        }
      }
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    return partitionMapping;
  }

  private int getReplicaCount(IdealState idealState, Set<String> eligibleInstances) {
    String replicaStr = idealState.getReplicas();
    int replica = 0;

    try {
      replica = Integer.parseInt(replicaStr);
    } catch (NumberFormatException ex) {
      if (replicaStr.equalsIgnoreCase(IdealState.IdealStateConstants.ANY_LIVEINSTANCE.name())) {
        replica = eligibleInstances.size();
      } else {
        LOG.error("Can not determine the replica count for resource " + idealState.getResourceName()
            + ", set to 0.");
      }
    }

    return replica;
  }

  private int getMinActiveReplica(IdealState idealState, int replicaCount) {
    int minActiveReplicas = idealState.getMinActiveReplicas();
    if (minActiveReplicas < 0) {
      minActiveReplicas = replicaCount;
    }
    return minActiveReplicas;
  }
}
