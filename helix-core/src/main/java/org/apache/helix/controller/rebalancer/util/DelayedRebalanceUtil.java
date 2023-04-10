package org.apache.helix.controller.rebalancer.util;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.stream.Collectors;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.PartitionWithReplicaCount;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The util for supporting delayed rebalance logic.
 */
public class DelayedRebalanceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(DelayedRebalanceUtil.class);

  private static RebalanceScheduler REBALANCE_SCHEDULER = new RebalanceScheduler();

  /**
   * @return true if delay rebalance is configured and enabled in the ClusterConfig configurations.
   */
  public static boolean isDelayRebalanceEnabled(ClusterConfig clusterConfig) {
    long delay = clusterConfig.getRebalanceDelayTime();
    return (delay > 0 && clusterConfig.isDelayRebalaceEnabled());
  }

  /**
   * @return true if delay rebalance is configured and enabled in Resource IdealState and the
   * ClusterConfig configurations.
   */
  public static boolean isDelayRebalanceEnabled(IdealState idealState,
      ClusterConfig clusterConfig) {
    long delay = getRebalanceDelay(idealState, clusterConfig);
    return (delay > 0 && idealState.isDelayRebalanceEnabled() && clusterConfig
        .isDelayRebalaceEnabled());
  }

  /**
   * @return the rebalance delay based on Resource IdealState and the ClusterConfig configurations.
   */
  public static long getRebalanceDelay(IdealState idealState, ClusterConfig clusterConfig) {
    long delayTime = idealState.getRebalanceDelay();
    if (delayTime < 0) {
      delayTime = clusterConfig.getRebalanceDelayTime();
    }
    return delayTime;
  }

  /**
   * @return all active nodes (live nodes plus offline-yet-active nodes) while considering cluster
   * delay rebalance configurations.
   */
  public static Set<String> getActiveNodes(Set<String> allNodes, Set<String> liveEnabledNodes,
      Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap, ClusterConfig clusterConfig) {
    if (!isDelayRebalanceEnabled(clusterConfig)) {
      return new HashSet<>(liveEnabledNodes);
    }
    return getActiveNodes(allNodes, liveEnabledNodes, instanceOfflineTimeMap, liveNodes,
        instanceConfigMap, clusterConfig.getRebalanceDelayTime(), clusterConfig);
  }

  /**
   * @return all active nodes (live nodes plus offline-yet-active nodes) while considering cluster
   * and the resource delay rebalance configurations.
   */
  public static Set<String> getActiveNodes(Set<String> allNodes, IdealState idealState,
      Set<String> liveEnabledNodes, Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap, long delay, ClusterConfig clusterConfig) {
    if (!isDelayRebalanceEnabled(idealState, clusterConfig)) {
      return new HashSet<>(liveEnabledNodes);
    }
    return getActiveNodes(allNodes, liveEnabledNodes, instanceOfflineTimeMap, liveNodes,
        instanceConfigMap, delay, clusterConfig);
  }

  private static Set<String> getActiveNodes(Set<String> allNodes, Set<String> liveEnabledNodes,
      Map<String, Long> instanceOfflineTimeMap, Set<String> liveNodes,
      Map<String, InstanceConfig> instanceConfigMap, long delay, ClusterConfig clusterConfig) {
    Set<String> activeNodes = new HashSet<>(liveEnabledNodes);
    Set<String> offlineOrDisabledInstances = new HashSet<>(allNodes);
    offlineOrDisabledInstances.removeAll(liveEnabledNodes);
    long currentTime = System.currentTimeMillis();
    for (String ins : offlineOrDisabledInstances) {
      long inactiveTime = getInactiveTime(ins, liveNodes, instanceOfflineTimeMap.get(ins), delay,
          instanceConfigMap.get(ins), clusterConfig);
      InstanceConfig instanceConfig = instanceConfigMap.get(ins);
      if (inactiveTime > currentTime && instanceConfig != null && instanceConfig
          .isDelayRebalanceEnabled()) {
        activeNodes.add(ins);
      }
    }
    return activeNodes;
  }

  /**
   * @return The time when an offline or disabled instance should be treated as inactive.
   * Return -1 if it is inactive now.
   */
  private static long getInactiveTime(String instance, Set<String> liveInstances, Long offlineTime,
      long delay, InstanceConfig instanceConfig, ClusterConfig clusterConfig) {
    long inactiveTime = Long.MAX_VALUE;

    // check the time instance went offline.
    if (!liveInstances.contains(instance)) {
      if (offlineTime != null && offlineTime > 0 && offlineTime + delay < inactiveTime) {
        inactiveTime = offlineTime + delay;
      }
    }

    // check the time instance got disabled.
    if (!InstanceValidationUtil.isInstanceEnabled(instanceConfig, clusterConfig)) {
      long disabledTime = instanceConfig.getInstanceEnabledTime();
      String batchedDisabledTime = clusterConfig.getInstanceHelixDisabledTimeStamp(instance);
      if (batchedDisabledTime != null && !batchedDisabledTime.isEmpty()) {
        // Update batch disable time
        long batchDisableTime = Long.parseLong(batchedDisabledTime);
        if (disabledTime == -1 || disabledTime > batchDisableTime) {
          disabledTime = batchDisableTime;
        }
      }
      if (disabledTime > 0 && disabledTime + delay < inactiveTime) {
        inactiveTime = disabledTime + delay;
      }
    }

    if (inactiveTime == Long.MAX_VALUE) {
      return -1;
    }

    return inactiveTime;
  }

  /**
   * Merge the new ideal preference list with the delayed mapping that is calculated based on the
   * delayed rebalance configurations.
   * The method will prioritize the "active" preference list so as to avoid unnecessary transient
   * state transitions.
   *
   * @param newIdealPreferenceList  the ideal mapping that was calculated based on the current
   *                                instance status
   * @param newDelayedPreferenceList the delayed mapping that was calculated based on the delayed
   *                                 instance status
   * @param liveEnabledInstances    list of all the nodes that are both alive and enabled.
   * @param minActiveReplica        the minimum replica count to ensure a valid mapping.
   *                                If the active list does not have enough replica assignment,
   *                                this method will fill the list with the new ideal mapping until
   *                                the replica count satisfies the minimum requirement.
   * @return the merged state mapping.
   */
  public static Map<String, List<String>> getFinalDelayedMapping(
      Map<String, List<String>> newIdealPreferenceList,
      Map<String, List<String>> newDelayedPreferenceList, Set<String> liveEnabledInstances,
      int minActiveReplica) {
    Map<String, List<String>> finalPreferenceList = new HashMap<>();
    for (String partition : newIdealPreferenceList.keySet()) {
      List<String> idealList = newIdealPreferenceList.get(partition);
      List<String> delayedIdealList = newDelayedPreferenceList.get(partition);

      List<String> liveList = new ArrayList<>();
      for (String ins : delayedIdealList) {
        if (liveEnabledInstances.contains(ins)) {
          liveList.add(ins);
        }
      }

      if (liveList.size() >= minActiveReplica) {
        finalPreferenceList.put(partition, delayedIdealList);
      } else {
        List<String> candidates = new ArrayList<>(idealList);
        candidates.removeAll(delayedIdealList);
        for (String liveIns : candidates) {
          liveList.add(liveIns);
          if (liveList.size() >= minActiveReplica) {
            break;
          }
        }
        finalPreferenceList.put(partition, liveList);
      }
    }
    return finalPreferenceList;
  }

  /**
   * Get the minimum active replica count threshold that allows delayed rebalance.
   * Prioritize of the input params:
   * 1. resourceConfig
   * 2. idealState
   * 3. replicaCount
   * The lower priority minimum active replica count will only be applied if the higher priority
   * items are missing.
   * TODO: Remove the idealState input once we have all the config information migrated to the
   * TODO: resource config by default.
   *
   * @param resourceConfig the resource config
   * @param idealState     the ideal state of the resource
   * @param replicaCount   the expected active replica count.
   * @return the expected minimum active replica count that is required
   */
  public static int getMinActiveReplica(ResourceConfig resourceConfig, IdealState idealState,
      int replicaCount) {
    int minActiveReplicas = resourceConfig == null ? -1 : resourceConfig.getMinActiveReplica();
    if (minActiveReplicas < 0) {
      minActiveReplicas = idealState.getMinActiveReplicas();
    }
    if (minActiveReplicas < 0) {
      minActiveReplicas = replicaCount;
    }
    return minActiveReplicas;
  }

  /**
   * Set a rebalance scheduler for the closest future rebalance time.
   */
  public static void setRebalanceScheduler(String resourceName, boolean isDelayedRebalanceEnabled,
      Set<String> offlineOrDisabledInstances, Map<String, Long> instanceOfflineTimeMap,
      Set<String> liveNodes, Map<String, InstanceConfig> instanceConfigMap, long delay,
      ClusterConfig clusterConfig, HelixManager manager) {
    if (!isDelayedRebalanceEnabled) {
      REBALANCE_SCHEDULER.removeScheduledRebalance(resourceName);
      return;
    }

    long currentTime = System.currentTimeMillis();
    long nextRebalanceTime = Long.MAX_VALUE;
    // calculate the closest future rebalance time
    for (String ins : offlineOrDisabledInstances) {
      long inactiveTime = getInactiveTime(ins, liveNodes, instanceOfflineTimeMap.get(ins), delay,
          instanceConfigMap.get(ins), clusterConfig);
      if (inactiveTime != -1 && inactiveTime > currentTime && inactiveTime < nextRebalanceTime) {
        nextRebalanceTime = inactiveTime;
      }
    }

    if (nextRebalanceTime == Long.MAX_VALUE) {
      long startTime = REBALANCE_SCHEDULER.removeScheduledRebalance(resourceName);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String
            .format("Remove exist rebalance timer for resource %s at %d\n", resourceName,
                startTime));
      }
    } else {
      long currentScheduledTime = REBALANCE_SCHEDULER.getRebalanceTime(resourceName);
      if (currentScheduledTime < 0 || currentScheduledTime > nextRebalanceTime) {
        REBALANCE_SCHEDULER.scheduleRebalance(manager, resourceName, nextRebalanceTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug(String
              .format("Set next rebalance time for resource %s at time %d\n", resourceName,
                  nextRebalanceTime));
        }
      }
    }
  }

  /**
   * Computes the partition replicas that needs to be brought up to satisfy minActiveReplicas while downed instances
   * are within the delayed window.
   * Keep all current assignment with their current allocation.
   * @param clusterData Cluster data cache.
   * @param replicaMap A set of assignable replicas by resource name.
   * @param activeInstances The set of active instances.
   * @param currentAssignment Current assignment by resource name.
   * @param allocatedReplicas The map from instance name to assigned replicas, the map is populated in this method.
   * @return The replicas that need to be assigned.
   */
  public static Set<AssignableReplica> findToBeAssignedReplicasForMinActiveReplica(
      ResourceControllerDataProvider clusterData,
      Map<String, Set<AssignableReplica>> replicaMap,
      Set<String> activeInstances,
      Map<String, ResourceAssignment> currentAssignment,
      Map<String, Set<AssignableReplica>> allocatedReplicas) {
    Set<AssignableReplica> toBeAssignedReplicas = new HashSet<>();

    Map<String, List<PartitionWithReplicaCount>> partitionsMissingMinActiveReplicas =
        getPartitionsNeedForRebalanceOverwrites(clusterData, currentAssignment);

    for (String resourceName : replicaMap.keySet()) {
      // <partition, <state, instances set>>
      Map<String, Map<String, Set<String>>> stateInstanceMap =
          ClusterModelProvider.getStateInstanceMap(currentAssignment.get(resourceName));
      ResourceAssignment resourceAssignment = currentAssignment.get(resourceName);
      String modelDef = clusterData.getIdealState(resourceName).getStateModelDefRef();
      Map<String, Integer> statePriorityMap = clusterData.getStateModelDef(modelDef).getStatePriorityMap();
      // keep all current assignment and add to allocated replicas
      resourceAssignment.getMappedPartitions().forEach(partition ->
          resourceAssignment.getReplicaMap(partition).forEach((instance, state) ->
              allocatedReplicas.computeIfAbsent(instance, key -> new HashSet<>())
                  .add(new AssignableReplica(clusterData.getClusterConfig(), clusterData.getResourceConfig(resourceName),
                      partition.getPartitionName(), state, statePriorityMap.get(state)))));
      // only proceed for resource requiring delayed rebalance overwrites
      List<PartitionWithReplicaCount> partitions =
          partitionsMissingMinActiveReplicas.getOrDefault(resourceName, Collections.emptyList());
      if (partitions.isEmpty()) {
        continue;
      }
      toBeAssignedReplicas.addAll(
          findAssignableReplicaForResource(clusterData, resourceName, partitions, stateInstanceMap, activeInstances));
    }
    return toBeAssignedReplicas;
  }

  private static Map<String, List<PartitionWithReplicaCount>> getPartitionsNeedForRebalanceOverwrites(
      ResourceControllerDataProvider clusterData,
      Map<String, ResourceAssignment> currentAssignment) {
    return currentAssignment.entrySet()
        .parallelStream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> findPartitionsMissingMinActiveReplica(clusterData, entry.getValue())
        ));
  }

  private static List<PartitionWithReplicaCount> findPartitionsMissingMinActiveReplica(
      ResourceControllerDataProvider clusterData,
      ResourceAssignment resourceAssignment) {
    String resourceName = resourceAssignment.getResourceName();
    IdealState currentIdealState = clusterData.getIdealState(resourceName);
    Set<String> enabledLiveInstances = clusterData.getEnabledLiveInstances();
    int numReplica = currentIdealState.getReplicaCount(enabledLiveInstances.size());
    int minActiveReplica = DelayedRebalanceUtil.getMinActiveReplica(ResourceConfig
        .mergeIdealStateWithResourceConfig(clusterData.getResourceConfig(resourceName),
            currentIdealState), currentIdealState, numReplica);
    return resourceAssignment.getMappedPartitions()
        .parallelStream()
        .filter(partition -> {
          long enabledLivePlacementCounter = resourceAssignment.getReplicaMap(partition).keySet()
              .stream()
              .filter(enabledLiveInstances::contains)
              .count();
          return enabledLivePlacementCounter < Math.min(minActiveReplica, numReplica);
        })
        .map(partition -> new PartitionWithReplicaCount(partition, minActiveReplica, numReplica))
        .collect(Collectors.toList());
  }

  /**
   * For the resource in the cluster, find additional AssignableReplica to close the gap on minActiveReplica.
   * @param clusterData Cluster data cache.
   * @param resourceName name of the resource
   * @param partitions Pre-computed list of partition missing minActiveReplica
   * @param stateInstanceMap <partition, <state, instances set>>
   * @param activeInstances A set of active instances
   * @return A set of AssignableReplica
   */
  private static Set<AssignableReplica> findAssignableReplicaForResource(
      ResourceControllerDataProvider clusterData,
      String resourceName,
      List<PartitionWithReplicaCount> partitions,
      Map<String, Map<String, Set<String>>> stateInstanceMap,
      Set<String> activeInstances) {
    LOG.info("Computing replicas requiring rebalance overwrite for resource: {}", resourceName);
    final List<String> priorityOrderedStates = getPriorityOrderedStates(resourceName, clusterData);
    final IdealState currentIdealState = clusterData.getIdealState(resourceName);
    final ResourceConfig resourceConfig = clusterData.getResourceConfig(resourceName);
    final Map<String, Integer> statePriorityMap =
        clusterData.getStateModelDef(currentIdealState.getStateModelDefRef()).getStatePriorityMap();
    final Set<AssignableReplica> toBeAssignedReplicas = new HashSet<>();

    for (PartitionWithReplicaCount partitionWithReplicaCount : partitions) {
      String partitionName = partitionWithReplicaCount.getPartition().getPartitionName();
      // count current active replicas of the partition
      Map<String, Set<String>> stateInstances = stateInstanceMap.getOrDefault(partitionName, Collections.emptyMap());
      long activeReplicas = stateInstances.entrySet().stream()
          .flatMap(e -> e.getValue().stream())
          .filter(activeInstances::contains)
          .count();
      int minActiveReplica = partitionWithReplicaCount.getMinActiveReplica();
      long replicaGapCount = minActiveReplica - activeReplicas;
      if (replicaGapCount <= 0) {
        // delayed rebalance overwrites isn't required, early stop and move on to next partition
        continue;
      }
      // follow the state priority order, add additional replicas to close the gap on replica count
      Map<String, Integer> stateCountMap = clusterData.getStateModelDef(currentIdealState.getStateModelDefRef())
          .getStateCountMap(minActiveReplica, minActiveReplica);
      // follow the priority order of states and prepare additional replicas to be assigned
      for (String state : priorityOrderedStates) {
        if (replicaGapCount <= 0) {
          break;
        }
        int priority = statePriorityMap.get(state);
        int curStateCount = stateInstances.getOrDefault(state, Collections.emptySet()).size();
        for (int i = 0; i < stateCountMap.get(state) - curStateCount; i++) {
          toBeAssignedReplicas.add(
              new AssignableReplica(clusterData.getClusterConfig(), resourceConfig, partitionName, state, priority));
          replicaGapCount--;
        }
      }
    }
    LOG.info("Replicas: {} need to be brought up for rebalance overwrite.", toBeAssignedReplicas);
    return toBeAssignedReplicas;
  }

  private static List<String> getPriorityOrderedStates(String resourceName, ResourceControllerDataProvider clusterData) {
    IdealState currentIdealState = clusterData.getIdealState(resourceName);
    Map<String, Integer> statePriorityMap =
        clusterData.getStateModelDef(currentIdealState.getStateModelDefRef()).getStatePriorityMap();

    List<String> priorityOrderedStates = new ArrayList<>(statePriorityMap.keySet());
    priorityOrderedStates.sort(Comparator.comparing(a -> statePriorityMap.getOrDefault(a, Integer.MAX_VALUE)));
    return priorityOrderedStates;
  }
}
