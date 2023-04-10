package org.apache.helix.controller.rebalancer.waged.model;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.PartitionWithReplicaCount;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;


public class DelayedRebalanceOverwriteUtil {

  /**
   * Computes the partition replicas that needs to be brought up since those are falling short on minActiveReplicas
   * while downed instances are within the delayed window.
   * Keep all current assignment with their current allocation
   * @param clusterData Cluster data cache.
   * @param replicaMap A set of assignable replicas by resource name.
   * @param activeInstances The set of active instances.
   * @param currentAssignment Current assignment by resource name.
   * @param allocatedReplicas The map from instance name to assigned replicas, the map is populated in this method.
   * @return The replicas that need to be assigned.
   */
  static Set<AssignableReplica> findToBeAssignedReplicasForMinActiveReplica(
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
      IdealState currentIdealState = clusterData.getIdealState(resourceName);
      Map<String, Integer> statePriorityMap =
          clusterData.getStateModelDef(currentIdealState.getStateModelDefRef()).getStatePriorityMap();
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
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    return bestPossibleAssignment.entrySet().parallelStream().collect(
        Collectors.toMap(
            Map.Entry::getKey,
            entry -> findPartitionsMissingMinActiveReplica(clusterData, entry.getValue()))
    );
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
   * @param clusterData
   * @param resourceName
   * @param partitions
   * @param stateInstanceMap
   * @param activeInstances
   * @return A set of AssignableReplica
   */
  private static Set<AssignableReplica> findAssignableReplicaForResource(
      ResourceControllerDataProvider clusterData,
      String resourceName,
      List<PartitionWithReplicaCount> partitions,
      Map<String, Map<String, Set<String>>> stateInstanceMap,
      Set<String> activeInstances) {
    List<String> priorityOrderedStates = getPriorityOrderedStates(resourceName, clusterData);
    IdealState currentIdealState = clusterData.getIdealState(resourceName);
    Map<String, Integer> statePriorityMap =
        clusterData.getStateModelDef(currentIdealState.getStateModelDefRef()).getStatePriorityMap();
    Set<AssignableReplica> toBeAssignedReplicas = new HashSet<>();

    for (PartitionWithReplicaCount partitionWithReplicaCount : partitions) {
      String partitionName = partitionWithReplicaCount.getPartition().getPartitionName();
      // count current active replicas of the partition
      Map<String, Set<String>> stateInstances = stateInstanceMap.getOrDefault(partitionName, Collections.emptyMap());
      int activeReplicas = 0;
      for (Map.Entry<String, Set<String>> entry : stateInstances.entrySet()) {
        activeReplicas += entry.getValue().stream().filter(activeInstances::contains).count();
      }
      int minActiveReplica = partitionWithReplicaCount.getMinActiveReplica();
      int replicaGapCount = minActiveReplica - activeReplicas;
      if (replicaGapCount <= 0) {
        // delayed rebalance overwrites isn't required, early stop and move on to next partition
        continue;
      }
      // follow the state priority order, add additional replicas to close the gap on replica count
      Map<String, Integer> stateCountMap =
          clusterData.getStateModelDef(currentIdealState.getStateModelDefRef())
              .getStateCountMap(minActiveReplica, minActiveReplica);

      for (String state : priorityOrderedStates) {
        if (replicaGapCount <= 0) {
          break;
        }
        int priority = statePriorityMap.get(state);
        int curStateCount = stateInstances.getOrDefault(state, Collections.emptySet()).size();
        for (int i = 0; i < stateCountMap.get(state) - curStateCount; i++) {
          toBeAssignedReplicas.add(new AssignableReplica(
              clusterData.getClusterConfig(),
              clusterData.getResourceConfig(resourceName), partitionName, state, priority));
          replicaGapCount--;
        }
      }
    }
    return toBeAssignedReplicas;
  }

  private static List<String> getPriorityOrderedStates(String resourceName, ResourceControllerDataProvider clusterData) {
    IdealState currentIdealState = clusterData.getIdealState(resourceName);
    Map<String, Integer> statePriorityMap =
        clusterData.getStateModelDef(currentIdealState.getStateModelDefRef())
            .getStatePriorityMap();

    List<String> priorityOrderedStates = new ArrayList<>(statePriorityMap.keySet());
    priorityOrderedStates.sort(Comparator.comparing(a -> statePriorityMap.getOrDefault(a, Integer.MAX_VALUE)));
    return priorityOrderedStates;
  }
}
