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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.apache.helix.model.ResourceAssignment;


/**
 * This class tracks the rebalance-related global cluster status.
 */
public class ClusterContext {
  // This estimation helps to ensure global partition count evenness
  private final int _estimatedMaxPartitionCount;
  // This estimation helps to ensure global top state replica count evenness
  private final int _estimatedMaxTopStateCount;
  // This estimation helps to ensure per-resource partition count evenness
  private final Map<String, Integer> _estimatedMaxPartitionByResource = new HashMap<>();
  // This estmations helps to ensure global resource usage evenness.
  private final float _estimatedMaxUtilization;

  // map{zoneName : map{resourceName : set(partitionNames)}}
  private Map<String, Map<String, Set<String>>> _assignmentForFaultZoneMap = new HashMap<>();
  // Records about the previous assignment
  // <ResourceName, ResourceAssignment contains the baseline assignment>
  private final Map<String, ResourceAssignment> _baselineAssignment;
  // <ResourceName, ResourceAssignment contains the best possible assignment>
  private final Map<String, ResourceAssignment> _bestPossibleAssignment;

  /**
   * Construct the cluster context based on the current instance status.
   * @param replicaSet All the partition replicas that are managed by the rebalancer
   * @param nodeSet All the active nodes that are managed by the rebalancer
   */
  ClusterContext(Set<AssignableReplica> replicaSet, Set<AssignableNode> nodeSet,
      Map<String, ResourceAssignment> baselineAssignment, Map<String, ResourceAssignment> bestPossibleAssignment) {
    int instanceCount = nodeSet.size();
    int totalReplicas = 0;
    int totalTopStateReplicas = 0;
    Map<String, Integer> totalUsage = new HashMap<>();
    Map<String, Integer> totalCapacity = new HashMap<>();

    for (Map.Entry<String, List<AssignableReplica>> entry : replicaSet.stream()
        .collect(Collectors.groupingBy(AssignableReplica::getResourceName))
        .entrySet()) {
      int replicas = entry.getValue().size();
      totalReplicas += replicas;

      int replicaCnt = Math.max(1, estimateAvgReplicaCount(replicas, instanceCount));
      _estimatedMaxPartitionByResource.put(entry.getKey(), replicaCnt);

      for (AssignableReplica replica : entry.getValue()) {
        if (replica.isReplicaTopState()) {
          totalTopStateReplicas += 1;
        }
        replica.getCapacity().entrySet().stream().forEach(capacityEntry -> totalUsage
            .computeIfPresent(capacityEntry.getKey(),
                (key, value) -> value + capacityEntry.getValue()));
      }
    }
    nodeSet.stream().forEach(node -> node.getMaxCapacity().entrySet().stream().forEach(
        capacityEntry -> totalCapacity.computeIfPresent(capacityEntry.getKey(),
            (key, value) -> value + capacityEntry.getValue())));

    if (totalCapacity.isEmpty()) {
      _estimatedMaxUtilization = 1f;
    } else {
      float estimatedMaxUsage = 0;
      for (String capacityKey : totalCapacity.keySet()) {
        int maxCapacity = totalCapacity.get(capacityKey);
        int usage = totalUsage.getOrDefault(capacityKey, 0);
        int utilization = (maxCapacity == 0) ? 1 : usage / maxCapacity;
        estimatedMaxUsage = Math.max(estimatedMaxUsage, utilization);
      }
      _estimatedMaxUtilization = estimatedMaxUsage;
    }
    _estimatedMaxPartitionCount = estimateAvgReplicaCount(totalReplicas, instanceCount);
    _estimatedMaxTopStateCount = estimateAvgReplicaCount(totalTopStateReplicas, instanceCount);
    _baselineAssignment = baselineAssignment;
    _bestPossibleAssignment = bestPossibleAssignment;
  }

  public Map<String, ResourceAssignment> getBaselineAssignment() {
    return _baselineAssignment == null || _baselineAssignment.isEmpty() ? Collections.emptyMap() : _baselineAssignment;
  }

  public Map<String, ResourceAssignment> getBestPossibleAssignment() {
    return _bestPossibleAssignment == null || _bestPossibleAssignment.isEmpty() ? Collections.emptyMap()
        : _bestPossibleAssignment;
  }

  public Map<String, Map<String, Set<String>>> getAssignmentForFaultZoneMap() {
    return _assignmentForFaultZoneMap;
  }

  public int getEstimatedMaxPartitionCount() {
    return _estimatedMaxPartitionCount;
  }

  public int getEstimatedMaxPartitionByResource(String resourceName) {
    return _estimatedMaxPartitionByResource.get(resourceName);
  }

  public int getEstimatedMaxTopStateCount() {
    return _estimatedMaxTopStateCount;
  }

  public float getEstimatedMaxUtilization() {
    return _estimatedMaxUtilization;
  }

  public Set<String> getPartitionsForResourceAndFaultZone(String resourceName, String faultZoneId) {
    return _assignmentForFaultZoneMap.getOrDefault(faultZoneId, Collections.emptyMap())
        .getOrDefault(resourceName, Collections.emptySet());
  }

  void addPartitionToFaultZone(String faultZoneId, String resourceName, String partition) {
    if (!_assignmentForFaultZoneMap.computeIfAbsent(faultZoneId, k -> new HashMap<>())
        .computeIfAbsent(resourceName, k -> new HashSet<>())
        .add(partition)) {
      throw new HelixException(
          String.format("Resource %s already has a replica from partition %s in fault zone %s", resourceName, partition,
              faultZoneId));
    }
  }

  boolean removePartitionFromFaultZone(String faultZoneId, String resourceName, String partition) {
    return _assignmentForFaultZoneMap.getOrDefault(faultZoneId, Collections.emptyMap())
        .getOrDefault(resourceName, Collections.emptySet())
        .remove(partition);
  }

  void setAssignmentForFaultZoneMap(Map<String, Map<String, Set<String>>> assignmentForFaultZoneMap) {
    _assignmentForFaultZoneMap = assignmentForFaultZoneMap;
  }

  private int estimateAvgReplicaCount(int replicaCount, int instanceCount) {
    // Use the floor to ensure evenness.
    // Note if we calculate estimation based on ceil, we might have some low usage participants.
    // For example, if the evaluation is between 1 and 2. While we use 2, many participants will be
    // allocated with 2 partitions. And the other participants only has 0 partitions. Otherwise,
    // if we use 1, most participant will have 1 partition assigned and several participant has 2
    // partitions. The later scenario is what we want to achieve.
    return (int) Math.floor((float) replicaCount / instanceCount);
  }
}
