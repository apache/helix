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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ResourceAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class tracks the rebalance-related global cluster status.
 */
public class ClusterContext {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterContext.class.getName());
  // This estimation helps to ensure global partition count evenness
  private final int _estimatedMaxPartitionCount;
  // This estimation helps to ensure global top state replica count evenness
  private final int _estimatedMaxTopStateCount;
  // This estimation helps to ensure per-resource partition count evenness
  private final Map<String, Integer> _estimatedMaxPartitionByResource = new HashMap<>();
  // This estimation helps to ensure global resource usage evenness.
  private final float _estimatedMaxUtilization;
  // This estimation helps to ensure global resource top state usage evenness.
  private final float _estimatedTopStateMaxUtilization;

  // map{zoneName : map{resourceName : set(partitionNames)}}
  private Map<String, Map<String, Set<String>>> _assignmentForFaultZoneMap = new HashMap<>();
  // Records about the previous assignment
  // <ResourceName, ResourceAssignment contains the baseline assignment>
  private final Map<String, ResourceAssignment> _baselineAssignment;
  // <ResourceName, ResourceAssignment contains the best possible assignment>
  private final Map<String, ResourceAssignment> _bestPossibleAssignment;
  // Estimate remaining capacity after assignment. Used to compute score when sorting replicas.
  private final Map<String, Integer> _estimateUtilizationMap;
  // Cluster total capacity. Used to compute score when sorting replicas.
  private final Map<String, Integer> _clusterCapacityMap;
  private final List<String> _preferredScoringKeys;
  private final String _clusterName;
  /**
   * Construct the cluster context based on the current instance status.
   * @param replicaSet All the partition replicas that are managed by the rebalancer
   * @param nodeSet All the active nodes that are managed by the rebalancer
   */
  ClusterContext(Set<AssignableReplica> replicaSet, Set<AssignableNode> nodeSet,
                 Map<String, ResourceAssignment> baselineAssignment, Map<String, ResourceAssignment> bestPossibleAssignment) {
    this(replicaSet, nodeSet, baselineAssignment, bestPossibleAssignment, null);
  }

  ClusterContext(Set<AssignableReplica> replicaSet, Set<AssignableNode> nodeSet,
                 Map<String, ResourceAssignment> baselineAssignment, Map<String, ResourceAssignment> bestPossibleAssignment,
                 ClusterConfig clusterConfig) {
    int instanceCount = nodeSet.size();
    int totalReplicas = 0;
    int totalTopStateReplicas = 0;
    Map<String, Integer> totalUsage = new HashMap<>();
    Map<String, Integer> totalTopStateUsage = new HashMap<>();
    Map<String, Integer> totalCapacity = new HashMap<>();
    _preferredScoringKeys = Optional.ofNullable(clusterConfig).map(ClusterConfig::getPreferredScoringKeys).orElse(null);
    _clusterName = Optional.ofNullable(clusterConfig).map(ClusterConfig::getClusterName).orElse(null);

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
          replica.getCapacity().forEach(
              (key, value) -> totalTopStateUsage.compute(key, (k, v) -> (v == null) ? value : (v + value)));
        }
        replica.getCapacity().forEach(
            (key, value) -> totalUsage.compute(key, (k, v) -> (v == null) ? value : (v + value)));
      }
    }
    nodeSet.forEach(node -> node.getMaxCapacity().forEach(
        (key, value) -> totalCapacity.compute(key, (k, v) -> (v == null) ? value : (v + value))));

    // TODO: these variables correspond to one constraint each, and may become unnecessary if the
    // constraints are not used. A better design is to make them pluggable.
    if (totalCapacity.isEmpty()) {
      // If no capacity is configured, we treat the cluster as fully utilized.
      _estimatedMaxUtilization = 1f;
      _estimatedTopStateMaxUtilization = 1f;
      _estimateUtilizationMap = Collections.emptyMap();
      _clusterCapacityMap = Collections.emptyMap();
    } else {
      _estimatedMaxUtilization = estimateMaxUtilization(totalCapacity, totalUsage, _preferredScoringKeys);
      _estimatedTopStateMaxUtilization = estimateMaxUtilization(totalCapacity, totalTopStateUsage, _preferredScoringKeys);
      _estimateUtilizationMap = estimateUtilization(totalCapacity, totalUsage);
      _clusterCapacityMap = Collections.unmodifiableMap(totalCapacity);
    }
    LOG.info(
        "clusterName: {}, preferredScoringKeys: {}, estimatedMaxUtilization: {}, estimatedTopStateMaxUtilization: {}",
        _clusterName, _preferredScoringKeys, _estimatedMaxUtilization,
        _estimatedTopStateMaxUtilization);
    _estimatedMaxPartitionCount = estimateAvgReplicaCount(totalReplicas, instanceCount);
    _estimatedMaxTopStateCount = estimateAvgReplicaCount(totalTopStateReplicas, instanceCount);
    _baselineAssignment = baselineAssignment;
    _bestPossibleAssignment = bestPossibleAssignment;
  }


  /**
   * Get List of preferred scoring keys if set.
   *
   * @return PreferredScoringKeys which is used in computation of evenness score
   */
  public List<String> getPreferredScoringKeys() {
    return _preferredScoringKeys;
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

  public float getEstimatedTopStateMaxUtilization() {
    return _estimatedTopStateMaxUtilization;
  }

  public Map<String, Integer> getEstimateUtilizationMap() {
    return _estimateUtilizationMap;
  }

  public Map<String, Integer> getClusterCapacityMap() {
    return _clusterCapacityMap;
  }

  public String getClusterName() {
    return _clusterName;
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

  private static int estimateAvgReplicaCount(int replicaCount, int instanceCount) {
    // Use the floor to ensure evenness.
    // Note if we calculate estimation based on ceil, we might have some low usage participants.
    // For example, if the evaluation is between 1 and 2. While we use 2, many participants will be
    // allocated with 2 partitions. And the other participants only has 0 partitions. Otherwise,
    // if we use 1, most participant will have 1 partition assigned and several participant has 2
    // partitions. The later scenario is what we want to achieve.
    return (int) Math.floor((float) replicaCount / instanceCount);
  }

  /**
   * Estimates the max utilization number from all capacity categories and their usages.
   * If the list of preferredScoringKeys is specified then max utilization number is computed based op the
   * specified capacity category (keys) in the list only.
   *
   * For example, if totalCapacity is {CPU: 0.6, MEM: 0.7, DISK: 0.9}, totalUsage is {CPU: 0.1, MEM: 0.2, DISK: 0.3},
   * preferredScoringKeys: [ CPU ]. Then this call shall return 0.16. If preferredScoringKeys
   * is not specified, this call returns 0.33 which would be the max utilization for the DISK.
   *
   * @param totalCapacity        Sum total of max capacity of all active nodes managed by a rebalancer
   * @param totalUsage           Sum total of capacity usage of all partition replicas that are managed by the rebalancer
   * @param preferredScoringKeys if provided, the max utilization will be calculated based on
   *                             the supplied keys only, else across all capacity categories.
   * @return The max utilization number from the specified capacity categories.
   */

  private static float estimateMaxUtilization(Map<String, Integer> totalCapacity,
                                              Map<String, Integer> totalUsage,
                                              List<String> preferredScoringKeys) {
    float estimatedMaxUsage = 0;
    Set<String> capacityKeySet = totalCapacity.keySet();
    if (preferredScoringKeys != null && preferredScoringKeys.size() != 0 && capacityKeySet.contains(preferredScoringKeys.get(0))) {
      capacityKeySet = preferredScoringKeys.stream().collect(Collectors.toSet());
    }
    for (String capacityKey : capacityKeySet) {
      int maxCapacity = totalCapacity.get(capacityKey);
      int usage = totalUsage.getOrDefault(capacityKey, 0);
      float utilization = (maxCapacity == 0) ? 1 : (float) usage / maxCapacity;
      estimatedMaxUsage = Math.max(estimatedMaxUsage, utilization);
    }

    return estimatedMaxUsage;
  }

  private static Map<String, Integer> estimateUtilization(Map<String, Integer> totalCapacity,
      Map<String, Integer> totalUsage) {
    Map<String, Integer> estimateUtilization = new HashMap<>();
    for (String capacityKey : totalCapacity.keySet()) {
      int maxCapacity = totalCapacity.get(capacityKey);
      int usage = totalUsage.getOrDefault(capacityKey, 0);
      estimateUtilization.put(capacityKey, maxCapacity - usage);
    }

    return Collections.unmodifiableMap(estimateUtilization);
  }
}
