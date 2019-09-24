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
  private final static float ERROR_MARGIN_FOR_ESTIMATED_MAX_COUNT = 1.1f;

  // This estimation helps to ensure global partition count evenness
  private final int _estimatedMaxPartitionCount;
  // This estimation helps to ensure global top state replica count evenness
  private final int _estimatedMaxTopStateCount;
  // This estimation helps to ensure per-resource partition count evenness
  private final Map<String, Integer> _estimatedMaxPartitionByResource = new HashMap<>();

  private Set<AssignableReplica> _allReplicas;
  // map{zoneName : map{resourceName : set(partitionNames)}}
  private Map<String, Map<String, Set<String>>> _assignmentForFaultZoneMap = new HashMap<>();
  // Records about the previous assignment
  // <ResourceName, ResourceAssignment contains the baseline assignment>
  private Map<String, ResourceAssignment> _baselineAssignment;
  // <ResourceName, ResourceAssignment contains the best possible assignment>
  private Map<String, ResourceAssignment> _bestPossibleAssignment;

  /**
   * Construct the cluster context based on the current instance status.
   * @param allReplicas All the partition replicas that are managed by the rebalancer
   * @param instanceCount The count of all the active instances that can be used to host partitions.
   */
  ClusterContext(Set<AssignableReplica> allReplicas, int instanceCount,
      Map<String, ResourceAssignment> baselineAssignment, Map<String, ResourceAssignment> bestPossibleAssignment) {
    int totalReplicas = 0;
    int totalTopStateReplicas = 0;
    _allReplicas = allReplicas;

    for (Map.Entry<String, List<AssignableReplica>> entry : allReplicas.stream()
        .collect(Collectors.groupingBy(AssignableReplica::getResourceName))
        .entrySet()) {
      int replicas = entry.getValue().size();
      totalReplicas += replicas;

      int replicaCnt = Math.max(1, estimateAvgReplicaCount(replicas, instanceCount));
      _estimatedMaxPartitionByResource.put(entry.getKey(), replicaCnt);

      totalTopStateReplicas += entry.getValue().stream().filter(AssignableReplica::isReplicaTopState).count();
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

  public void setBestPossibleAssignment(Map<String, ResourceAssignment> assignment) {
    _bestPossibleAssignment = assignment;
  }

  public void setBaselineAssignment(Map<String, ResourceAssignment> assignment) {
    _baselineAssignment = assignment;
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

  public Set<AssignableReplica> getAllReplicas() {
    return _allReplicas;
  }

  public int getEstimatedMaxTopStateCount() {
    return _estimatedMaxTopStateCount;
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
    return (int) Math.ceil((float) replicaCount / instanceCount * ERROR_MARGIN_FOR_ESTIMATED_MAX_COUNT);
  }
}
