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

import org.apache.helix.HelixException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.max;

/**
 * This class represents a possible allocation of the replication.
 * Note that any usage updates to the AssignableNode are not thread safe.
 */
public class AssignableNode {
  private static final Logger LOG = LoggerFactory.getLogger(AssignableNode.class.getName());

  // basic node information
  private final String _instanceName;
  private Set<String> _instanceTags;
  private String _faultZone;
  private Map<String, List<String>> _disabledPartitionsMap;
  private Map<String, Integer> _maxCapacity;
  private int _maxPartition; // maximum number of the partitions that can be assigned to the node.

  // proposed assignment tracking
  // <resource name, partition name set>
  private Map<String, Set<String>> _currentAssignments;
  // <resource name, top state partition name>
  private Map<String, Set<String>> _currentTopStateAssignments;
  // <capacity key, capacity value>
  private Map<String, Integer> _currentCapacity;
  // The maximum capacity utilization (0.0 - 1.0) across all the capacity categories.
  private float _highestCapacityUtilization;

  AssignableNode(ClusterConfig clusterConfig, InstanceConfig instanceConfig, String instanceName,
      Collection<AssignableReplica> existingAssignment) {
    _instanceName = instanceName;
    refresh(clusterConfig, instanceConfig, existingAssignment);
  }

  private void reset() {
    _currentAssignments = new HashMap<>();
    _currentTopStateAssignments = new HashMap<>();
    _currentCapacity = new HashMap<>();
    _highestCapacityUtilization = 0;
  }

  /**
   * Update the node with a ClusterDataCache. This resets the current assignment and recalculates currentCapacity.
   * NOTE: While this is required to be used in the constructor, this can also be used when the clusterCache needs to be
   * refreshed. This is under the assumption that the capacity mappings of InstanceConfig and ResourceConfig could
   * subject to change. If the assumption is no longer true, this function should become private.
   *
   * @param clusterConfig  - the Cluster Config of the cluster where the node is located
   * @param instanceConfig - the Instance Config of the node
   * @param existingAssignment - all the existing replicas that are current assigned to the node
   */
  private void refresh(ClusterConfig clusterConfig, InstanceConfig instanceConfig,
      Collection<AssignableReplica> existingAssignment) {
    reset();

    _currentCapacity.putAll(instanceConfig.getInstanceCapacityMap());
    _faultZone = computeFaultZone(clusterConfig, instanceConfig);
    _instanceTags = new HashSet<>(instanceConfig.getTags());
    _disabledPartitionsMap = instanceConfig.getDisabledPartitionsMap();
    _maxCapacity = instanceConfig.getInstanceCapacityMap();
    _maxPartition = clusterConfig.getMaxPartitionsPerInstance();

    assignNewBatch(existingAssignment);
  }

  /**
   * Assign a replica to the node.
   *
   * @param assignableReplica - the replica to be assigned
   */
  void assign(AssignableReplica assignableReplica) {
    if (!addToAssignmentRecord(assignableReplica, _currentAssignments)) {
      throw new HelixException(String
          .format("Resource %s already has a replica from partition %s on node %s",
              assignableReplica.getResourceName(), assignableReplica.getPartitionName(),
              getInstanceName()));
    } else {
      if (assignableReplica.isReplicaTopState()) {
        addToAssignmentRecord(assignableReplica, _currentTopStateAssignments);
      }
      assignableReplica.getCapacity().entrySet().stream().forEach(
          capacity -> updateCapacityAndUtilization(capacity.getKey(), capacity.getValue()));
    }
  }

  /**
   * Release a replica from the node.
   * If the replication is not on this node, the assignable node is not updated.
   *
   * @param assignableReplica - the replica to be released
   */
  void release(AssignableReplica assignableReplica) throws IllegalArgumentException {
    String resourceName = assignableReplica.getResourceName();
    String partitionName = assignableReplica.getPartitionName();

    // Check if the release is necessary
    if (!_currentAssignments.containsKey(resourceName)) {
      LOG.warn("Resource {} is not on node {}. Ignore the release call.", resourceName,
          getInstanceName());
      return;
    }
    Set<String> partitions = _currentAssignments.get(resourceName);
    if (!partitions.contains(partitionName)) {
      LOG.warn(String
          .format("Resource %s does not have a replica from partition %s on node %s", resourceName,
              partitionName, getInstanceName()));
      return;
    }

    partitions.remove(assignableReplica.getPartitionName());
    if (assignableReplica.isReplicaTopState()) {
      _currentTopStateAssignments.get(resourceName).remove(partitionName);
    }
    // Recalculate utilization because of release
    _highestCapacityUtilization = 0;
    assignableReplica.getCapacity().entrySet().stream()
        .forEach(entry -> updateCapacityAndUtilization(entry.getKey(), -1 * entry.getValue()));
  }

  public Map<String, Set<String>> getCurrentAssignmentsMap() {
    return _currentAssignments;
  }

  public Set<String> getCurrentAssignmentsByResource(String resource) {
    return _currentAssignments.getOrDefault(resource, Collections.emptySet());
  }

  public Set<String> getCurrentTopStateAssignmentsByResource(String resource) {
    return _currentTopStateAssignments.getOrDefault(resource, Collections.emptySet());
  }

  public int getTopStateAssignmentTotalSize() {
    return _currentTopStateAssignments.values().stream().mapToInt(Set::size).sum();
  }

  public int getCurrentAssignmentCount() {
    return _currentAssignments.values().stream().mapToInt(Set::size).sum();
  }

  public Map<String, Integer> getCurrentCapacity() {
    return _currentCapacity;
  }

  public float getHighestCapacityUtilization() {
    return _highestCapacityUtilization;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public Set<String> getInstanceTags() {
    return _instanceTags;
  }

  public String getFaultZone() {
    return _faultZone;
  }

  public Map<String, List<String>> getDisabledPartitionsMap() {
    return _disabledPartitionsMap;
  }

  public Map<String, Integer> getMaxCapacity() {
    return _maxCapacity;
  }

  public int getMaxPartition() {
    return _maxPartition;
  }

  /**
   * Computes the fault zone id based on the domain and fault zone type when topology is enabled. For example, when
   * the domain is "zone=2, instance=testInstance" and the fault zone type is "zone", this function returns "2".
   * If cannot find the fault zone id, this function leaves the fault zone id as the instance name.
   * TODO merge this logic with Topology.java tree building logic.
   * For now, the WAGED rebalancer has a more strict topology def requirement.
   * Any missing field will cause an invalid topology config exception.
   */
  private String computeFaultZone(ClusterConfig clusterConfig, InstanceConfig instanceConfig) {
    if (clusterConfig.isTopologyAwareEnabled()) {
      String topologyStr = clusterConfig.getTopology();
      String faultZoneType = clusterConfig.getFaultZoneType();
      if (topologyStr == null || faultZoneType == null) {
        throw new HelixException("Fault zone or cluster topology information is not configured.");
      }

      String[] topologyDef = topologyStr.trim().split("/");
      if (topologyDef.length == 0 || Arrays.stream(topologyDef)
          .noneMatch(type -> type.equals(faultZoneType))) {
        throw new HelixException(
            "The configured topology definition is empty or does not contain the fault zone type.");
      }

      Map<String, String> domainAsMap = instanceConfig.getDomainAsMap();
      if (domainAsMap == null) {
        throw new HelixException(
            String.format("The domain configuration of node %s is not configured", _instanceName));
      } else {
        StringBuilder faultZoneStringBuilder = new StringBuilder();
        for (String key : topologyDef) {
          if (!key.isEmpty()) {
            if (domainAsMap.containsKey(key)) {
              faultZoneStringBuilder.append(domainAsMap.get(key));
              faultZoneStringBuilder.append('/');
            } else {
              throw new HelixException(String.format(
                  "The domain configuration of node %s is not complete. Type %s is not found.",
                  _instanceName, key));
            }
            if (key.equals(faultZoneType)) {
              break;
            }
          }
        }
        return faultZoneStringBuilder.toString();
      }
    } else {
      // For backward compatibility
      String zoneId = instanceConfig.getZoneId();
      return zoneId == null ? instanceConfig.getInstanceName() : zoneId;
    }
  }

  /**
   * This function should only be used to assign a set of new partitions that are not allocated on this node.
   * Using this function avoids the overhead of updating capacity repeatedly.
   */
  private void assignNewBatch(Collection<AssignableReplica> replicas) {
    Map<String, Integer> totalPartitionCapacity = new HashMap<>();
    for (AssignableReplica replica : replicas) {
      addToAssignmentRecord(replica, _currentAssignments);
      if (replica.isReplicaTopState()) {
        addToAssignmentRecord(replica, _currentTopStateAssignments);
      }
      // increment the capacity requirement according to partition's capacity configuration.
      for (Map.Entry<String, Integer> capacity : replica.getCapacity().entrySet()) {
        totalPartitionCapacity.compute(capacity.getKey(),
            (key, totalValue) -> (totalValue == null) ?
                capacity.getValue() :
                totalValue + capacity.getValue());
      }
    }

    // Update the global state after all single replications' calculation is done.
    for (String key : totalPartitionCapacity.keySet()) {
      updateCapacityAndUtilization(key, totalPartitionCapacity.get(key));
    }
  }

  private boolean addToAssignmentRecord(AssignableReplica replica,
      Map<String, Set<String>> currentAssignments) {
    return currentAssignments.computeIfAbsent(replica.getResourceName(), k -> new HashSet<>())
        .add(replica.getPartitionName());
  }

  private void updateCapacityAndUtilization(String capacityKey, int valueToSubtract) {
    if (_currentCapacity.containsKey(capacityKey)) {
      int newCapacity = _currentCapacity.get(capacityKey) - valueToSubtract;
      _currentCapacity.put(capacityKey, newCapacity);
      // For the purpose of constraint calculation, the max utilization cannot be larger than 100%.
      float utilization = Math.min(
          (float) (_maxCapacity.get(capacityKey) - newCapacity) / _maxCapacity.get(capacityKey), 1);
      _highestCapacityUtilization = max(_highestCapacityUtilization, utilization);
    }
    // else if the capacityKey does not exist in the capacity map, this method essentially becomes
    // a NOP; in other words, this node will be treated as if it has unlimited capacity.
  }

  @Override
  public int hashCode() {
    return _instanceName.hashCode();
  }
}
