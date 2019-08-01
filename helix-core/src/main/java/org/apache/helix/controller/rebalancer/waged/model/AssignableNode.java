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
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
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
  private static final Logger _logger = LoggerFactory.getLogger(AssignableNode.class.getName());

  // basic node information
  private final String _instanceName;
  private Set<String> _instanceTags;
  private String _faultZone;
  private Map<String, List<String>> _disabledPartitionsMap;
  private Map<String, Integer> _maxCapacity;
  private int _maxPartition;

  // proposed assignment tracking
  // <resource name, partition name>
  private Map<String, Set<String>> _currentAssignments;
  // <resource name, top state partition name>
  private Map<String, Set<String>> _currentTopStateAssignments;
  // <capacity key, capacity value>
  private Map<String, Integer> _currentCapacity;
  // runtime usage tracking
  private int _totalReplicaAssignmentCount;
  private float _highestCapacityUtilization;

  AssignableNode(ResourceControllerDataProvider clusterCache, String instanceName,
      Collection<AssignableReplica> existingAssignment) {
    _instanceName = instanceName;
    refresh(clusterCache, existingAssignment);
  }

  private void reset() {
    _currentAssignments = new HashMap<>();
    _currentTopStateAssignments = new HashMap<>();
    _currentCapacity = new HashMap<>();
    _totalReplicaAssignmentCount = 0;
    _highestCapacityUtilization = 0;
  }

  /**
   * Update the node with a ClusterDataCache. This resets the current assignment and recalculate currentCapacity.
   * NOTE: While this is required to be used in the constructor, this can also be used when the clusterCache needs to be
   * refreshed. This is under the assumption that the capacity mappings of InstanceConfig and ResourceConfig could
   * subject to changes. If the assumption is no longer true, this function should become private.
   *
   * @param clusterCache - the current cluster cache to initial the AssignableNode.
   */
  private void refresh(ResourceControllerDataProvider clusterCache,
      Collection<AssignableReplica> existingAssignment) {
    reset();

    InstanceConfig instanceConfig = clusterCache.getInstanceConfigMap().get(_instanceName);
    ClusterConfig clusterConfig = clusterCache.getClusterConfig();

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
          .format("Resource %s already has a replica from partition %s on this node",
              assignableReplica.getResourceName(), assignableReplica.getPartitionName()));
    } else {
      if (assignableReplica.isReplicaTopState()) {
        addToAssignmentRecord(assignableReplica, _currentTopStateAssignments);
      }
      _totalReplicaAssignmentCount += 1;
      assignableReplica.getCapacity().entrySet().stream()
          .forEach(entry -> updateCapacityAndUtilization(entry.getKey(), entry.getValue()));
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
      _logger.warn("Resource " + resourceName + " is not on this node. Ignore the release call.");
      return;
    }
    Set<String> partitions = _currentAssignments.get(resourceName);
    if (!partitions.contains(partitionName)) {
      _logger.warn(String
          .format("Resource %s does not have a replica from partition %s on this node",
              resourceName, partitionName));
      return;
    }

    partitions.remove(assignableReplica.getPartitionName());
    if (assignableReplica.isReplicaTopState()) {
      _currentTopStateAssignments.get(resourceName).remove(partitionName);
    }
    _totalReplicaAssignmentCount -= 1;
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
    return _totalReplicaAssignmentCount;
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
   *
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
        throw new HelixException(String
            .format("The domain configuration of instance %s is not configured", _instanceName));
      } else {
        StringBuilder faultZoneStringBuilder = new StringBuilder();
        for (String key : topologyDef) {
          if (!key.isEmpty()) {
            if (domainAsMap.containsKey(key)) {
              faultZoneStringBuilder.append(domainAsMap.get(key));
              faultZoneStringBuilder.append('/');
            } else {
              throw new HelixException(String.format(
                  "The domain configuration of instance %s is not complete. Type %s is not found.",
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
   * This function should only be used to assign a set of partitions that doesn't exist before.
   * Using this function avoids the overhead of updating capacity repeatedly.
   */
  private void assignNewBatch(Collection<AssignableReplica> replicas) {
    Map<String, Integer> totalPartitionCapacity = new HashMap<>();
    for (AssignableReplica replica : replicas) {
      addToAssignmentRecord(replica, _currentAssignments);
      if (replica.isReplicaTopState()) {
        addToAssignmentRecord(replica, _currentTopStateAssignments);
      }
      // increment the capacity requirement according to partition's capacity configure.
      for (Map.Entry<String, Integer> capacity : replica.getCapacity().entrySet()) {
        totalPartitionCapacity.compute(capacity.getKey(),
            (k, v) -> (v == null) ? capacity.getValue() : v + capacity.getValue());
      }
    }
    _totalReplicaAssignmentCount += replicas.size();

    // Update to the global state after all single replications' calculation is done.
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
  }

  @Override
  public int hashCode() {
    return _instanceName.hashCode();
  }
}
