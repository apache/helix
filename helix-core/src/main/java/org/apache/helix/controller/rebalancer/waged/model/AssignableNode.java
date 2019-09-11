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

import static java.lang.Math.max;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a possible allocation of the replication.
 * Note that any usage updates to the AssignableNode are not thread safe.
 */
public class AssignableNode implements Comparable<AssignableNode> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignableNode.class.getName());

  // basic node information
  private final String _instanceName;
  private Set<String> _instanceTags;
  private String _faultZone;
  private Map<String, List<String>> _disabledPartitionsMap;
  private Map<String, Integer> _maxCapacity;
  private int _maxPartition; // maximum number of the partitions that can be assigned to the node.

  // A map of <resource name, <partition name, replica>> that tracks the replicas assigned to the
  // node.
  private Map<String, Map<String, AssignableReplica>> _currentAssignedReplicaMap;
  // A map of <capacity key, capacity value> that tracks the current available node capacity
  private Map<String, Integer> _currentCapacityMap;
  // The maximum capacity utilization (0.0 - 1.0) across all the capacity categories.
  private float _highestCapacityUtilization;

  /**
   * @param clusterConfig
   * @param instanceConfig
   * @param instanceName
   * @param existingAssignment A collection of replicas that have been pre-allocated to the node.
   */
  AssignableNode(ClusterConfig clusterConfig, InstanceConfig instanceConfig, String instanceName,
      Collection<AssignableReplica> existingAssignment) {
    _instanceName = instanceName;
    refresh(clusterConfig, instanceConfig, existingAssignment);
  }

  private void reset() {
    _currentAssignedReplicaMap = new HashMap<>();
    _currentCapacityMap = new HashMap<>();
    _highestCapacityUtilization = 0;
  }

  /**
   * Update the node with a ClusterDataCache. This resets the current assignment and recalculates
   * currentCapacity.
   * NOTE: While this is required to be used in the constructor, this can also be used when the
   * clusterCache needs to be
   * refreshed. This is under the assumption that the capacity mappings of InstanceConfig and
   * ResourceConfig could
   * subject to change. If the assumption is no longer true, this function should become private.
   * @param clusterConfig - the Cluster Config of the cluster where the node is located
   * @param instanceConfig - the Instance Config of the node
   * @param existingAssignment - all the existing replicas that are current assigned to the node
   */
  private void refresh(ClusterConfig clusterConfig, InstanceConfig instanceConfig,
      Collection<AssignableReplica> existingAssignment) {
    reset();

    Map<String, Integer> instanceCapacity = fetchInstanceCapacity(clusterConfig, instanceConfig);
    _currentCapacityMap.putAll(instanceCapacity);
    _faultZone = computeFaultZone(clusterConfig, instanceConfig);
    _instanceTags = new HashSet<>(instanceConfig.getTags());
    _disabledPartitionsMap = instanceConfig.getDisabledPartitionsMap();
    _maxCapacity = instanceCapacity;
    _maxPartition = clusterConfig.getMaxPartitionsPerInstance();

    assignNewBatch(existingAssignment);
  }

  /**
   * Assign a replica to the node.
   * @param assignableReplica - the replica to be assigned
   */
  void assign(AssignableReplica assignableReplica) {
    addToAssignmentRecord(assignableReplica);
    assignableReplica.getCapacity().entrySet().stream()
        .forEach(capacity -> updateCapacityAndUtilization(capacity.getKey(), capacity.getValue()));
  }

  /**
   * Release a replica from the node.
   * If the replication is not on this node, the assignable node is not updated.
   * @param replica - the replica to be released
   */
  void release(AssignableReplica replica) throws IllegalArgumentException {
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();

    // Check if the release is necessary
    if (!_currentAssignedReplicaMap.containsKey(resourceName)) {
      LOG.warn("Resource {} is not on node {}. Ignore the release call.", resourceName,
          getInstanceName());
      return;
    }

    Map<String, AssignableReplica> partitionMap = _currentAssignedReplicaMap.get(resourceName);
    if (!partitionMap.containsKey(partitionName)
        || !partitionMap.get(partitionName).equals(replica)) {
      LOG.warn("Replica {} is not assigned to node {}. Ignore the release call.",
          replica.toString(), getInstanceName());
      return;
    }

    AssignableReplica removedReplica = partitionMap.remove(partitionName);
    // Recalculate utilization because of release
    _highestCapacityUtilization = 0;
    removedReplica.getCapacity().entrySet().stream()
        .forEach(entry -> updateCapacityAndUtilization(entry.getKey(), -1 * entry.getValue()));
  }

  /**
   * @return A set of all assigned replicas on the node.
   */
  public Set<AssignableReplica> getAssignedReplicas() {
    return _currentAssignedReplicaMap.values().stream()
        .flatMap(replicaMap -> replicaMap.values().stream()).collect(Collectors.toSet());
  }

  /**
   * @return The current assignment in a map of <resource name, set of partition names>
   */
  public Map<String, Set<String>> getAssignedPartitionsMap() {
    Map<String, Set<String>> assignmentMap = new HashMap<>();
    for (String resourceName : _currentAssignedReplicaMap.keySet()) {
      assignmentMap.put(resourceName, _currentAssignedReplicaMap.get(resourceName).keySet());
    }
    return assignmentMap;
  }

  /**
   * @param resource Resource name
   * @return A set of the current assigned replicas' partition names in the specified resource.
   */
  public Set<String> getAssignedPartitionsByResource(String resource) {
    return _currentAssignedReplicaMap.getOrDefault(resource, Collections.emptyMap()).keySet();
  }

  /**
   * @param resource Resource name
   * @return A set of the current assigned replicas' partition names with the top state in the
   *         specified resource.
   */
  public Set<String> getAssignedTopStatePartitionsByResource(String resource) {
    return _currentAssignedReplicaMap.getOrDefault(resource, Collections.emptyMap()).entrySet()
        .stream().filter(partitionEntry -> partitionEntry.getValue().isReplicaTopState())
        .map(partitionEntry -> partitionEntry.getKey()).collect(Collectors.toSet());
  }

  /**
   * @return The total count of assigned top state partitions.
   */
  public int getAssignedTopStatePartitionsCount() {
    return (int) _currentAssignedReplicaMap.values().stream()
        .flatMap(replicaMap -> replicaMap.values().stream())
        .filter(AssignableReplica::isReplicaTopState).count();
  }

  /**
   * @return The total count of assigned replicas.
   */
  public int getAssignedReplicaCount() {
    return _currentAssignedReplicaMap.values().stream().mapToInt(Map::size).sum();
  }

  /**
   * @return The current available capacity.
   */
  public Map<String, Integer> getCurrentCapacity() {
    return _currentCapacityMap;
  }

  /**
   * Return the most concerning capacity utilization number for evenly partition assignment.
   * The method dynamically returns the highest utilization number among all the capacity
   * categories.
   * For example, if the current node usage is {CPU: 0.9, MEM: 0.4, DISK: 0.6}. Then this call shall
   * return 0.9.
   *
   * @return The highest utilization number of the node among all the capacity category.
   */
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

  public boolean hasFaultZone() {
    return _faultZone != null;
  }

  /**
   * @return A map of <resource name, set of partition names> contains all the partitions that are
   *         disabled on the node.
   */
  public Map<String, List<String>> getDisabledPartitionsMap() {
    return _disabledPartitionsMap;
  }

  /**
   * @return A map of <capacity category, capacity number> that describes the max capacity of the
   *         node.
   */
  public Map<String, Integer> getMaxCapacity() {
    return _maxCapacity;
  }

  /**
   * @return The max partition count that are allowed to be allocated on the node.
   */
  public int getMaxPartition() {
    return _maxPartition;
  }

  /**
   * Computes the fault zone id based on the domain and fault zone type when topology is enabled.
   * For example, when
   * the domain is "zone=2, instance=testInstance" and the fault zone type is "zone", this function
   * returns "2".
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
      if (topologyDef.length == 0
          || Arrays.stream(topologyDef).noneMatch(type -> type.equals(faultZoneType))) {
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
   * This function should only be used to assign a set of new partitions that are not allocated on
   * this node.
   * Using this function avoids the overhead of updating capacity repeatedly.
   */
  private void assignNewBatch(Collection<AssignableReplica> replicas) {
    Map<String, Integer> totalPartitionCapacity = new HashMap<>();
    for (AssignableReplica replica : replicas) {
      addToAssignmentRecord(replica);
      // increment the capacity requirement according to partition's capacity configuration.
      for (Map.Entry<String, Integer> capacity : replica.getCapacity().entrySet()) {
        totalPartitionCapacity.compute(capacity.getKey(),
            (key, totalValue) -> (totalValue == null) ? capacity.getValue()
                : totalValue + capacity.getValue());
      }
    }

    // Update the global state after all single replications' calculation is done.
    for (String key : totalPartitionCapacity.keySet()) {
      updateCapacityAndUtilization(key, totalPartitionCapacity.get(key));
    }
  }

  /**
   * @throws HelixException if the replica has already been assigned to the node.
   */
  private void addToAssignmentRecord(AssignableReplica replica) {
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();
    if (_currentAssignedReplicaMap.containsKey(resourceName)
        && _currentAssignedReplicaMap.get(resourceName).containsKey(partitionName)) {
      throw new HelixException(String.format(
          "Resource %s already has a replica with state %s from partition %s on node %s",
          replica.getResourceName(), replica.getReplicaState(), replica.getPartitionName(),
          getInstanceName()));
    } else {
      _currentAssignedReplicaMap.computeIfAbsent(resourceName, key -> new HashMap<>())
          .put(partitionName, replica);
    }
  }

  private void updateCapacityAndUtilization(String capacityKey, int valueToSubtract) {
    if (_currentCapacityMap.containsKey(capacityKey)) {
      int newCapacity = _currentCapacityMap.get(capacityKey) - valueToSubtract;
      _currentCapacityMap.put(capacityKey, newCapacity);
      // For the purpose of constraint calculation, the max utilization cannot be larger than 100%.
      float utilization = Math.min(
          (float) (_maxCapacity.get(capacityKey) - newCapacity) / _maxCapacity.get(capacityKey), 1);
      _highestCapacityUtilization = max(_highestCapacityUtilization, utilization);
    }
    // else if the capacityKey does not exist in the capacity map, this method essentially becomes
    // a NOP; in other words, this node will be treated as if it has unlimited capacity.
  }

  /**
   * Get and validate the instance capacity from instance config.
   *
   * @throws HelixException if any required capacity key is not configured in the instance config.
   */
  private Map<String, Integer> fetchInstanceCapacity(ClusterConfig clusterConfig,
      InstanceConfig instanceConfig) {
    List<String> requiredCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    Map<String, Integer> instanceCapacity = instanceConfig.getInstanceCapacityMap();
    if (instanceCapacity.isEmpty()) {
      instanceCapacity = clusterConfig.getDefaultInstanceCapacityMap();
    }
    // Remove all the non-required capacity items from the map.
    instanceCapacity.keySet().retainAll(requiredCapacityKeys);
    // All the required keys must exist in the instance config.
    if (!instanceCapacity.keySet().containsAll(requiredCapacityKeys)) {
      throw new HelixException(String.format(
          "The required capacity keys %s are not fully configured in the instance %s capacity map %s.",
          requiredCapacityKeys.toString(), _instanceName, instanceCapacity.toString()));
    }
    return instanceCapacity;
  }

  @Override
  public int hashCode() {
    return _instanceName.hashCode();
  }

  @Override
  public int compareTo(AssignableNode o) {
    return _instanceName.compareTo(o.getInstanceName());
  }
}
