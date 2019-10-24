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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

  // Immutable Instance Properties
  private final String _instanceName;
  private final String _faultZone;
  // maximum number of the partitions that can be assigned to the instance.
  private final int _maxPartition;
  private final ImmutableSet<String> _instanceTags;
  private final ImmutableMap<String, List<String>> _disabledPartitionsMap;
  private final ImmutableMap<String, Integer> _maxAllowedCapacity;

  // Mutable (Dynamic) Instance Properties
  // A map of <resource name, <partition name, replica>> that tracks the replicas assigned to the
  // node.
  private Map<String, Map<String, AssignableReplica>> _currentAssignedReplicaMap;
  // A map of <capacity key, capacity value> that tracks the current available node capacity
  private Map<String, Integer> _remainingCapacity;

  /**
   * Update the node with a ClusterDataCache. This resets the current assignment and recalculates
   * currentCapacity.
   * NOTE: While this is required to be used in the constructor, this can also be used when the
   * clusterCache needs to be
   * refreshed. This is under the assumption that the capacity mappings of InstanceConfig and
   * ResourceConfig could
   * subject to change. If the assumption is no longer true, this function should become private.
   */
  AssignableNode(ClusterConfig clusterConfig, InstanceConfig instanceConfig, String instanceName) {
    _instanceName = instanceName;
    Map<String, Integer> instanceCapacity = fetchInstanceCapacity(clusterConfig, instanceConfig);
    _faultZone = computeFaultZone(clusterConfig, instanceConfig);
    _instanceTags = ImmutableSet.copyOf(instanceConfig.getTags());
    _disabledPartitionsMap = ImmutableMap.copyOf(instanceConfig.getDisabledPartitionsMap());
    // make a copy of max capacity
    _maxAllowedCapacity = ImmutableMap.copyOf(instanceCapacity);
    _remainingCapacity = new HashMap<>(instanceCapacity);
    _maxPartition = clusterConfig.getMaxPartitionsPerInstance();
    _currentAssignedReplicaMap = new HashMap<>();
  }

  /**
   * This function should only be used to assign a set of new partitions that are not allocated on
   * this node. It's because the any exception could occur at the middle of batch assignment and the
   * previous finished assignment cannot be reverted
   * Using this function avoids the overhead of updating capacity repeatedly.
   */
  void assignInitBatch(Collection<AssignableReplica> replicas) {
    Map<String, Integer> totalPartitionCapacity = new HashMap<>();
    for (AssignableReplica replica : replicas) {
      // TODO: the exception could occur in the middle of for loop and the previous added records cannot be reverted
      addToAssignmentRecord(replica);
      // increment the capacity requirement according to partition's capacity configuration.
      for (Map.Entry<String, Integer> capacity : replica.getCapacity().entrySet()) {
        totalPartitionCapacity.compute(capacity.getKey(),
            (key, totalValue) -> (totalValue == null) ? capacity.getValue()
                : totalValue + capacity.getValue());
      }
    }

    // Update the global state after all single replications' calculation is done.
    for (String capacityKey : totalPartitionCapacity.keySet()) {
      updateCapacityAndUtilization(capacityKey, totalPartitionCapacity.get(capacityKey));
    }
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
    if (!partitionMap.containsKey(partitionName) || !partitionMap.get(partitionName)
        .equals(replica)) {
      LOG.warn("Replica {} is not assigned to node {}. Ignore the release call.",
          replica.toString(), getInstanceName());
      return;
    }

    AssignableReplica removedReplica = partitionMap.remove(partitionName);
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
  public Map<String, Integer> getRemainingCapacity() {
    return _remainingCapacity;
  }

  /**
   * @return A map of <capacity category, capacity number> that describes the max capacity of the
   *         node.
   */
  public Map<String, Integer> getMaxCapacity() {
    return _maxAllowedCapacity;
  }

  /**
   * Return the most concerning capacity utilization number for evenly partition assignment.
   * The method dynamically calculates the projected highest utilization number among all the
   * capacity categories assuming the new capacity usage is added to the node.
   * For example, if the current node usage is {CPU: 0.9, MEM: 0.4, DISK: 0.6}. Then this call shall
   * return 0.9.
   * @param newUsage the proposed new additional capacity usage.
   * @return The highest utilization number of the node among all the capacity category.
   */
  public float getProjectedHighestUtilization(Map<String, Integer> newUsage) {
    float highestCapacityUtilization = 0;
    for (String capacityKey : _maxAllowedCapacity.keySet()) {
      float capacityValue = _maxAllowedCapacity.get(capacityKey);
      float utilization = (capacityValue - _remainingCapacity.get(capacityKey) + newUsage
          .getOrDefault(capacityKey, 0)) / capacityValue;
      highestCapacityUtilization = Math.max(highestCapacityUtilization, utilization);
    }
    return highestCapacityUtilization;
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
   * If cannot find the fault zone type, this function leaves the fault zone id as the instance name.
   * Note the WAGED rebalancer does not require full topology tree to be created. So this logic is
   * simpler than the CRUSH based rebalancer.
   */
  private String computeFaultZone(ClusterConfig clusterConfig, InstanceConfig instanceConfig) {
    if (!clusterConfig.isTopologyAwareEnabled()) {
      // Instance name is the default fault zone if topology awareness is false.
      return instanceConfig.getInstanceName();
    }
    String topologyStr = clusterConfig.getTopology();
    String faultZoneType = clusterConfig.getFaultZoneType();
    if (topologyStr == null || faultZoneType == null) {
      LOG.debug("Topology configuration is not complete. Topology define: {}, Fault Zone Type: {}",
          topologyStr, faultZoneType);
      // Use the instance name, or the deprecated ZoneId field (if exists) as the default fault
      // zone.
      String zoneId = instanceConfig.getZoneId();
      return zoneId == null ? instanceConfig.getInstanceName() : zoneId;
    } else {
      // Get the fault zone information from the complete topology definition.
      String[] topologyKeys = topologyStr.trim().split("/");
      if (topologyKeys.length == 0 || Arrays.stream(topologyKeys)
          .noneMatch(type -> type.equals(faultZoneType))) {
        throw new HelixException(
            "The configured topology definition is empty or does not contain the fault zone type.");
      }

      Map<String, String> domainAsMap = instanceConfig.getDomainAsMap();
      StringBuilder faultZoneStringBuilder = new StringBuilder();
      for (String key : topologyKeys) {
        if (!key.isEmpty()) {
          // if a key does not exist in the instance domain config, apply the default domain value.
          faultZoneStringBuilder.append(domainAsMap.getOrDefault(key, "Default_" + key));
          if (key.equals(faultZoneType)) {
            break;
          } else {
            faultZoneStringBuilder.append('/');
          }
        }
      }
      return faultZoneStringBuilder.toString();
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

  private void updateCapacityAndUtilization(String capacityKey, int usage) {
    if (!_remainingCapacity.containsKey(capacityKey)) {
      //if the capacityKey belongs to replicas does not exist in the instance's capacity,
      // it will be treated as if it has unlimited capacity of that capacityKey
      return;
    }
    int newCapacity = _remainingCapacity.get(capacityKey) - usage;
    _remainingCapacity.put(capacityKey, newCapacity);
  }

  /**
   * Get and validate the instance capacity from instance config.
   * @throws HelixException if any required capacity key is not configured in the instance config.
   */
  private Map<String, Integer> fetchInstanceCapacity(ClusterConfig clusterConfig,
      InstanceConfig instanceConfig) {
    // Fetch the capacity of instance from 2 possible sources according to the following priority.
    // 1. The instance capacity that is configured in the instance config.
    // 2. If the default instance capacity that is configured in the cluster config contains more capacity keys, fill the capacity map with those additional values.
    Map<String, Integer> instanceCapacity =
        new HashMap<>(clusterConfig.getDefaultInstanceCapacityMap());
    instanceCapacity.putAll(instanceConfig.getInstanceCapacityMap());

    List<String> requiredCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    // All the required keys must exist in the instance config.
    if (!instanceCapacity.keySet().containsAll(requiredCapacityKeys)) {
      throw new HelixException(String.format(
          "The required capacity keys: %s are not fully configured in the instance: %s, capacity map: %s.",
          requiredCapacityKeys.toString(), _instanceName, instanceCapacity.toString()));
    }
    // Remove all the non-required capacity items from the map.
    instanceCapacity.keySet().retainAll(requiredCapacityKeys);

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

  @Override
  public String toString() {
    return _instanceName;
  }
}
