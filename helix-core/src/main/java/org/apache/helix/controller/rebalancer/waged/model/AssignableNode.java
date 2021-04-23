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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.HelixException;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
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
  private Map<String, Integer> _remainingTopStateCapacity;

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
    _remainingTopStateCapacity = new HashMap<>(instanceCapacity);
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
    Map<String, Integer> totalTopStatePartitionCapacity = new HashMap<>();
    Map<String, Integer> totalPartitionCapacity = new HashMap<>();
    for (AssignableReplica replica : replicas) {
      // TODO: the exception could occur in the middle of for loop and the previous added records cannot be reverted
      addToAssignmentRecord(replica);
      // increment the capacity requirement according to partition's capacity configuration.
      for (Map.Entry<String, Integer> capacity : replica.getCapacity().entrySet()) {
        if (replica.isReplicaTopState()) {
          totalTopStatePartitionCapacity.compute(capacity.getKey(),
              (key, totalValue) -> (totalValue == null) ? capacity.getValue()
                  : totalValue + capacity.getValue());
        }
        totalPartitionCapacity.compute(capacity.getKey(),
            (key, totalValue) -> (totalValue == null) ? capacity.getValue()
                : totalValue + capacity.getValue());
      }
    }

    // Update the global state after all single replications' calculation is done.
    updateRemainingCapacity(totalTopStatePartitionCapacity, _remainingTopStateCapacity, false);
    updateRemainingCapacity(totalPartitionCapacity, _remainingCapacity, false);
  }

  /**
   * Assign a replica to the node.
   * @param assignableReplica - the replica to be assigned
   */
  void assign(AssignableReplica assignableReplica) {
    addToAssignmentRecord(assignableReplica);
    updateRemainingCapacity(assignableReplica.getCapacity(), _remainingCapacity, false);
    if (assignableReplica.isReplicaTopState()) {
      updateRemainingCapacity(assignableReplica.getCapacity(), _remainingTopStateCapacity, false);
    }
  }

  /**
   * Release a replica from the node.
   * If the replication is not on this node, the assignable node is not updated.
   * @param replica - the replica to be released
   */
  void release(AssignableReplica replica)
      throws IllegalArgumentException {
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
    updateRemainingCapacity(removedReplica.getCapacity(), _remainingCapacity, true);
    if (removedReplica.isReplicaTopState()) {
      updateRemainingCapacity(removedReplica.getCapacity(), _remainingTopStateCapacity, true);
    }
  }

  /**
   * @return A set of all assigned replicas on the node.
   */
  Set<AssignableReplica> getAssignedReplicas() {
    return _currentAssignedReplicaMap.values().stream()
        .flatMap(replicaMap -> replicaMap.values().stream()).collect(Collectors.toSet());
  }

  /**
   * @return The current assignment in a map of <resource name, set of partition names>
   */
  Map<String, Set<String>> getAssignedPartitionsMap() {
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
  Set<String> getAssignedTopStatePartitionsByResource(String resource) {
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
  public float getGeneralProjectedHighestUtilization(Map<String, Integer> newUsage) {
    return getProjectedHighestUtilization(newUsage, _remainingCapacity);
  }

  /**
   * Return the most concerning capacity utilization number for evenly partition assignment.
   * The method dynamically calculates the projected highest utilization number among all the
   * capacity categories assuming the new capacity usage is added to the node.
   * For example, if the current node usage is {CPU: 0.9, MEM: 0.4, DISK: 0.6}. Then this call shall
   * return 0.9.
   * This function returns projected highest utilization for only top state partitions.
   * @param newUsage the proposed new additional capacity usage.
   * @return The highest utilization number of the node among all the capacity category.
   */
  public float getTopStateProjectedHighestUtilization(Map<String, Integer> newUsage) {
    return getProjectedHighestUtilization(newUsage, _remainingTopStateCapacity);
  }

  private float getProjectedHighestUtilization(Map<String, Integer> newUsage,
      Map<String, Integer> remainingCapacity) {
    float highestCapacityUtilization = 0;
    for (String capacityKey : _maxAllowedCapacity.keySet()) {
      float capacityValue = _maxAllowedCapacity.get(capacityKey);
      float utilization = (capacityValue - remainingCapacity.get(capacityKey) + newUsage
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
    LinkedHashMap<String, String> instanceTopologyMap = Topology
        .computeInstanceTopologyMap(clusterConfig, instanceConfig.getInstanceName(), instanceConfig,
            true /*earlyQuitTillFaultZone*/);

    StringBuilder faultZoneStringBuilder = new StringBuilder();
    for (Map.Entry<String, String> entry : instanceTopologyMap.entrySet()) {
      faultZoneStringBuilder.append(entry.getValue());
      faultZoneStringBuilder.append('/');
    }
    faultZoneStringBuilder.setLength(faultZoneStringBuilder.length() - 1);
    return faultZoneStringBuilder.toString();
  }

  /**
   * @throws HelixException if the replica has already been assigned to the node.
   */
  private void addToAssignmentRecord(AssignableReplica replica) {
    String resourceName = replica.getResourceName();
    String partitionName = replica.getPartitionName();
    if (_currentAssignedReplicaMap.containsKey(resourceName) && _currentAssignedReplicaMap
        .get(resourceName).containsKey(partitionName)) {
      throw new HelixException(String
          .format("Resource %s already has a replica with state %s from partition %s on node %s",
              replica.getResourceName(), replica.getReplicaState(), replica.getPartitionName(),
              getInstanceName()));
    } else {
      _currentAssignedReplicaMap.computeIfAbsent(resourceName, key -> new HashMap<>())
          .put(partitionName, replica);
    }
  }

  private void updateRemainingCapacity(Map<String, Integer> usedCapacity, Map<String, Integer> remainingCapacity,
      boolean isRelease) {
    int multiplier = isRelease ? -1 : 1;
    // if the used capacity key does not exist in the node's capacity, ignore it
    usedCapacity.forEach((capacityKey, capacityValue) -> remainingCapacity.compute(capacityKey,
        (key, value) -> value == null ? null : value - multiplier * capacityValue));
  }

  /**
   * Get and validate the instance capacity from instance config.
   * @throws HelixException if any required capacity key is not configured in the instance config.
   */
  private Map<String, Integer> fetchInstanceCapacity(ClusterConfig clusterConfig,
      InstanceConfig instanceConfig) {
    Map<String, Integer> instanceCapacity =
        WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
    // Remove all the non-required capacity items from the map.
    instanceCapacity.keySet().retainAll(clusterConfig.getInstanceCapacityKeys());
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
