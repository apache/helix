package org.apache.helix.controller.common;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.InstanceConfig;

/**
 * A Node is an entity that can serve capacity recording purpose. It has a capacity and knowledge
 * of partitions assigned to it, so it can decide if it can receive additional partitions.
 */
public class CapacityNode implements Comparable<CapacityNode> {
  private int _currentlyAssigned;
  private int _capacity;
  private final String _instanceName;
  private final String _logicaId;
  private final String _faultZone;
  private final Map<String, Set<String>> _partitionMap;

  /**
   * Constructor used for non-topology-aware use case
   * @param instanceName  The instance name of this node
   * @param capacity  The capacity of this node
   */
  public CapacityNode(String instanceName, int capacity) {
    this(instanceName, null, null, null);
    this._capacity = capacity;
  }

  /**
   * Constructor used for topology-aware use case
   * @param instanceName  The instance name of this node
   * @param clusterConfig  The cluster config for current helix cluster
   * @param clusterTopologyConfig  The cluster topology config for current helix cluster
   * @param instanceConfig  The instance config for current instance
   */
  public CapacityNode(String instanceName, ClusterConfig clusterConfig,
      ClusterTopologyConfig clusterTopologyConfig, InstanceConfig instanceConfig) {
    this._instanceName = instanceName;
    this._logicaId = clusterTopologyConfig != null ? instanceConfig.getLogicalId(
        clusterTopologyConfig.getEndNodeType()) : instanceName;
    this._faultZone =
        clusterConfig != null ? computeFaultZone(clusterConfig, instanceConfig) : null;
    this._partitionMap = new HashMap<>();
    this._capacity =
        clusterConfig != null ? clusterConfig.getGlobalMaxPartitionAllowedPerInstance() : 0;
    this._currentlyAssigned = 0;
  }

  /**
   * Check if this replica can be legally added to this node
   *
   * @param resource  The resource to assign
   * @param partition The partition to assign
   * @return true if the assignment can be made, false otherwise
   */
  public boolean canAdd(String resource, String partition) {
    if (_currentlyAssigned >= _capacity || (_partitionMap.containsKey(resource)
        && _partitionMap.get(resource).contains(partition))) {
      return false;
    }

    // Add the partition to the resource's set of partitions in this node
    _partitionMap.computeIfAbsent(resource, k -> new HashSet<>()).add(partition);
    _currentlyAssigned++;
    return true;
  }

  /**
   * Checks if a specific resource + partition is assigned to this node.
   *
   * @param resource  the name of the resource
   * @param partition the partition
   * @return {@code true} if the resource + partition is assigned to this node, {@code false} otherwise
   */
  public boolean hasPartition(String resource, String partition) {
    Set<String> partitions = _partitionMap.get(resource);
    return partitions != null && partitions.contains(partition);
  }

  /**
   * Set the capacity of this node
   * @param capacity  The capacity to set
   */
  public void setCapacity(int capacity) {
    _capacity = capacity;
  }

  /**
   * Get the instance name of this node
   * @return The instance name of this node
   */
  public String getInstanceName() {
    return _instanceName;
  }

  /**
   * Get the logical id of this node
   * @return The logical id of this node
   */
  public String getLogicalId() {
    return _logicaId;
  }

  /**
   * Get the fault zone of this node
   * @return The fault zone of this node
   */
  public String getFaultZone() {
    return _faultZone;
  }

  /**
   * Get number of partitions currently assigned to this node
   * @return The number of partitions currently assigned to this node
   */
  public int getCurrentlyAssigned() {
    return _currentlyAssigned;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("##########\nname=").append(_instanceName).append("\nassigned:")
        .append(_currentlyAssigned).append("\ncapacity:").append(_capacity).append("\nlogicalId:")
        .append(_logicaId).append("\nfaultZone:").append(_faultZone);
    return sb.toString();
  }

  @Override
  public int compareTo(CapacityNode o) {
    if (_logicaId != null) {
      return _logicaId.compareTo(o.getLogicalId());
    }
    return _instanceName.compareTo(o.getInstanceName());
  }

  /**
   * Computes the fault zone id based on the domain and fault zone type when topology is enabled.
   * For example, when
   * the domain is "zone=2, instance=testInstance" and the fault zone type is "zone", this function
   * returns "2".
   * If cannot find the fault zone type, this function leaves the fault zone id as the instance name.
   * TODO: change the return value to logical id when no fault zone type found. Also do the same for
   *  waged rebalancer in helix-core/src/main/java/org/apache/helix/controller/rebalancer/waged/model/AssignableNode.java
   */
  private String computeFaultZone(ClusterConfig clusterConfig, InstanceConfig instanceConfig) {
    LinkedHashMap<String, String> instanceTopologyMap =
        Topology.computeInstanceTopologyMap(clusterConfig, instanceConfig.getInstanceName(),
            instanceConfig, true /*earlyQuitTillFaultZone*/);

    StringBuilder faultZoneStringBuilder = new StringBuilder();
    for (Map.Entry<String, String> entry : instanceTopologyMap.entrySet()) {
      faultZoneStringBuilder.append(entry.getValue());
      faultZoneStringBuilder.append('/');
    }
    faultZoneStringBuilder.setLength(faultZoneStringBuilder.length() - 1);
    return faultZoneStringBuilder.toString();
  }
}
