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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a partition replication that needs to be allocated.
 */
public class AssignableReplica implements Comparable<AssignableReplica> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignableReplica.class);

  private final String _partitionName;
  private final String _resourceName;
  private final String _resourceInstanceGroupTag;
  private final int _resourceMaxPartitionsPerInstance;
  private final Map<String, Integer> _capacityUsage;
  // The priority of the replica's state
  private final int _statePriority;
  // The state of the replica
  private final String _replicaState;

  /**
   * @param clusterConfig The cluster config.
   * @param resourceConfig The resource config for the resource which contains the replication.
   * @param partitionName The replication's partition name.
   * @param replicaState The state of the replication.
   * @param statePriority The priority of the replication's state.
   */
  AssignableReplica(ClusterConfig clusterConfig, ResourceConfig resourceConfig,
      String partitionName, String replicaState, int statePriority) {
    _partitionName = partitionName;
    _replicaState = replicaState;
    _statePriority = statePriority;
    _resourceName = resourceConfig.getResourceName();
    _capacityUsage = fetchCapacityUsage(partitionName, resourceConfig, clusterConfig);
    _resourceInstanceGroupTag = resourceConfig.getInstanceGroupTag();
    _resourceMaxPartitionsPerInstance = resourceConfig.getMaxPartitionsPerInstance();
  }

  private AssignableReplica(Builder builder) {
    _partitionName = builder._partitionName;
    _resourceName = builder._resourceName;
    _resourceInstanceGroupTag = builder._resourceInstanceGroupTag;
    _resourceMaxPartitionsPerInstance = builder._resourceMaxPartitionsPerInstance;
    _capacityUsage = builder._capacityUsage;
    _statePriority = builder._statePriority;
    _replicaState = builder._replicaState;
  }

  public Map<String, Integer> getCapacity() {
    return _capacityUsage;
  }

  public String getPartitionName() {
    return _partitionName;
  }

  public String getReplicaState() {
    return _replicaState;
  }

  public boolean isReplicaTopState() {
    return _statePriority == StateModelDefinition.TOP_STATE_PRIORITY;
  }

  public int getStatePriority() {
    return _statePriority;
  }

  public String getResourceName() {
    return _resourceName;
  }

  public String getResourceInstanceGroupTag() {
    return _resourceInstanceGroupTag;
  }

  public boolean hasResourceInstanceGroupTag() {
    return _resourceInstanceGroupTag != null && !_resourceInstanceGroupTag.isEmpty();
  }

  public int getResourceMaxPartitionsPerInstance() {
    return _resourceMaxPartitionsPerInstance;
  }

  @Override
  public String toString() {
    return generateReplicaKey(_resourceName, _partitionName, _replicaState);
  }

  @Override
  public int compareTo(AssignableReplica replica) {
    if (!_resourceName.equals(replica._resourceName)) {
      return _resourceName.compareTo(replica._resourceName);
    }
    if (!_partitionName.equals(replica._partitionName)) {
      return _partitionName.compareTo(replica._partitionName);
    }
    if (!_replicaState.equals(replica._replicaState)) {
      return _replicaState.compareTo(replica._replicaState);
    }
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof AssignableReplica) {
      return compareTo((AssignableReplica) obj) == 0;
    } else {
      return false;
    }
  }

  public static String generateReplicaKey(String resourceName, String partitionName, String state) {
    return String.format("%s:%s:%s", resourceName, partitionName, state);
  }

  /**
   * Parse the resource config for the partition weight.
   */
  private Map<String, Integer> fetchCapacityUsage(String partitionName,
      ResourceConfig resourceConfig, ClusterConfig clusterConfig) {
    Map<String, Map<String, Integer>> capacityMap;
    try {
      capacityMap = resourceConfig.getPartitionCapacityMap();
    } catch (IOException ex) {
      throw new IllegalArgumentException("Invalid partition capacity configuration of resource: "
          + resourceConfig.getResourceName(), ex);
    }

    Map<String, Integer> partitionCapacity = capacityMap.get(partitionName);
    if (partitionCapacity == null) {
      partitionCapacity = capacityMap.get(ResourceConfig.DEFAULT_PARTITION_KEY);
    }
    if (partitionCapacity == null) {
      LOG.warn("The capacity usage of the specified partition {} is not configured in the Resource"
          + " Config {}. No default partition capacity is configured either. Will proceed with"
          + " empty capacity configuration.", partitionName, resourceConfig.getResourceName());
      partitionCapacity = new HashMap<>();
    }

    List<String> requiredCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    // Remove the non-required capacity items.
    partitionCapacity.keySet().retainAll(requiredCapacityKeys);
    // If any required capacity key is not configured in the resource config, fill the partition
    // capacity map with 0 usage.
    for (String capacityKey : requiredCapacityKeys) {
      partitionCapacity.putIfAbsent(capacityKey, 0);
    }

    return partitionCapacity;
  }

  //TODO: migrate existing constructor to use constructor only
  static final class Builder {
    private String _partitionName;
    private String _resourceName;
    private String _resourceInstanceGroupTag;
    private int _resourceMaxPartitionsPerInstance;
    private Map<String, Integer> _capacityUsage;
    private int _statePriority;
    private String _replicaState;

    Builder(String partitionName, String resourceName) {
      _resourceName = resourceName;
      _partitionName = partitionName;
    }

    Builder resourceInstanceGroupTag(String resourceInstanceGroupTag) {
      this._resourceInstanceGroupTag = resourceInstanceGroupTag;
      return this;
    }

    Builder resourceMaxPartitionsPerInstance(int resourceMaxPartitionsPerInstance) {
      this._resourceMaxPartitionsPerInstance = resourceMaxPartitionsPerInstance;
      return this;
    }

    Builder capacityUsage(Map<String, Integer> capacityUsage) {
      this._capacityUsage = capacityUsage;
      return this;
    }

    Builder statePriority(int statePriority) {
      this._statePriority = statePriority;
      return this;
    }

    Builder replicaState(String replicaState) {
      this._replicaState = replicaState;
      return this;
    }

     AssignableReplica build() {
      return new AssignableReplica(this);
    }
  }
}
