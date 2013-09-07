package org.apache.helix.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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

/**
 * Full configuration of a Helix resource. Typically used to add or modify resources on a cluster
 */
public class ResourceConfig {
  private final ResourceId _id;
  private final Map<PartitionId, Partition> _partitionMap;
  private final RebalancerConfig _rebalancerConfig;
  private final SchedulerTaskConfig _schedulerTaskConfig;

  /**
   * Instantiate a configuration. Consider using ResourceConfig.Builder
   * @param id resource id
   * @param partitionMap map of partition identifiers to partition objects
   * @param schedulerTaskConfig configuration for scheduler tasks associated with the resource
   * @param rebalancerConfig configuration for rebalancing the resource
   */
  public ResourceConfig(ResourceId id, Map<PartitionId, Partition> partitionMap,
      SchedulerTaskConfig schedulerTaskConfig, RebalancerConfig rebalancerConfig) {
    _id = id;
    _partitionMap = ImmutableMap.copyOf(partitionMap);
    _schedulerTaskConfig = schedulerTaskConfig;
    _rebalancerConfig = rebalancerConfig;
  }

  /**
   * Get the partitions of the resource
   * @return map of partition id to partition or empty map if none
   */
  public Map<PartitionId, Partition> getPartitionMap() {
    return _partitionMap;
  }

  /**
   * Get a partition that the resource contains
   * @param partitionId the partition id to look up
   * @return Partition or null if none is present with the given id
   */
  public Partition getPartition(PartitionId partitionId) {
    return _partitionMap.get(partitionId);
  }

  /**
   * Get the set of partition ids that the resource contains
   * @return partition id set, or empty if none
   */
  public Set<PartitionId> getPartitionSet() {
    Set<PartitionId> partitionSet = new HashSet<PartitionId>();
    partitionSet.addAll(_partitionMap.keySet());
    return ImmutableSet.copyOf(partitionSet);
  }

  /**
   * Get the resource properties configuring rebalancing
   * @return RebalancerConfig properties
   */
  public RebalancerConfig getRebalancerConfig() {
    return _rebalancerConfig;
  }

  /**
   * Get the resource id
   * @return ResourceId
   */
  public ResourceId getId() {
    return _id;
  }

  /**
   * Get the properties configuring scheduler tasks
   * @return SchedulerTaskConfig properties
   */
  public SchedulerTaskConfig getSchedulerTaskConfig() {
    return _schedulerTaskConfig;
  }

  /**
   * Assembles a ResourceConfig
   */
  public static class Builder {
    private final ResourceId _id;
    private final Map<PartitionId, Partition> _partitionMap;
    private RebalancerConfig _rebalancerConfig;
    private SchedulerTaskConfig _schedulerTaskConfig;

    /**
     * Build a Resource with an id
     * @param id resource id
     */
    public Builder(ResourceId id) {
      _id = id;
      _partitionMap = new HashMap<PartitionId, Partition>();
    }

    /**
     * Add a partition that the resource serves
     * @param partition fully-qualified partition
     * @return Builder
     */
    public Builder addPartition(Partition partition) {
      _partitionMap.put(partition.getId(), partition);
      return this;
    }

    /**
     * Add a collection of partitions
     * @param partitions
     * @return Builder
     */
    public Builder addPartitions(Collection<Partition> partitions) {
      for (Partition partition : partitions) {
        addPartition(partition);
      }
      return this;
    }

    /**
     * Set the rebalancer configuration
     * @param rebalancerConfig properties of interest for rebalancing
     * @return Builder
     */
    public Builder rebalancerConfig(RebalancerConfig rebalancerConfig) {
      _rebalancerConfig = rebalancerConfig;
      return this;
    }

    /**
     * @param schedulerTaskConfig
     * @return
     */
    public Builder schedulerTaskConfig(SchedulerTaskConfig schedulerTaskConfig) {
      _schedulerTaskConfig = schedulerTaskConfig;
      return this;
    }

    /**
     * Create a Resource object
     * @return instantiated Resource
     */
    public ResourceConfig build() {
      return new ResourceConfig(_id, _partitionMap, _schedulerTaskConfig, _rebalancerConfig);
    }
  }
}
