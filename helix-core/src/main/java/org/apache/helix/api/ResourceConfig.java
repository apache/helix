package org.apache.helix.api;

import java.util.Map;
import java.util.Set;

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
  private final RebalancerConfig _rebalancerConfig;
  private final SchedulerTaskConfig _schedulerTaskConfig;
  private final UserConfig _userConfig;
  private final int _bucketSize;
  private final boolean _batchMessageMode;

  /**
   * Instantiate a configuration. Consider using ResourceConfig.Builder
   * @param id resource id
   * @param partitionMap map of partition identifiers to partition objects
   * @param schedulerTaskConfig configuration for scheduler tasks associated with the resource
   * @param rebalancerConfig configuration for rebalancing the resource
   * @param userConfig user-defined resource properties
   * @param bucketSize bucket size for this resource
   * @param batchMessageMode whether or not batch messaging is allowed
   */
  public ResourceConfig(ResourceId id, SchedulerTaskConfig schedulerTaskConfig,
      RebalancerConfig rebalancerConfig, UserConfig userConfig, int bucketSize,
      boolean batchMessageMode) {
    _id = id;
    _schedulerTaskConfig = schedulerTaskConfig;
    _rebalancerConfig = rebalancerConfig;
    _userConfig = userConfig;
    _bucketSize = bucketSize;
    _batchMessageMode = batchMessageMode;
  }

  /**
   * Get the partitions of the resource
   * @return map of partition id to partition or empty map if none
   */
  public Map<PartitionId, Partition> getPartitionMap() {
    return _rebalancerConfig.getPartitionMap();
  }

  /**
   * Get a partition that the resource contains
   * @param partitionId the partition id to look up
   * @return Partition or null if none is present with the given id
   */
  public Partition getPartition(PartitionId partitionId) {
    return _rebalancerConfig.getPartition(partitionId);
  }

  /**
   * Get the set of partition ids that the resource contains
   * @return partition id set, or empty if none
   */
  public Set<PartitionId> getPartitionSet() {
    return _rebalancerConfig.getPartitionSet();
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
   * Get user-specified configuration properties of this resource
   * @return UserConfig properties
   */
  public UserConfig getUserConfig() {
    return _userConfig;
  }

  /**
   * Get the bucket size for this resource
   * @return bucket size
   */
  public int getBucketSize() {
    return _bucketSize;
  }

  /**
   * Get the batch message mode
   * @return true if enabled, false if disabled
   */
  public boolean getBatchMessageMode() {
    return _batchMessageMode;
  }

  @Override
  public String toString() {
    return _rebalancerConfig.getPartitionMap().toString();
  }

  /**
   * Assembles a ResourceConfig
   */
  public static class Builder {
    private final ResourceId _id;
    private RebalancerConfig _rebalancerConfig;
    private SchedulerTaskConfig _schedulerTaskConfig;
    private UserConfig _userConfig;
    private int _bucketSize;
    private boolean _batchMessageMode;

    /**
     * Build a Resource with an id
     * @param id resource id
     */
    public Builder(ResourceId id) {
      _id = id;
      _bucketSize = 0;
      _batchMessageMode = false;
      _userConfig = new UserConfig(Scope.resource(id));
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
     * Set the user configuration
     * @param userConfig user-specified properties
     * @return Builder
     */
    public Builder userConfig(UserConfig userConfig) {
      _userConfig = userConfig;
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
     * Set the bucket size
     * @param bucketSize the size to use
     * @return Builder
     */
    public Builder bucketSize(int bucketSize) {
      _bucketSize = bucketSize;
      return this;
    }

    /**
     * Set the batch message mode
     * @param batchMessageMode true to enable, false to disable
     * @return Builder
     */
    public Builder batchMessageMode(boolean batchMessageMode) {
      _batchMessageMode = batchMessageMode;
      return this;
    }

    /**
     * Create a Resource object
     * @return instantiated Resource
     */
    public ResourceConfig build() {
      return new ResourceConfig(_id, _schedulerTaskConfig, _rebalancerConfig, _userConfig,
          _bucketSize, _batchMessageMode);
    }
  }
}
