package org.apache.helix.api.config;

import java.util.Map;
import java.util.Set;

import org.apache.helix.api.Partition;
import org.apache.helix.api.Scope;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;

import com.google.common.collect.Sets;

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
  /**
   * Type of a resource. A resource is any entity that can be managed by Helix.
   */
  public enum ResourceType {
    /**
     * A resource that is persistent, and potentially partitioned and replicated.
     */
    DATA
  }

  private final ResourceId _id;
  private final RebalancerConfig _rebalancerConfig;
  private final SchedulerTaskConfig _schedulerTaskConfig;
  private final ProvisionerConfig _provisionerConfig;
  private final UserConfig _userConfig;
  private final int _bucketSize;
  private final boolean _batchMessageMode;
  private final ResourceType _resourceType;

  /**
   * Instantiate a configuration. Consider using ResourceConfig.Builder
   * @param id resource id
   * @param partitionMap map of partition identifiers to partition objects
   * @param schedulerTaskConfig configuration for scheduler tasks associated with the resource
   * @param rebalancerConfig configuration for rebalancing the resource
   * @param provisionerConfig configuration for provisioning for the resource
   * @param userConfig user-defined resource properties
   * @param bucketSize bucket size for this resource
   * @param batchMessageMode whether or not batch messaging is allowed
   */
  public ResourceConfig(ResourceId id, ResourceType resourceType,
      SchedulerTaskConfig schedulerTaskConfig, RebalancerConfig rebalancerConfig,
      ProvisionerConfig provisionerConfig, UserConfig userConfig, int bucketSize,
      boolean batchMessageMode) {
    _id = id;
    _resourceType = resourceType;
    _schedulerTaskConfig = schedulerTaskConfig;
    _rebalancerConfig = rebalancerConfig;
    _provisionerConfig = provisionerConfig;
    _userConfig = userConfig;
    _bucketSize = bucketSize;
    _batchMessageMode = batchMessageMode;
  }

  /**
   * Get the subunits of the resource
   * @return map of subunit id to subunit or empty map if none
   */
  public Map<? extends PartitionId, ? extends Partition> getSubUnitMap() {
    return _rebalancerConfig.getSubUnitMap();
  }

  /**
   * Get a subunit that the resource contains
   * @param subUnitId the subunit id to look up
   * @return Partition or null if none is present with the given id
   */
  public Partition getSubUnit(PartitionId subUnitId) {
    return getSubUnitMap().get(subUnitId);
  }

  /**
   * Get the set of subunit ids that the resource contains
   * @return subunit id set, or empty if none
   */
  public Set<? extends PartitionId> getSubUnitSet() {
    return getSubUnitMap().keySet();
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
   * Get the resource type
   * @return ResourceType
   */
  public ResourceType getType() {
    return _resourceType;
  }

  /**
   * Get the properties configuring scheduler tasks
   * @return SchedulerTaskConfig properties
   */
  public SchedulerTaskConfig getSchedulerTaskConfig() {
    return _schedulerTaskConfig;
  }

  /**
   * Get the properties configuring the provisioner
   * @return ProvisionerConfig properties
   */
  public ProvisionerConfig getProvisionerConfig() {
    return _provisionerConfig;
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
    return getSubUnitMap().toString();
  }

  /**
   * Update context for a ResourceConfig
   */
  public static class Delta {
    private enum Fields {
      TYPE,
      REBALANCER_CONFIG,
      PROVISIONER_CONFIG,
      USER_CONFIG,
      BUCKET_SIZE,
      BATCH_MESSAGE_MODE
    }

    private Set<Fields> _updateFields;
    private Builder _builder;

    /**
     * Instantiate the delta for a resource config
     * @param resourceId the resource to update
     */
    public Delta(ResourceId resourceId) {
      _builder = new Builder(resourceId);
      _updateFields = Sets.newHashSet();
    }

    /**
     * Set the type of this resource
     * @param type ResourceType
     * @return Delta
     */
    public Delta setType(ResourceType type) {
      _builder.type(type);
      _updateFields.add(Fields.TYPE);
      return this;
    }

    /**
     * Set the rebalancer configuration
     * @param config properties of interest for rebalancing
     * @return Delta
     */
    public Delta setRebalancerConfig(RebalancerConfig config) {
      _builder.rebalancerConfig(config);
      _updateFields.add(Fields.REBALANCER_CONFIG);
      return this;
    }

    /**
     * Set the provisioner configuration
     * @param config properties of interest for provisioning
     * @return Delta
     */
    public Delta setProvisionerConfig(ProvisionerConfig config) {
      _builder.provisionerConfig(config);
      _updateFields.add(Fields.PROVISIONER_CONFIG);
      return this;
    }

    /**
     * Set the user configuration
     * @param userConfig user-specified properties
     * @return Delta
     */
    public Delta setUserConfig(UserConfig userConfig) {
      _builder.userConfig(userConfig);
      _updateFields.add(Fields.USER_CONFIG);
      return this;
    }

    /**
     * Set the bucket size
     * @param bucketSize the size to use
     * @return Delta
     */
    public Delta setBucketSize(int bucketSize) {
      _builder.bucketSize(bucketSize);
      _updateFields.add(Fields.BUCKET_SIZE);
      return this;
    }

    /**
     * Set the batch message mode
     * @param batchMessageMode true to enable, false to disable
     * @return Delta
     */
    public Delta setBatchMessageMode(boolean batchMessageMode) {
      _builder.batchMessageMode(batchMessageMode);
      _updateFields.add(Fields.BATCH_MESSAGE_MODE);
      return this;
    }

    /**
     * Create a ResourceConfig that is the combination of an existing ResourceConfig and this delta
     * @param orig the original ResourceConfig
     * @return updated ResourceConfig
     */
    public ResourceConfig mergeInto(ResourceConfig orig) {
      ResourceConfig deltaConfig = _builder.build();
      Builder builder =
          new Builder(orig.getId()).type(orig.getType())
              .rebalancerConfig(orig.getRebalancerConfig())
              .provisionerConfig(orig.getProvisionerConfig())
              .schedulerTaskConfig(orig.getSchedulerTaskConfig()).userConfig(orig.getUserConfig())
              .bucketSize(orig.getBucketSize()).batchMessageMode(orig.getBatchMessageMode());
      for (Fields field : _updateFields) {
        switch (field) {
        case TYPE:
          builder.type(deltaConfig.getType());
          break;
        case REBALANCER_CONFIG:
          builder.rebalancerConfig(deltaConfig.getRebalancerConfig());
          break;
        case PROVISIONER_CONFIG:
          builder.provisionerConfig(deltaConfig.getProvisionerConfig());
          break;
        case USER_CONFIG:
          builder.userConfig(deltaConfig.getUserConfig());
          break;
        case BUCKET_SIZE:
          builder.bucketSize(deltaConfig.getBucketSize());
          break;
        case BATCH_MESSAGE_MODE:
          builder.batchMessageMode(deltaConfig.getBatchMessageMode());
          break;
        }
      }
      return builder.build();
    }
  }

  /**
   * Assembles a ResourceConfig
   */
  public static class Builder {
    private final ResourceId _id;
    private ResourceType _type;
    private RebalancerConfig _rebalancerConfig;
    private SchedulerTaskConfig _schedulerTaskConfig;
    private ProvisionerConfig _provisionerConfig;
    private UserConfig _userConfig;
    private int _bucketSize;
    private boolean _batchMessageMode;

    /**
     * Build a Resource with an id
     * @param id resource id
     */
    public Builder(ResourceId id) {
      _id = id;
      _type = ResourceType.DATA;
      _bucketSize = 0;
      _batchMessageMode = false;
      _userConfig = new UserConfig(Scope.resource(id));
    }

    /**
     * Set the type of this resource
     * @param type ResourceType
     * @return Builder
     */
    public Builder type(ResourceType type) {
      _type = type;
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
     * @param schedulerTaskConfig
     * @return
     */
    public Builder provisionerConfig(ProvisionerConfig provisionerConfig) {
      _provisionerConfig = provisionerConfig;
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
      return new ResourceConfig(_id, _type, _schedulerTaskConfig, _rebalancerConfig,
          _provisionerConfig, _userConfig, _bucketSize, _batchMessageMode);
    }
  }
}
