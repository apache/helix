package org.apache.helix.api.config;

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

import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfigHolder;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ProvisionerConfigHolder;
import org.apache.helix.model.ResourceConfiguration;

/**
 * Full configuration of a Helix resource. Typically used to add or modify resources on a cluster
 */
public class ResourceConfig {

  private final ResourceId _id;
  private final RebalancerConfig _rebalancerConfig;
  private final IdealState _idealState;
  private final SchedulerTaskConfig _schedulerTaskConfig;
  private final ProvisionerConfig _provisionerConfig;
  private final UserConfig _userConfig;

  /**
   * Instantiate a configuration. Consider using ResourceConfig.Builder
   * @param id resource id
   * @param idealState the ideal state of the resource
   * @param schedulerTaskConfig configuration for scheduler tasks associated with the resource
   * @param rebalancerConfig configuration for rebalancing the resource
   * @param provisionerConfig configuration for provisioning for the resource
   * @param userConfig user-defined resource properties
   */
  private ResourceConfig(ResourceId id, IdealState idealState,
      SchedulerTaskConfig schedulerTaskConfig, RebalancerConfig rebalancerConfig,
      ProvisionerConfig provisionerConfig, UserConfig userConfig) {
    _id = id;
    _schedulerTaskConfig = schedulerTaskConfig;
    _idealState = idealState;
    _rebalancerConfig = rebalancerConfig;
    _provisionerConfig = provisionerConfig;
    _userConfig = userConfig;
  }

  /**
   * Get the set of subunit ids that the resource contains
   * @return subunit id set, or empty if none
   */
  public Set<? extends PartitionId> getSubUnitSet() {
    return _idealState.getPartitionIdSet();
  }

  /**
   * Get the resource properties configuring rebalancing
   * @return RebalancerConfig properties
   */
  public RebalancerConfig getRebalancerConfig() {
    return _rebalancerConfig;
  }

  /**
   * Get the ideal state for this resource
   * @return IdealState instance
   */
  public IdealState getIdealState() {
    return _idealState;
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

  @Override
  public String toString() {
    return _idealState.toString();
  }

  /**
   * Update context for a ResourceConfig
   */
  public static class Delta {
    private Builder _builder;

    /**
     * Instantiate the delta for a resource config
     * @param resourceId the resource to update
     */
    public Delta(ResourceId resourceId) {
      _builder = new Builder(resourceId);
    }

    /**
     * Set the ideal state
     * @param idealState updated ideal state
     * @return Delta
     */
    public Delta setIdealState(IdealState idealState) {
      _builder.idealState(idealState);
      return this;
    }

    /**
     * Set the rebalancer configuration
     * @param config properties of interest for rebalancing
     * @return Delta
     */
    public Delta setRebalancerConfig(RebalancerConfig config) {
      _builder.rebalancerConfig(config);
      return this;
    }

    /**
     * Set the provisioner configuration
     * @param config properties of interest for provisioning
     * @return Delta
     */
    public Delta setProvisionerConfig(ProvisionerConfig config) {
      _builder.provisionerConfig(config);
      return this;
    }

    /**
     * Add the user configuration
     * @param userConfig user-specified properties
     * @return Delta
     */
    public Delta addUserConfig(UserConfig userConfig) {
      _builder.userConfig(userConfig);
      return this;
    }

    /**
     * Given a physical accessor, merge in the updated logical properties
     * @param accessor the physical accessor
     */
    public void merge(HelixDataAccessor accessor) {
      // Construct the logical delta
      ResourceConfig deltaConfig = _builder.build();
      ResourceId resourceId = deltaConfig.getId();

      // Update the ideal state if set
      IdealState updatedIdealState = deltaConfig.getIdealState();
      if (updatedIdealState != null) {
        accessor.updateProperty(accessor.keyBuilder().idealStates(resourceId.toString()),
            updatedIdealState);
      }

      // Update the resource config if any of its inner configs is set
      boolean addedAnything = false;
      ResourceConfiguration updatedResourceConfig = new ResourceConfiguration(resourceId);
      ProvisionerConfig provisionerConfig = deltaConfig.getProvisionerConfig();
      if (provisionerConfig != null) {
        updatedResourceConfig.addNamespacedConfig(new ProvisionerConfigHolder(provisionerConfig)
            .toNamespacedConfig());
        addedAnything = true;
      }
      RebalancerConfig rebalancerConfig = deltaConfig.getRebalancerConfig();
      if (rebalancerConfig != null) {
        updatedResourceConfig.addNamespacedConfig(new RebalancerConfigHolder(rebalancerConfig)
            .toNamespacedConfig());
        addedAnything = true;
      }
      UserConfig userConfig = deltaConfig.getUserConfig();
      if (userConfig != null) {
        updatedResourceConfig.addNamespacedConfig(userConfig);
        addedAnything = true;
      }
      if (addedAnything) {
        accessor.updateProperty(accessor.keyBuilder().resourceConfig(resourceId.toString()),
            updatedResourceConfig);
      }
    }
  }

  /**
   * Assembles a ResourceConfig
   */
  public static class Builder {
    private final ResourceId _id;
    private IdealState _idealState;
    private RebalancerConfig _rebalancerConfig;
    private SchedulerTaskConfig _schedulerTaskConfig;
    private ProvisionerConfig _provisionerConfig;
    private UserConfig _userConfig;

    /**
     * Build a Resource with an id
     * @param id resource id
     */
    public Builder(ResourceId id) {
      _id = id;
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
     * Set the ideal state
     * @param idealState a description of a resource
     * @return Builder
     */
    public Builder idealState(IdealState idealState) {
      _idealState = idealState;
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
     * Create a Resource object
     * @return instantiated Resource
     */
    public ResourceConfig build() {
      return new ResourceConfig(_id, _idealState, _schedulerTaskConfig, _rebalancerConfig,
          _provisionerConfig, _userConfig);
    }
  }
}
