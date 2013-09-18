package org.apache.helix.api;

import java.util.Map;

import org.apache.helix.model.IdealState.RebalanceMode;

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
 * Configuration properties for the FULL_AUTO rebalancer
 */
public final class FullAutoRebalancerConfig extends RebalancerConfig {
  /**
   * Instantiate a new config for FULL_AUTO
   * @param resourceId the resource to rebalance
   * @param stateModelDefId the state model that the resource follows
   * @param partitionMap map of partition id to partition
   */
  public FullAutoRebalancerConfig(ResourceId resourceId, StateModelDefId stateModelDefId,
      Map<PartitionId, Partition> partitionMap) {
    super(resourceId, RebalanceMode.FULL_AUTO, stateModelDefId, partitionMap);
  }

  /**
   * Instantiate from a base RebalancerConfig
   * @param config populated rebalancer config
   */
  private FullAutoRebalancerConfig(RebalancerConfig config) {
    super(config);
  }

  /**
   * Get a FullAutoRebalancerConfig from a RebalancerConfig
   * @param config populated RebalancerConfig
   * @return FullAutoRebalancerConfig
   */
  public static FullAutoRebalancerConfig from(RebalancerConfig config) {
    return new FullAutoRebalancerConfig(config);
  }

  /**
   * Assembler for a FULL_AUTO configuration
   */
  public static class Builder extends RebalancerConfig.Builder<Builder> {
    /**
     * Build for a specific resource
     * @param resourceId the resource to rebalance
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
    }

    /**
     * Construct a builder using an existing full-auto rebalancer config
     * @param config
     */
    public Builder(FullAutoRebalancerConfig config) {
      super(config);
    }

    @Override
    public FullAutoRebalancerConfig build() {
      if (_partitionMap.isEmpty()) {
        addPartitions(1);
      }
      FullAutoRebalancerConfig config =
          new FullAutoRebalancerConfig(_resourceId, _stateModelDefId, _partitionMap);
      update(config);
      return config;
    }

    @Override
    protected Builder self() {
      return this;
    }
  }
}
