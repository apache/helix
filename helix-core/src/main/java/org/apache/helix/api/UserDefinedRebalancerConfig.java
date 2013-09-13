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
 * Configuration properties for the USER_DEFINED rebalancer. If additional fields are necessary, all
 * getters and setters for simple, list, and map fields are available.
 */
public final class UserDefinedRebalancerConfig extends RebalancerConfig {
  public enum Fields {
    REBALANCER_CLASS_NAME
  }

  /**
   * Instantiate a new config for USER_DEFINED
   * @param resourceId the resource to rebalance
   * @param stateModelDefId the state model that the resource follows
   * @param partitionMap map of partition id to partition
   * @param rebalancerRef instantiated rebalancer reference
   */
  public UserDefinedRebalancerConfig(ResourceId resourceId, StateModelDefId stateModelDefId,
      Map<PartitionId, Partition> partitionMap, RebalancerRef rebalancerRef) {
    super(resourceId, RebalanceMode.USER_DEFINED, stateModelDefId, partitionMap);
    setSimpleField(Fields.REBALANCER_CLASS_NAME.toString(), rebalancerRef.toString());
  }

  /**
   * Instantiate from a base RebalancerConfig
   * @param config populated rebalancer config
   */
  private UserDefinedRebalancerConfig(RebalancerConfig config) {
    super(config);
  }

  /**
   * Get a reference to the class used to rebalance this resource
   * @return RebalancerRef, or null if none set
   */
  public RebalancerRef getRebalancerRef() {
    String rebalancerClassName = getStringField(Fields.REBALANCER_CLASS_NAME.toString(), null);
    if (rebalancerClassName != null) {
      return RebalancerRef.from(rebalancerClassName);
    }
    return null;
  }

  /**
   * Get a UserDefinedRebalancerConfig from a RebalancerConfig
   * @param config populated RebalancerConfig
   * @return UserDefinedRebalancerConfig
   */
  public static UserDefinedRebalancerConfig from(RebalancerConfig config) {
    return new UserDefinedRebalancerConfig(config);
  }

  /**
   * Assembler for a USER_DEFINED configuration
   */
  public static class Builder extends RebalancerConfig.Builder<Builder> {
    private RebalancerRef _rebalancerRef;

    /**
     * Build for a specific resource
     * @param resourceId the resource to rebalance
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
    }

    public Builder rebalancerRef(RebalancerRef rebalancerRef) {
      _rebalancerRef = rebalancerRef;
      return this;
    }

    @Override
    public UserDefinedRebalancerConfig build() {
      if (_partitionMap.isEmpty()) {
        addPartitions(1);
      }
      UserDefinedRebalancerConfig config =
          new UserDefinedRebalancerConfig(_resourceId, _stateModelDefId, _partitionMap,
              _rebalancerRef);
      update(config);
      return config;
    }

    @Override
    protected Builder self() {
      return this;
    }
  }
}
