package org.apache.helix.api;

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

import org.apache.helix.model.IdealState.RebalanceMode;

public class RebalancerConfig {
  private final RebalanceMode _rebalancerMode;
  private final RebalancerRef _rebalancerRef;
  private final StateModelDefId _stateModelDefId;

  private final RscAssignment _resourceAssignment;

  public RebalancerConfig(RebalanceMode mode, RebalancerRef rebalancerRef,
      StateModelDefId stateModelDefId, RscAssignment resourceAssignment) {
    _rebalancerMode = mode;
    _rebalancerRef = rebalancerRef;
    _stateModelDefId = stateModelDefId;
    _resourceAssignment = resourceAssignment;
  }

  /**
   * Get the rebalancer mode
   * @return rebalancer mode
   */
  public RebalanceMode getRebalancerMode() {
    return _rebalancerMode;
  }

  /**
   * Get the rebalancer class name
   * @return rebalancer class name or null if not exist
   */
  public RebalancerRef getRebalancerRef() {
    return _rebalancerRef;
  }

  /**
   * Get state model definition name of the resource
   * @return state model definition
   */
  public StateModelDefId getStateModelDefId() {
    return _stateModelDefId;
  }

  /**
   * Get the ideal node and state assignment of the resource
   * @return resource assignment
   */
  public RscAssignment getResourceAssignment() {
    return _resourceAssignment;
  }

  /**
   * Assembles a RebalancerConfig
   */
  public static class Builder {
    private RebalanceMode _mode = RebalanceMode.NONE;
    private RebalancerRef _rebalancerRef;
    private StateModelDefId _stateModelDefId;
    private RscAssignment _resourceAssignment;

    /**
     * Set the rebalancer mode
     * @param mode {@link RebalanceMode}
     */
    public Builder rebalancerMode(RebalanceMode mode) {
      _mode = mode;
      return this;
    }

    /**
     * Set a user-defined rebalancer
     * @param rebalancerRef a reference to the rebalancer
     * @return Builder
     */
    public Builder rebalancer(RebalancerRef rebalancerRef) {
      _rebalancerRef = rebalancerRef;
      return this;
    }

    /**
     * Set the state model definition
     * @param stateModelDefId state model identifier
     * @return Builder
     */
    public Builder stateModelDef(StateModelDefId stateModelDefId) {
      _stateModelDefId = stateModelDefId;
      return this;
    }

    /**
     * Set the full assignment of partitions to nodes and corresponding states
     * @param resourceAssignment resource assignment
     * @return Builder
     */
    public Builder resourceAssignment(RscAssignment resourceAssignment) {
      _resourceAssignment = resourceAssignment;
      return this;
    }

    /**
     * Assemble a RebalancerConfig
     * @return a fully defined rebalancer configuration
     */
    public RebalancerConfig build() {
      return new RebalancerConfig(_mode, _rebalancerRef, _stateModelDefId, _resourceAssignment);
    }
  }
}
