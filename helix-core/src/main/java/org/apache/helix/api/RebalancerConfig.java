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

  public RebalancerConfig() {
    _rebalancerMode = RebalanceMode.NONE;
    _rebalancerRef = null;
    _stateModelDefId = null;

    _resourceAssignment = null;
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
}
