package org.apache.helix.controller.rebalancer.waged;

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

import org.apache.helix.controller.rebalancer.waged.constraints.HardConstraint;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.model.ResourceAssignment;

import java.util.Map;

/**
 * A generic rebalance algorithm interface for the WAGED rebalancer.
 *
 * @see <a href="Rebalance Algorithm">https://github.com/apache/helix/wiki/Design-Proposal---Weight-Aware-Globally-Even-Distribute-Rebalancer#rebalance-algorithm-adapter</a>
 */
public interface RebalanceAlgorithm {

  /**
   * Rebalance the Helix resource partitions based on the input cluster model.
   *
   * @param clusterModel
   * @param failureReasons Return the failures <ResourceName, <FailureReason, Count>> that happen during the rebalance calculation.
   *                       If the map is null, no failure will be returned.
   * @return A map of <ResourceName, ResourceAssignment>.
   */
  Map<String, ResourceAssignment> rebalance(ClusterModel clusterModel,
      Map<String, Map<HardConstraint.FailureReason, Integer>> failureReasons);
}
