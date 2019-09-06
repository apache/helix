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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;

/**
 * A generic interface to generate the optimal assignment given the runtime cluster environment.
 *
 * <pre>
 * @see <a href="https://github.com/apache/helix/wiki/
 * Design-Proposal---Weight-Aware-Globally-Even-Distribute-Rebalancer
 * #rebalance-algorithm-adapter">Rebalance Algorithm</a>
 * </pre>
 */
public interface RebalanceAlgorithm {

  /**
   * Rebalance the Helix resource partitions based on the input cluster model.
   * @param clusterModel The run time cluster model that contains all necessary information
   * @return An instance of {@link OptimalAssignment}
   */
  OptimalAssignment calculate(ClusterModel clusterModel) throws HelixRebalanceException;
}
