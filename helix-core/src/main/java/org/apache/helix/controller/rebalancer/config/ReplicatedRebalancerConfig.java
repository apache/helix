package org.apache.helix.controller.rebalancer.config;

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
 * Methods specifying a rebalancer config that allows replicas. For instance, a rebalancer config
 * with partitions may accept state model definitions that support multiple replicas per partition,
 * and it's possible that the policy is that each live participant in the system should have a
 * replica.
 */
public interface ReplicatedRebalancerConfig extends RebalancerConfig {
  /**
   * Check if this resource should be assigned to any live participant
   * @return true if any live participant expected, false otherwise
   */
  public boolean anyLiveParticipant();

  /**
   * Get the number of replicas that each resource subunit should have
   * @return replica count
   */
  public int getReplicaCount();
}
