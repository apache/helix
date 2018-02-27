package org.apache.helix.api.rebalancer.constraint.dataprovider;

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
 * An interface for getting participant capacity information.
 * The return value will be used to estimate available capacity, as well as utilization in percentage for prioritizing participants.
 *
 * Note that all return values of the provider are supposed to be in the same unit.
 * For example, if the provider is for memory capacity of a host (total memory size is 8G, current usage is 512MB),
 * the return values could be {@link org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider#getParticipantCapacity(String)} = 8192, and {@link org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider#getParticipantUsage(String)} = 512.
 * Another example, if the provider is for partition count capacity (max count 1000 partitions, currently no partition assigned),
 * the return values should be {@link org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider#getParticipantCapacity(String)} = 1000, and {@link org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider#getParticipantUsage(String)} = 0.
 *
 * Moreover, while this provider is used together with a {@link org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider PartitionWeightProvider},
 * both providers are supposed to return values in the same unit so they can be used to estimate the resource usage of an proposed assignment.
 */
public interface CapacityProvider {

  /**
   * @param participant
   * @return The total participant capacity.
   */
  int getParticipantCapacity(String participant);

  /**
   * @param participant
   * @return The participant usage.
   */
  int getParticipantUsage(String participant);
}
