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
 * An interface for getting partition weight information.
 * This provider is supposed to return an estimation of max resource usage if a partition assigned to a participant.
 *
 * The return value will be used to calculate resource requirement of a proposed assignment to estimate if the assignment is possible or not.
 * In detail,
 * NewResourceUsage = {@link org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider#getParticipantUsage(String)} + SUM({@link org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider#getPartitionWeight(String, String)})
 * And for validate, the constraint checks if {@link org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider#getParticipantCapacity(String) TotalCapacity} >= NewResourceUsage.
 *
 * For example, a database resource partition will need certain amount of storage usage, as well as CPU, memory, and network IO.
 * If a provider is implemented for estimating storage usage, for instance,
 * {@link org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider#getPartitionWeight(String, String)} should return
 * disk size that need to be reserved by this partition. Returns 10 for 10 GB etc.
 * In addition, if another provider is implemented for estimating memory usage, {@link org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider#getPartitionWeight(String, String)}
 * should return max memory that could be used by this partition. Returns 100 for 100 MB memory usage etc.
 *
 * While this provider is used together with a {@link org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider CapacityProvider},
 * both providers are supposed to return values in the same unit so they can be used to estimate the resource usage of an proposed assignment.
 * For example, if the providers are designed for providing memory usage, and the {@link org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider CapacityProvider}
 * returns value in the unit of MB, then {@link org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider#getPartitionWeight(String, String)} should also return
 * the weight in the unit of MB.
 */
public interface PartitionWeightProvider {

  /**
   * @param resource
   * @param partition
   * @return The weight (of participant capacity) that is required by a certain partition in the specified resource
   */
  int getPartitionWeight(String resource, String partition);
}
