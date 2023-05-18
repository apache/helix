package org.apache.helix.controller.dataproviders;

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

import java.util.Map;

/**
 * An interface to provide capacity data for instance.
 * It will consider the pending state transition and assigned partitions capacity.
 * It will be dynamic and will always provide the available "headroom".
 *
 * The actual implementation will be stateful.
 */
public interface InstanceCapacityDataProvider {

 /**
   * Get the instance remaining capacity. 
   * Capacity and weight both are represented as Key-Value.
   * Returns the capacity map of available head room for the instance.
   * @param instanceName - instance name to query
   * @return Map<String, Integer> - capacity pair for all defined attributes for the instance.
   */
  public Map<String, Integer> getInstanceAvailableCapacity(String instanceName);

  /**
   * Check if partition can be placed on the instance.
   *
   * @param instanceName - instance name 
   * @param partitionCapacity - Partition capacity expresed in capacity map.
   * @return boolean - True if the partition can be placed, False otherwise
   */
  public boolean isInstanceCapacityAvailable(String instanceName, Map<String, Integer> partitionCapacity);

  /**
   * Reduce the available capacity by specified Partition Capacity Map.
   *
   * @param instanceName - instance name 
   * @param partitionCapacity - Partition capacity expresed in capacity map.
   * @returns boolean - True if successfully updated partition capacity, false otherwise.
   */
  public boolean reduceAvailableInstanceCapacity(String instanceName, Map<String, Integer> partitionCapacity);
}
