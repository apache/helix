package org.apache.helix.controller.rebalancer.util;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;


/**
 * A util class that contains validation-related static methods for WAGED rebalancer.
 */
public class WagedValidationUtil {
  /**
   * Validates and returns instance capacities. The validation logic ensures that all required capacity keys (in ClusterConfig) are present in InstanceConfig.
   * @param clusterConfig
   * @param instanceConfig
   * @return
   */
  public static Map<String, Integer> validateAndGetInstanceCapacity(ClusterConfig clusterConfig,
      InstanceConfig instanceConfig) {
    // Fetch the capacity of instance from 2 possible sources according to the following priority.
    // 1. The instance capacity that is configured in the instance config.
    // 2. If the default instance capacity that is configured in the cluster config contains more capacity keys, fill the capacity map with those additional values.
    Map<String, Integer> instanceCapacity =
        new HashMap<>(clusterConfig.getDefaultInstanceCapacityMap());
    instanceCapacity.putAll(instanceConfig.getInstanceCapacityMap());

    List<String> requiredCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    // All the required keys must exist in the instance config.
    if (!instanceCapacity.keySet().containsAll(requiredCapacityKeys)) {
      throw new HelixException(String.format(
          "The required capacity keys: %s are not fully configured in the instance: %s, capacity map: %s.",
          requiredCapacityKeys.toString(), instanceConfig.getInstanceName(),
          instanceCapacity.toString()));
    }
    return instanceCapacity;
  }

  /**
   * Validates and returns partition capacities. The validation logic ensures that all required capacity keys (from ClusterConfig) are present in the ResourceConfig for the partition.
   * @param partitionName
   * @param resourceConfig
   * @param clusterConfig
   * @return
   */
  public static Map<String, Integer> validateAndGetPartitionCapacity(String partitionName,
      ResourceConfig resourceConfig, Map<String, Map<String, Integer>> capacityMap,
      ClusterConfig clusterConfig) {
    // Fetch the capacity of partition from 3 possible sources according to the following priority.
    // 1. The partition capacity that is explicitly configured in the resource config.
    // 2. Or, the default partition capacity that is configured under partition name DEFAULT_PARTITION_KEY in the resource config.
    // 3. If the default partition capacity that is configured in the cluster config contains more capacity keys, fill the capacity map with those additional values.
    Map<String, Integer> partitionCapacity =
        new HashMap<>(clusterConfig.getDefaultPartitionWeightMap());
    partitionCapacity.putAll(capacityMap.getOrDefault(partitionName,
        capacityMap.getOrDefault(ResourceConfig.DEFAULT_PARTITION_KEY, new HashMap<>())));

    List<String> requiredCapacityKeys = clusterConfig.getInstanceCapacityKeys();
    // If any required capacity key is not configured in the resource config, fail the model creating.
    if (!partitionCapacity.keySet().containsAll(requiredCapacityKeys)) {
      throw new HelixException(String.format(
          "The required capacity keys: %s are not fully configured in the resource: %s, partition: %s, weight map: %s.",
          requiredCapacityKeys.toString(), resourceConfig.getResourceName(), partitionName,
          partitionCapacity.toString()));
    }
    return partitionCapacity;
  }
}
