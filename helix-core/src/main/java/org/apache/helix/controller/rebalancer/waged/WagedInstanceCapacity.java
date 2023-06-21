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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.controller.dataproviders.InstanceCapacityDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WagedInstanceCapacity implements InstanceCapacityDataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(WagedInstanceCapacity.class);

  // Available Capacity per Instance
  private final Map<String, Map<String, Integer>> _instanceCapacityMap;
  private final Map<String, Map<String, Set<String>>>  _allocatedPartitionMap;

  public WagedInstanceCapacity(ResourceControllerDataProvider clusterData) {
    _instanceCapacityMap = new ConcurrentHashMap<>();
    _allocatedPartitionMap = new ConcurrentHashMap<>();

    ClusterConfig clusterConfig = clusterData.getClusterConfig();
    for (InstanceConfig instanceConfig : clusterData.getInstanceConfigMap().values()) {
      Map<String, Integer> instanceCapacity =
        WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
      _instanceCapacityMap.put(instanceConfig.getInstanceName(), instanceCapacity);
    }
  }

  /**
   * Get the instance remaining capacity.
   * Capacity and weight both are represented as Key-Value.
   * Returns the capacity map of available headroom for the instance.
   * @param instanceName - instance name to query
   * @return Map<String, Integer> - capacity pair for all defined attributes for the instance.
   */
  @Override
  public Map<String, Integer> getInstanceAvailableCapacity(String instanceName) {
    return _instanceCapacityMap.get(instanceName);
  }

  @Override
  public boolean isInstanceCapacityAvailable(String instance, Map<String, Integer> partitionCapacity) {
    synchronized (_instanceCapacityMap) {
      Map<String, Integer> instanceCapacity = _instanceCapacityMap.get(instance);
      for (String key : instanceCapacity.keySet()) {
        int partCapacity = partitionCapacity.getOrDefault(key, 0);
        if (partCapacity != 0 && instanceCapacity.get(key) < partCapacity) {
          return false;
        }
      }
    }
    return true;
  }

  public boolean isPartitionAssigned(String instance, String resourceName, String partitionName) {
    return _allocatedPartitionMap.containsKey(instance)
        && _allocatedPartitionMap.get(instance).containsKey(resourceName)
        && _allocatedPartitionMap.get(instance).get(resourceName).contains(partitionName);
  }

  public boolean reduceAvailableInstanceCapacity(String instance,
      Map<String, Integer> partitionCapacity, String resourceName, String partitionName) {

    synchronized (_instanceCapacityMap) {
      Map<String, Integer> instanceCapacity = _instanceCapacityMap.get(instance);
      if (instanceCapacity == null) {
        LOG.error("Instance capacity for instance {} is not found.", instance);
        return false;
      }
      // Check if the instance has enough capacity to host the partition
      Set<String> keysUpdated = new HashSet<>();
      synchronized (instanceCapacity) {
        for (String key : instanceCapacity.keySet()) {
          if (partitionCapacity.containsKey(key)) {
            int partCapacity = partitionCapacity.get(key);
            // Check if the instance has enough capacity to host the partition for the given "key".
            if (instanceCapacity.get(key) < partCapacity) {
              // Clear the previously updated keys.
              for (String updatedKey : keysUpdated) {
                instanceCapacity.put(updatedKey, instanceCapacity.get(updatedKey)
                  + partitionCapacity.get(updatedKey));
              }
              return false;
            }
            keysUpdated.add(key);
            instanceCapacity.put(key, instanceCapacity.get(key) - partCapacity);
          }
        }
      } // end synchronized instanceCapacity
      _instanceCapacityMap.put(instance, instanceCapacity);
    } // end synchronized _instanceCapacityMap
    synchronized (_allocatedPartitionMap) {
       _allocatedPartitionMap.computeIfAbsent(instance, k -> new HashMap<>())
          .computeIfAbsent(resourceName, k -> new HashSet<>()).add(partitionName);
    } // end synchronized _allocatedPartitionMap
    return true;
  }
}
