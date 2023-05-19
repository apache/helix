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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.controller.dataproviders.InstanceCapacityDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WagedInstanceCapacity implements InstanceCapacityDataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(WagedInstanceCapacity.class);

  // Available Capacity per Instance
  private Map<String, Map<String, Integer>> _perInstance;
  private ResourceControllerDataProvider _cache;

  public WagedInstanceCapacity(ResourceControllerDataProvider clusterData) {
    _cache = clusterData;
    _perInstance = new HashMap<>();

    ClusterConfig clusterConfig = _cache.getClusterConfig();
    for (InstanceConfig instanceConfig : _cache.getInstanceConfigMap().values()) {
      Map<String, Integer> instanceCapacity =
        WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
      _perInstance.put(instanceConfig.getInstanceName(), instanceCapacity);
    }
  }

  /**
   * Create Default Capacity Map.
   * This is a utility method to create a default capacity map matching instance capacity map for participants.
   * This is required as non-WAGED partitions will be placed on same instance and we don't know their actual capacity.
   * This will generate default values of 0 for all the capacity keys.
   */
  private Map<String, Integer> createDefaultParticipantWeight() {
    // copy the value of first Instance capacity.
    Map<String, Integer> partCapacity = new HashMap<>(_perInstance.values().iterator().next());

    // reset the value of all capacity to 0.
    for (String key : partCapacity.keySet()) {
      partCapacity.put(key, 0);
    }
    return partCapacity;
  }

  /**
   * Process the pending messages based on the Current states
   * @param currentState - Current state of the resources.
   */
  public void processPendingMessages(CurrentStateOutput currentState) {
    Map<String, Map<Partition, Map<String, Message>>> pendingMsgs = currentState.getPendingMessages();
    
    for (String resource : pendingMsgs.keySet()) {
      Map<Partition, Map<String, Message>> partitionMsgs = pendingMsgs.get(resource);

      for (Partition partition : partitionMsgs.keySet()) {
        String partitionName = partition.getPartitionName();

        // Get Partition Weight
        Map<String, Integer> partCapacity = getPartitionCapacity(resource, partitionName);

        // TODO - check
        Map<String, Message> msgs = partitionMsgs.get(partition);
        // TODO - Check
        for (String instance : msgs.keySet()) {
           reduceAvailableInstanceCapacity(instance, partCapacity);
        }
      }
    }
  }

  /**
   * Get the partition capacity given Resource and Partition name.
   */
  private Map<String, Integer> getPartitionCapacity(String resource, String partition) {
    ClusterConfig clusterConfig = _cache.getClusterConfig();
    ResourceConfig resourceConfig = _cache.getResourceConfig(resource);


    // Parse the entire capacityMap from ResourceConfig
    Map<String, Map<String, Integer>> capacityMap;
    try {
      capacityMap = resourceConfig.getPartitionCapacityMap();
    } catch (IOException ex) {
      return createDefaultParticipantWeight();
    }
    return WagedValidationUtil.validateAndGetPartitionCapacity(partition, resourceConfig, capacityMap, clusterConfig);
  }

  /**
   * Get the instance remaining capacity.
   * Capacity and weight both are represented as Key-Value.
   * Returns the capacity map of available head room for the instance.
   * @param instanceName - instance name to query
   * @return Map<String, Integer> - capacity pair for all defined attributes for the instance.
   */
  @Override
  public Map<String, Integer> getInstanceAvailableCapacity(String instanceName) {
    return _perInstance.get(instanceName);
  }

  @Override
  public boolean isInstanceCapacityAvailable(String instance, Map<String, Integer> partitionCapacity) {
    Map<String, Integer> instanceCapacity = _perInstance.get(instance);
    for (String key : instanceCapacity.keySet()) {
      Integer partCapacity = partitionCapacity.getOrDefault(key, 0);
      if (partCapacity != 0 && instanceCapacity.get(key) < partCapacity) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean reduceAvailableInstanceCapacity(String instance, Map<String, Integer> partitionCapacity) {
    Map<String, Integer> instanceCapacity = _perInstance.get(instance);
    for (String key : instanceCapacity.keySet()) {
      if (partitionCapacity.containsKey(key)) {
        Integer partCapacity = partitionCapacity.getOrDefault(key, 0);
        if (partCapacity != 0 && instanceCapacity.get(key) < partCapacity) {
          return false;
        }
        if (partCapacity != 0) {
          instanceCapacity.put(key, instanceCapacity.get(key) - partCapacity);
        }
      }
    }
    return true;
  }
}
