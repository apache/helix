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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.controller.dataproviders.InstanceCapacityDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WagedInstanceCapacity implements InstanceCapacityDataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(WagedInstanceCapacity.class);

  // Available Capacity per Instance
  private final Map<String, Map<String, Integer>> _instanceCapacityMap;
  private final ResourceControllerDataProvider _cache;
  private final Map<String, Map<String, List<String>>> _instancePartitions;


  public WagedInstanceCapacity(ResourceControllerDataProvider clusterData) {
    _cache = clusterData;
    _instanceCapacityMap = new HashMap<>();
    _instancePartitions = new HashMap<>();

    ClusterConfig clusterConfig = _cache.getClusterConfig();
    for (InstanceConfig instanceConfig : _cache.getInstanceConfigMap().values()) {
      Map<String, Integer> instanceCapacity =
        WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
      _instanceCapacityMap.put(instanceConfig.getInstanceName(), instanceCapacity);
      _instancePartitions.put(instanceConfig.getInstanceName(), new HashMap<>());
    }
  }

  /**
   * Process the pending messages based on the Current states
   * @param currentState - Current state of the resources.

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
   */

  /**
   * Get the instance remaining capacity.
   * Capacity and weight both are represented as Key-Value.
   * Returns the capacity map of available head room for the instance.
   * @param instanceName - instance name to query
   * @return Map<String, Integer> - capacity pair for all defined attributes for the instance.
   */
  @Override
  public Map<String, Integer> getInstanceAvailableCapacity(String instanceName) {
    return _instanceCapacityMap.get(instanceName);
  }

  @Override
  public boolean isInstanceCapacityAvailable(String instance, Map<String, Integer> partitionCapacity) {
    Map<String, Integer> instanceCapacity = _instanceCapacityMap.get(instance);
    for (String key : instanceCapacity.keySet()) {
      int partCapacity = partitionCapacity.getOrDefault(key, 0);
      if (partCapacity != 0 && instanceCapacity.get(key) < partCapacity) {
        return false;
      }
    }
    return true;
  }

  private boolean reduceAvailableInstanceCapacity(String instance, Map<String, Integer> partitionCapacity) {
    Map<String, Integer> instanceCapacity = _instanceCapacityMap.get(instance);
    for (String key : instanceCapacity.keySet()) {
      if (partitionCapacity.containsKey(key)) {
        int partCapacity = partitionCapacity.getOrDefault(key, 0);
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

  public void process(Map<String, Resource> resourceMap, final CurrentStateOutput currentStateOutput,
      WagedResourceWeightsProvider weightProvider) {
    // For all resources, get the partition weight and add it to the instance capacity.

    for (String resourceName : resourceMap.keySet()) {
      Resource resource = resourceMap.get(resourceName);
      for (Partition partition : resource.getPartitions()) {
        String partitionName = partition.getPartitionName();
        Map<String, Integer> partitionCapacity = weightProvider.getPartitionWeights(resourceName, partitionName);
        for (String instance : currentStateOutput.getCurrentStateMap(resourceName).get(partitionName).keySet()) {
          Map<String, Integer> instanceCapacity = getInstanceAvailableCapacity(instance);
          if (instanceCapacity == null) {
            LOG.error("Instance Capacity not found for instance: " + instance);
            continue;
          }

          // This should not happen as we always have instance capacity for all instances.
          Map<String, List<String>> resourcePartitions = _instancePartitions.get(instance);
          if (resourcePartitions == null) {
            LOG.error("Instance Partitions not found for instance: " + instance);
            continue;
          }

          // Add resource/partition mapping to instances.
          reduceAvailableInstanceCapacity(instance, partitionCapacity);
          List<String> partitions = resourcePartitions.getOrDefault(resourceName, new ArrayList<>());
          partitions.add(partitionName);
        }
      }
    }
  }
}
