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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.controller.dataproviders.InstanceCapacityDataProvider;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WagedInstanceCapacity implements InstanceCapacityDataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(WagedInstanceCapacity.class);

  // Available Capacity per Instance
  private final Map<String, Map<String, Integer>> _instanceCapacityMap;
  private final Map<String, Map<String, Set<String>>> _allocatedPartitionsMap;

  public WagedInstanceCapacity(ResourceControllerDataProvider clusterData) {
    _instanceCapacityMap = new HashMap<>();
    _allocatedPartitionsMap = new HashMap<>();
    ClusterConfig clusterConfig = clusterData.getClusterConfig();
    if (clusterConfig == null) {
      LOG.error("Cluster config is null, cannot initialize instance capacity map.");
      return;
    }
    for (InstanceConfig instanceConfig : clusterData.getInstanceConfigMap().values()) {
      Map<String, Integer> instanceCapacity = WagedValidationUtil.validateAndGetInstanceCapacity(clusterConfig, instanceConfig);
      _instanceCapacityMap.put(instanceConfig.getInstanceName(), instanceCapacity);
      _allocatedPartitionsMap.put(instanceConfig.getInstanceName(), new HashMap<>());
    }
  }

  // Helper methods.
  // TODO: Currently, we don't allow double-accounting. But there may be
  // future scenarios, where we may want to allow.
  private boolean hasPartitionChargedForCapacity(String instance, String resource, String partition) {
    if (!_allocatedPartitionsMap.containsKey(instance)) {
      _allocatedPartitionsMap.put(instance, new HashMap<>());
      return false;
    }
    return _allocatedPartitionsMap.get(instance).containsKey(resource)
        && _allocatedPartitionsMap.get(instance).get(resource).contains(partition);
  }

  public void process(ResourceControllerDataProvider cache, CurrentStateOutput currentStateOutput,
      Map<String, Resource> resourceMap, WagedResourceWeightsProvider weightProvider) {
    processCurrentState(cache, currentStateOutput, resourceMap, weightProvider);
    processPendingMessages(cache, currentStateOutput, resourceMap, weightProvider);
  }

  /**
   * Process the pending messages based on the Current states
   * @param currentState - Current state of the resources.
   */
  public void processPendingMessages(ResourceControllerDataProvider cache,
      CurrentStateOutput currentState, Map<String, Resource> resourceMap,
      WagedResourceWeightsProvider weightProvider) {

    for (Map.Entry<String, Resource> resourceEntry : resourceMap.entrySet()) {
      String resName = resourceEntry.getKey();
      Resource resource = resourceEntry.getValue();

      // if Resource is WAGED managed, then we need to manage the capacity.
      if (!WagedValidationUtil.isWagedEnabled(cache.getIdealState(resName))) {
        continue;
      }

      // list of partitions in the resource
      Collection<Partition> partitions = resource.getPartitions();
      // State model definition for the resource
      StateModelDefinition stateModelDef = cache.getStateModelDef(resource.getStateModelDefRef());
      if (stateModelDef == null) {
        LOG.warn("State Model Definition for resource: " + resName + " is null");
        continue;
      }
      Map<String, Integer> statePriorityMap = stateModelDef.getStatePriorityMap();

      for (Partition partition : partitions) {
        String partitionName = partition.getPartitionName();
        // Get Partition Weight
        Map<String, Integer> partCapacity = weightProvider.getPartitionWeights(resName, partitionName);
        if (partCapacity == null || partCapacity.isEmpty()) {
          LOG.info("Partition: " + partitionName + " in resource: " + resName
              + " has no weight specified. Skipping it.");
          continue;
        }
        // Get the pending messages for the partition
        Map<String, Message> pendingMessages = currentState.getPendingMessageMap(resName, partition);
        if (pendingMessages != null && !pendingMessages.isEmpty()) {
          for (Map.Entry<String, Message> entry :  pendingMessages.entrySet()) {
            String instance = entry.getKey();
            if (hasPartitionChargedForCapacity(instance, resName, partitionName)) {
              continue;
            }
            Message msg = entry.getValue();
            // If boot-strapping message is pending, we should deduct the capacity.
            if (statePriorityMap.get(msg.getFromState()) < statePriorityMap.get(msg.getToState())
                && msg.getFromState().equals(stateModelDef.getInitialState())) {
              LOG.info("For bootstrappiing - deducting capacity for instance: " + instance
                  + " for resource: " + resName + " for partition: " + partitionName);
              checkAndReduceInstanceCapacity(instance, resName, partitionName, partCapacity);
            }
          }
        }
      }
    }
  }

  private void processCurrentState(ResourceControllerDataProvider cache,
      CurrentStateOutput currentStateOutput, Map<String, Resource> resourceMap,
      WagedResourceWeightsProvider weightProvider) {

    // Iterate through all the resources
    for (Map.Entry<String, Resource> entry : resourceMap.entrySet()) {
      String resName = entry.getKey();
      Resource resource = entry.getValue();

      // if Resource is WAGED managed, then we need to manage the capacity.
      if (!WagedValidationUtil.isWagedEnabled(cache.getIdealState(resName))) {
        continue;
      }

      // list of partitions in the resource
      Collection<Partition> partitions = resource.getPartitions();

      for (Partition partition : partitions) {
        String partitionName = partition.getPartitionName();
        // Get Partition Weight
        Map<String, Integer> partCapacity = weightProvider.getPartitionWeights(resName, partitionName);
        // Get the current state for the partition
        Map<String, String> currentStateMap = currentStateOutput.getCurrentStateMap(resName, partition);
        if (currentStateMap != null && !currentStateMap.isEmpty()) {
          for (String instance : currentStateMap.keySet()) {
            checkAndReduceInstanceCapacity(instance, resName, partitionName, partCapacity);
          }
        }
      }
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
    Map<String, Integer> instanceCapacity = _instanceCapacityMap.get(instance);
    for (String key : instanceCapacity.keySet()) {
      int partCapacity = partitionCapacity.getOrDefault(key, 0);
      if (partCapacity != 0 && instanceCapacity.get(key) < partCapacity) {
        return false;
      }
    }
    return true;
  }


  public synchronized boolean checkAndReduceInstanceCapacity(String instance, String resName,
      String partitionName, Map<String, Integer> partitionCapacity) {

    if (hasPartitionChargedForCapacity(instance, resName, partitionName)) {
      LOG.info("Instance: " + instance + " for resource: " + resName
          + " for partition: " + partitionName + " already charged for capacity.");
      return true;
    }

    Map<String, Integer> instanceCapacity = _instanceCapacityMap.get(instance);
    Map<String, Integer> processedCapacity = new HashMap<>();
    for (String key : instanceCapacity.keySet()) {
      if (partitionCapacity.containsKey(key)) {
        int partCapacity = partitionCapacity.get(key);
        if (instanceCapacity.get(key) < partCapacity) {
          // rollback the previously processed capacity.
          for (String processedKey : processedCapacity.keySet()) {
            instanceCapacity.put(processedKey, instanceCapacity.get(processedKey) + processedCapacity.get(processedKey));
          }
          return false;
        }
        instanceCapacity.put(key, instanceCapacity.get(key) - partCapacity);
        processedCapacity.put(key, partCapacity);
      }
    }
    _allocatedPartitionsMap.computeIfAbsent(instance, k -> new HashMap<>())
        .computeIfAbsent(resName, k -> new HashSet<>()).add(partitionName);
    LOG.info("Reduced capacity for instance: " + instance + " for resource: " + resName
        + " for partition: " + partitionName);
    return true;
  }
}
