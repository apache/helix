package org.apache.helix.controller.stages;

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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
import org.apache.helix.task.TaskConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stage computes all the resources in a cluster. The resources are
 * computed from IdealStates -> this gives all the resources currently active
 * CurrentState for liveInstance-> Helps in finding resources that are inactive
 * and needs to be dropped
 */
public class ResourceComputationStage extends AbstractBaseStage {
  private static Logger LOG = LoggerFactory.getLogger(ResourceComputationStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    if (cache == null) {
      throw new StageException("Missing attributes in event:" + event + ". Requires DataCache");
    }

    Map<String, IdealState> idealStates = cache.getIdealStates();

    Map<String, Resource> resourceMap = new LinkedHashMap<String, Resource>();
    Map<String, Resource> resourceToRebalance = new LinkedHashMap<>();

    if (idealStates != null && idealStates.size() > 0) {
      for (IdealState idealState : idealStates.values()) {
        if (idealState == null) {
          continue;
        }
        Set<String> partitionSet = idealState.getPartitionSet();
        String resourceName = idealState.getResourceName();
        if (!resourceMap.containsKey(resourceName)) {
          Resource resource = new Resource(resourceName, cache.getClusterConfig(),
              cache.getResourceConfig(resourceName));
          resourceMap.put(resourceName, resource);

          if (!idealState.isValid() && !cache.isTaskCache()
              || idealState.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME) && cache
              .isTaskCache()
              || !idealState.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME) && !cache
              .isTaskCache()) {
            resourceToRebalance.put(resourceName, resource);
          }
          resource.setStateModelDefRef(idealState.getStateModelDefRef());
          resource.setStateModelFactoryName(idealState.getStateModelFactoryName());
          resource.setBucketSize(idealState.getBucketSize());
          boolean batchMessageMode = idealState.getBatchMessageMode();
          ClusterConfig clusterConfig = cache.getClusterConfig();
          if (clusterConfig != null) {
            batchMessageMode |= clusterConfig.getBatchMessageMode();
          }
          resource.setBatchMessageMode(batchMessageMode);
          resource.setResourceGroupName(idealState.getResourceGroupName());
          resource.setResourceTag(idealState.getInstanceGroupTag());
        }

        for (String partition : partitionSet) {
          addPartition(partition, resourceName, resourceMap);
        }
      }
    }

    // It's important to get partitions from CurrentState as well since the
    // idealState might be removed.
    Map<String, LiveInstance> availableInstances = cache.getLiveInstances();

    if (availableInstances != null && availableInstances.size() > 0) {
      for (LiveInstance instance : availableInstances.values()) {
        String instanceName = instance.getInstanceName();
        String clientSessionId = instance.getSessionId();

        Map<String, CurrentState> currentStateMap =
            cache.getCurrentState(instanceName, clientSessionId);
        if (currentStateMap == null || currentStateMap.size() == 0) {
          continue;
        }
        for (CurrentState currentState : currentStateMap.values()) {

          String resourceName = currentState.getResourceName();
          Map<String, String> resourceStateMap = currentState.getPartitionStateMap();

          if (resourceStateMap.keySet().isEmpty()) {
            // don't include empty current state for dropped resource
            continue;
          }

          // don't overwrite ideal state settings
          if (!resourceMap.containsKey(resourceName)) {
            addResource(resourceName, resourceMap);
            Resource resource = resourceMap.get(resourceName);
            resource.setStateModelDefRef(currentState.getStateModelDefRef());
            resource.setStateModelFactoryName(currentState.getStateModelFactoryName());
            resource.setBucketSize(currentState.getBucketSize());
            resource.setBatchMessageMode(currentState.getBatchMessageMode());
            if (resource.getStateModelDefRef() == null && !cache.isTaskCache()
                || resource.getStateModelDefRef() != null && (
                resource.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME) && cache
                    .isTaskCache()
                    || !resource.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)
                    && !cache.isTaskCache())) {
              resourceToRebalance.put(resourceName, resource);
            }

            IdealState idealState = idealStates.get(resourceName);
            if (idealState != null) {
              resource.setResourceGroupName(idealState.getResourceGroupName());
              resource.setResourceTag(idealState.getInstanceGroupTag());
            }
          }

          if (currentState.getStateModelDefRef() == null) {
            LOG.error("state model def is null." + "resource:" + currentState.getResourceName()
                + ", partitions: " + currentState.getPartitionStateMap().keySet() + ", states: "
                + currentState.getPartitionStateMap().values());
            throw new StageException("State model def is null for resource:"
                + currentState.getResourceName());
          }

          for (String partition : resourceStateMap.keySet()) {
            addPartition(partition, resourceName, resourceMap);
          }
        }
      }
    }

    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceToRebalance);
  }

  private void addResource(String resource, Map<String, Resource> resourceMap) {
    if (resource == null || resourceMap == null) {
      return;
    }
    if (!resourceMap.containsKey(resource)) {
      resourceMap.put(resource, new Resource(resource));
    }
  }

  private void addPartition(String partition, String resourceName, Map<String, Resource> resourceMap) {
    if (resourceName == null || partition == null || resourceMap == null) {
      return;
    }
    if (!resourceMap.containsKey(resourceName)) {
      resourceMap.put(resourceName, new Resource(resourceName));
    }
    Resource resource = resourceMap.get(resourceName);
    resource.addPartition(partition);

  }
}
