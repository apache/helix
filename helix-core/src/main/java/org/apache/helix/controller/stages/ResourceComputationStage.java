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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.WorkflowConfig;
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
    _eventId = event.getEventId();
    BaseControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    if (cache == null) {
      throw new StageException("Missing attributes in event:" + event + ". Requires DataCache");
    }

    Map<String, IdealState> idealStates = cache.getIdealStates();

    Map<String, Resource> resourceMap = new LinkedHashMap<>();
    Map<String, Resource> resourceToRebalance = new LinkedHashMap<>();
    Map<String, Resource> taskResourcesToDrop = new LinkedHashMap<>();

    boolean isTaskCache = cache instanceof WorkflowControllerDataProvider;

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

          if (!isTaskCache && (!idealState.isValid() || !idealState.getStateModelDefRef()
              .equals(TaskConstants.STATE_MODEL_NAME))) {
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

    // Add TaskFramework resources from workflow and job configs as Task Framework will no longer
    // use IdealState
    if (isTaskCache) {
      WorkflowControllerDataProvider taskDataCache =
          event.getAttribute(AttributeName.ControllerDataProvider.name());
      for (Map.Entry<String, WorkflowConfig> workflowConfigEntry : taskDataCache
          .getWorkflowConfigMap().entrySet()) {
        // always overwrite, because the resource could be created by IS
        String resourceName = workflowConfigEntry.getKey();
        WorkflowConfig workflowConfig = workflowConfigEntry.getValue();
        addResourceConfigToResourceMap(resourceName, workflowConfig, cache.getClusterConfig(),
            resourceMap, resourceToRebalance);
        addPartition(resourceName, resourceName, resourceMap);
      }

      for (Map.Entry<String, JobConfig> jobConfigEntry : taskDataCache.getJobConfigMap()
          .entrySet()) {
        // always overwrite, because the resource could be created by IS
        String resourceName = jobConfigEntry.getKey();
        JobConfig jobConfig = jobConfigEntry.getValue();
        addResourceConfigToResourceMap(resourceName, jobConfig, cache.getClusterConfig(),
            resourceMap, resourceToRebalance);
        int numPartitions = jobConfig.getTaskConfigMap().size();
        if (numPartitions == 0 && idealStates != null) {
          IdealState targetIs = idealStates.get(jobConfig.getTargetResource());
          if (targetIs == null) {
            LOG.warn("Target resource does not exist for job " + resourceName);
          } else {
            numPartitions = targetIs.getPartitionSet().size();
          }
        }
        for (int i = 0; i < numPartitions; i++) {
          addPartition(resourceName + "_" + i, resourceName, resourceMap);
        }
      }
    }

    // It's important to get partitions from CurrentState as well since the
    // idealState might be removed.
    Map<String, LiveInstance> availableInstances = cache.getLiveInstances();

    if (availableInstances != null && availableInstances.size() > 0) {
      for (LiveInstance instance : availableInstances.values()) {
        String instanceName = instance.getInstanceName();
        String clientSessionId = instance.getEphemeralOwner();

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
            if (!isTaskCache && (resource.getStateModelDefRef() == null
                || !TaskConstants.STATE_MODEL_NAME.equals(resource.getStateModelDefRef()))) {
              resourceToRebalance.put(resourceName, resource);
            }

            if (isTaskCache && TaskConstants.STATE_MODEL_NAME
                .equals(resource.getStateModelDefRef())) {
              // If a task current state exists without configs, it needs to be cleaned up
              taskResourcesToDrop.put(resourceName, resource);
              resourceToRebalance.put(resourceName, resource);
            }

            IdealState idealState = idealStates.get(resourceName);
            if (idealState != null) {
              resource.setResourceGroupName(idealState.getResourceGroupName());
              resource.setResourceTag(idealState.getInstanceGroupTag());
            }
          }

          if (currentState.getStateModelDefRef() == null) {
            LogUtil.logError(LOG, _eventId,
                "state model def is null." + "resource:" + currentState.getResourceName()
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
    if (isTaskCache) {
      event.addAttribute(AttributeName.TASK_RESOURCES_TO_DROP.name(), taskResourcesToDrop);
    }
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

  private void addResourceConfigToResourceMap(String resourceName, ResourceConfig resourceConfig,
      ClusterConfig clusterConfig, Map<String, Resource> resourceMap,
      Map<String, Resource> resourceToRebalance) {
    Resource resource = new Resource(resourceName, clusterConfig, resourceConfig);
    resourceMap.put(resourceName, resource);
    resource.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    resource.setStateModelFactoryName(resourceConfig.getStateModelFactoryName());
    boolean batchMessageMode = resourceConfig.getBatchMessageMode();
    if (clusterConfig != null) {
      batchMessageMode |= clusterConfig.getBatchMessageMode();
    }
    resource.setBatchMessageMode(batchMessageMode);
    resource.setResourceGroupName(resourceConfig.getResourceGroupName());
    resource.setResourceTag(resourceConfig.getInstanceGroupTag());
    resourceToRebalance.put(resourceName, resource);
  }
}
