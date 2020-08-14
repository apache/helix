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

import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomizedStateComputationStage extends AbstractBaseStage {
  private static Logger LOG = LoggerFactory.getLogger(CustomizedStateComputationStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    ResourceControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    final Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    Set<String> aggregationEnabledTypes = cache.getAggregationEnabledCustomizedStateTypes();

    if (cache == null || resourceMap == null) {
      throw new StageException(
          "Missing attributes in event:" + event + ". Requires DataCache|RESOURCE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    final CustomizedStateOutput customizedStateOutput = new CustomizedStateOutput();

    for (LiveInstance instance : liveInstances.values()) {
      String instanceName = instance.getInstanceName();
      // update customized states.
      for (String customizedStateType : aggregationEnabledTypes) {
        Map<String, CustomizedState> customizedStateMap =
            cache.getCustomizedState(instanceName, customizedStateType);
        updateCustomizedStates(instanceName, customizedStateType, customizedStateMap,
            customizedStateOutput, resourceMap);
      }
    }
    event.addAttribute(AttributeName.CUSTOMIZED_STATE.name(), customizedStateOutput);
  }

  // update customized state in CustomizedStateOutput
  private void updateCustomizedStates(String instanceName, String customizedStateType,
      Map<String, CustomizedState> customizedStates, CustomizedStateOutput customizedStateOutput,
      Map<String, Resource> resourceMap) {
    // for each CustomizedState, update corresponding entry in CustomizedStateOutput
    for (CustomizedState customizedState : customizedStates.values()) {
      String resourceName = customizedState.getResourceName();
      Resource resource = resourceMap.get(resourceName);
      if (resource == null) {
        continue;
      }

      Map<String, String> partitionStateMap = customizedState
          .getPartitionStateMap(CustomizedState.CustomizedStateProperty.CURRENT_STATE);
      for (String partitionName : partitionStateMap.keySet()) {
        Partition partition = resource.getPartition(partitionName);
        if (partition != null) {
          customizedStateOutput
              .setCustomizedState(customizedStateType, resourceName, partition, instanceName,
                  customizedState.getState(partitionName),
                  customizedState.getStartTime(partitionName));
        }
      }
    }
  }
}