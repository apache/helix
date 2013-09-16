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

import java.util.List;
import java.util.Map;

import org.apache.helix.api.PartitionId;
import org.apache.helix.api.ResourceId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;

/**
 * For each LiveInstances select currentState and message whose sessionId matches
 * sessionId from LiveInstance Get Partition,State for all the resources computed in
 * previous State [ResourceComputationStage]
 */
@Deprecated
public class CurrentStateComputationStage extends AbstractBaseStage {
  @Override
  public void process(ClusterEvent event) throws Exception {
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());

    if (cache == null || resourceMap == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires DataCache|RESOURCE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    for (LiveInstance instance : liveInstances.values()) {
      String instanceName = instance.getInstanceName();
      Map<String, Message> instanceMessages = cache.getMessages(instanceName);
      for (Message message : instanceMessages.values()) {
        if (!MessageType.STATE_TRANSITION.toString().equalsIgnoreCase(message.getMsgType())) {
          continue;
        }
        if (!instance.getSessionId().equals(message.getTgtSessionId())) {
          continue;
        }
        ResourceId resourceId = message.getResourceId();
        Resource resource = resourceMap.get(resourceId.stringify());
        if (resource == null) {
          continue;
        }

        if (!message.getBatchMessageMode()) {
          PartitionId partitionId = message.getPartitionId();
          Partition partition = resource.getPartition(partitionId.stringify());
          if (partition != null) {
            currentStateOutput.setPendingState(resourceId.stringify(), partition, instanceName,
                message.getToState().toString());
          } else {
            // log
          }
        } else {
          List<PartitionId> partitionNames = message.getPartitionIds();
          if (!partitionNames.isEmpty()) {
            for (PartitionId partitionId : partitionNames) {
              Partition partition = resource.getPartition(partitionId.stringify());
              if (partition != null) {
                currentStateOutput.setPendingState(resourceId.stringify(), partition, instanceName,
                    message.getToState().toString());
              } else {
                // log
              }
            }
          }
        }
      }
    }
    for (LiveInstance instance : liveInstances.values()) {
      String instanceName = instance.getInstanceName();

      String clientSessionId = instance.getSessionId().stringify();
      Map<String, CurrentState> currentStateMap =
          cache.getCurrentState(instanceName, clientSessionId);
      for (CurrentState currentState : currentStateMap.values()) {

        if (!instance.getSessionId().equals(currentState.getSessionId())) {
          continue;
        }
        String resourceName = currentState.getResourceName();
        String stateModelDefName = currentState.getStateModelDefRef();
        Resource resource = resourceMap.get(resourceName);
        if (resource == null) {
          continue;
        }
        if (stateModelDefName != null) {
          currentStateOutput.setResourceStateModelDef(resourceName, stateModelDefName);
        }

        currentStateOutput.setBucketSize(resourceName, currentState.getBucketSize());

        Map<String, String> partitionStateMap = currentState.getPartitionStateStringMap();
        for (String partitionName : partitionStateMap.keySet()) {
          Partition partition = resource.getPartition(partitionName);
          if (partition != null) {
            currentStateOutput.setCurrentState(resourceName, partition, instanceName,
                currentState.getState(partitionName));

          } else {
            // log
          }
        }
      }
    }
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);
  }
}
