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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.*;
import org.apache.helix.model.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * For each LiveInstances select currentState and message whose sessionId matches
 * sessionId from LiveInstance Get Partition,State for all the resources computed in
 * previous State [ResourceComputationStage]
 */
public class CurrentStateComputationStage extends AbstractBaseStage {
  private static Logger LOG = LoggerFactory.getLogger(CurrentStateComputationStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    BaseControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    final Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());

    if (cache == null || resourceMap == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires DataCache|RESOURCE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    final CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    for (LiveInstance instance : liveInstances.values()) {
      String instanceName = instance.getInstanceName();
      String instanceSessionId = instance.getSessionId();

      // update pending messages
      Map<String, Message> messages = cache.getMessages(instanceName);
      Map<String, Message> relayMessages = cache.getRelayMessages(instanceName);
      updatePendingMessages(instance, messages.values(), currentStateOutput, relayMessages.values(), resourceMap);

      // update current states.
      Map<String, CurrentState> currentStateMap = cache.getCurrentState(instanceName,
          instanceSessionId);
      updateCurrentStates(instance, currentStateMap.values(), currentStateOutput, resourceMap);
    }
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
  }

  // update all pending messages to CurrentStateOutput.
  private void updatePendingMessages(LiveInstance instance, Collection<Message> pendingMessages,
      CurrentStateOutput currentStateOutput, Collection<Message> pendingRelayMessages,
      Map<String, Resource> resourceMap) {
    String instanceName = instance.getInstanceName();
    String instanceSessionId = instance.getSessionId();

    // update all pending messages
    for (Message message : pendingMessages) {
      if (!MessageType.STATE_TRANSITION.name().equalsIgnoreCase(message.getMsgType())
          && !MessageType.STATE_TRANSITION_CANCELLATION.name().equalsIgnoreCase(message.getMsgType())) {
        continue;
      }
      if (!instanceSessionId.equals(message.getTgtSessionId())) {
        continue;
      }
      String resourceName = message.getResourceName();
      Resource resource = resourceMap.get(resourceName);
      if (resource == null) {
        LogUtil.logInfo(LOG, _eventId, String.format(
            "Ignore a pending relay message %s for a non-exist resource %s and partition %s",
            message.getMsgId(), resourceName, message.getPartitionName()));
        continue;
      }

      if (!message.getBatchMessageMode()) {
        String partitionName = message.getPartitionName();
        Partition partition = resource.getPartition(partitionName);
        if (partition != null) {
          setMessageState(currentStateOutput, resourceName, partition, instanceName, message);
        } else {
          LogUtil.logInfo(LOG, _eventId, String
              .format("Ignore a pending message %s for a non-exist resource %s and partition %s",
                  message.getMsgId(), resourceName, message.getPartitionName()));
        }
      } else {
        List<String> partitionNames = message.getPartitionNames();
        if (!partitionNames.isEmpty()) {
          for (String partitionName : partitionNames) {
            Partition partition = resource.getPartition(partitionName);
            if (partition != null) {
              setMessageState(currentStateOutput, resourceName, partition, instanceName, message);
            } else {
              LogUtil.logInfo(LOG, _eventId, String.format(
                  "Ignore a pending message %s for a non-exist resource %s and partition %s",
                  message.getMsgId(), resourceName, message.getPartitionName()));
            }
          }
        }
      }

      // Add the state model into the map for lookup of Task Framework pending partitions
      if (resource.getStateModelDefRef() != null) {
        currentStateOutput.setResourceStateModelDef(resourceName, resource.getStateModelDefRef());
      }
    }


    // update all pending relay messages
    for (Message message : pendingRelayMessages) {
      if (!message.isRelayMessage()) {
        LogUtil.logWarn(LOG, _eventId,
            String.format("Not a relay message %s, ignored!", message.getMsgId()));
        continue;
      }
      String resourceName = message.getResourceName();
      Resource resource = resourceMap.get(resourceName);
      if (resource == null) {
        LogUtil.logInfo(LOG, _eventId, String.format(
            "Ignore a pending relay message %s for a non-exist resource %s and partition %s",
            message.getMsgId(), resourceName, message.getPartitionName()));
        continue;
      }

      if (!message.getBatchMessageMode()) {
        String partitionName = message.getPartitionName();
        Partition partition = resource.getPartition(partitionName);
        if (partition != null) {
          currentStateOutput.setPendingRelayMessage(resourceName, partition, instanceName, message);
        } else {
          LogUtil.logInfo(LOG, _eventId, String.format(
              "Ignore a pending relay message %s for a non-exist resource %s and partition %s",
              message.getMsgId(), resourceName, message.getPartitionName()));
        }
      } else {
        LogUtil.logWarn(LOG, _eventId,
            String.format("A relay message %s should not be batched, ignored!", message.getMsgId()));
      }
    }
  }

  // update current states in CurrentStateOutput
  private void updateCurrentStates(LiveInstance instance, Collection<CurrentState> currentStates,
      CurrentStateOutput currentStateOutput, Map<String, Resource> resourceMap) {
    String instanceName = instance.getInstanceName();
    String instanceSessionId = instance.getSessionId();

    for (CurrentState currentState : currentStates) {
      if (!instanceSessionId.equals(currentState.getSessionId())) {
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

      Map<String, String> partitionStateMap = currentState.getPartitionStateMap();
      for (String partitionName : partitionStateMap.keySet()) {
        Partition partition = resource.getPartition(partitionName);
        if (partition != null) {
          currentStateOutput.setCurrentState(resourceName, partition, instanceName,
              currentState.getState(partitionName));
          currentStateOutput.setEndTime(resourceName, partition, instanceName,
              currentState.getEndTime(partitionName));
          String info = currentState.getInfo(partitionName);
          // This is to avoid null value entries in the map, and reduce memory usage by avoiding extra empty entries in the map.
          if (info != null) {
            currentStateOutput.setInfo(resourceName, partition, instanceName, info);
          }
          String requestState = currentState.getRequestedState(partitionName);
          if (requestState != null) {
            currentStateOutput
                .setRequestedState(resourceName, partition, instanceName, requestState);
          }
        }
      }
    }
  }

  private void setMessageState(CurrentStateOutput currentStateOutput, String resourceName,
      Partition partition, String instanceName, Message message) {
    if (MessageType.STATE_TRANSITION.name().equalsIgnoreCase(message.getMsgType())) {
      currentStateOutput.setPendingMessage(resourceName, partition, instanceName, message);
    } else {
      currentStateOutput.setCancellationMessage(resourceName, partition, instanceName, message);
    }
  }
}
