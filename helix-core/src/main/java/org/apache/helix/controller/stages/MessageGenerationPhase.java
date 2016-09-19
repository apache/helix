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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.log4j.Logger;

/**
 * Compares the currentState, pendingState with IdealState and generate messages
 */
public class MessageGenerationPhase extends AbstractBaseStage {
  private static Logger logger = Logger.getLogger(MessageGenerationPhase.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager manager = event.getAttribute("helixmanager");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    if (manager == null || cache == null || resourceMap == null || currentStateOutput == null
        || bestPossibleStateOutput == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|DataCache|RESOURCES|CURRENT_STATE|BEST_POSSIBLE_STATE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    Map<String, String> sessionIdMap = new HashMap<String, String>();

    for (LiveInstance liveInstance : liveInstances.values()) {
      sessionIdMap.put(liveInstance.getInstanceName(), liveInstance.getSessionId());
    }
    MessageGenerationOutput output = new MessageGenerationOutput();

    for (String resourceName : resourceMap.keySet()) {
      Resource resource = resourceMap.get(resourceName);

      StateModelDefinition stateModelDef = cache.getStateModelDef(resource.getStateModelDefRef());

      for (Partition partition : resource.getPartitions()) {

        Map<String, String> instanceStateMap =
            bestPossibleStateOutput.getInstanceStateMap(resourceName, partition);

        // we should generate message based on the desired-state priority
        // so keep generated messages in a temp map keyed by state
        // desired-state->list of generated-messages
        Map<String, List<Message>> messageMap = new HashMap<String, List<Message>>();

        for (String instanceName : instanceStateMap.keySet()) {
          String desiredState = instanceStateMap.get(instanceName);

          String currentState =
              currentStateOutput.getCurrentState(resourceName, partition, instanceName);
          if (currentState == null) {
            currentState = stateModelDef.getInitialState();
          }

          if (desiredState.equalsIgnoreCase(currentState)) {
            continue;
          }

          Message pendingMessage =
              currentStateOutput.getPendingState(resourceName, partition, instanceName);

          String nextState = stateModelDef.getNextStateForTransition(currentState, desiredState);
          if (nextState == null) {
            logger.error("Unable to find a next state for resource: " + resource.getResourceName()
                + " partition: " + partition.getPartitionName() + " from stateModelDefinition"
                + stateModelDef.getClass() + " from:" + currentState + " to:" + desiredState);
            continue;
          }

          if (pendingMessage != null) {
            String pendingState = pendingMessage.getToState();
            if (nextState.equalsIgnoreCase(pendingState)) {
              logger.debug("Message already exists for " + instanceName + " to transit " + resource
                  .getResourceName() + "." + partition.getPartitionName() + " from " + currentState
                  + " to " + nextState);
            } else if (currentState.equalsIgnoreCase(pendingState)) {
              logger.info("Message hasn't been removed for " + instanceName + " to transit " +
                  resource.getResourceName() + "." + partition.getPartitionName() + " to "
                      + pendingState + ", desiredState: " + desiredState);
            } else {
              logger.info("IdealState changed before state transition completes for " +
                  resource.getResourceName() + "." + partition.getPartitionName() + " on "
                      + instanceName + ", pendingState: " + pendingState + ", currentState: "
                      + currentState + ", nextState: " + nextState);
            }
          } else {

            Message message =
                createMessage(manager, resource, partition.getPartitionName(), instanceName,
                    currentState, nextState, sessionIdMap.get(instanceName), stateModelDef.getId());

            IdealState idealState = cache.getIdealState(resourceName);
            if (idealState != null
                && idealState.getStateModelDefRef().equalsIgnoreCase(
                    DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
              if (idealState.getRecord().getMapField(partition.getPartitionName()) != null) {
                message.getRecord().setMapField(Message.Attributes.INNER_MESSAGE.toString(),
                    idealState.getRecord().getMapField(partition.getPartitionName()));
              }
            }
            // Set timeout of needed
            String stateTransition =
                currentState + "-" + nextState + "_" + Message.Attributes.TIMEOUT;
            if (idealState != null) {
              String timeOutStr = idealState.getRecord().getSimpleField(stateTransition);
              if (timeOutStr == null
                  && idealState.getStateModelDefRef().equalsIgnoreCase(
                      DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
                // scheduled task queue
                if (idealState.getRecord().getMapField(partition.getPartitionName()) != null) {
                  timeOutStr =
                      idealState.getRecord().getMapField(partition.getPartitionName())
                          .get(Message.Attributes.TIMEOUT.toString());
                }
              }
              if (timeOutStr != null) {
                try {
                  int timeout = Integer.parseInt(timeOutStr);
                  if (timeout > 0) {
                    message.setExecutionTimeout(timeout);
                  }
                } catch (Exception e) {
                  logger.error("", e);
                }
              }
            }
            message.getRecord().setSimpleField("ClusterEventName", event.getName());
            // output.addMessage(resourceName, partition, message);
            if (!messageMap.containsKey(desiredState)) {
              messageMap.put(desiredState, new ArrayList<Message>());
            }
            messageMap.get(desiredState).add(message);
          }
        }

        // add generated messages to output according to state priority
        List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
        for (String state : statesPriorityList) {
          if (messageMap.containsKey(state)) {
            for (Message message : messageMap.get(state)) {
              output.addMessage(resourceName, partition, message);
            }
          }
        }

      } // end of for-each-partition
    }
    event.addAttribute(AttributeName.MESSAGES_ALL.toString(), output);
  }

  private Message createMessage(HelixManager manager, Resource resource, String partitionName,
      String instanceName, String currentState, String nextState, String sessionId,
      String stateModelDefName) {
    String uuid = UUID.randomUUID().toString();
    Message message = new Message(MessageType.STATE_TRANSITION, uuid);
    message.setSrcName(manager.getInstanceName());
    message.setTgtName(instanceName);
    message.setMsgState(MessageState.NEW);
    message.setPartitionName(partitionName);
    message.setResourceName(resource.getResourceName());
    message.setFromState(currentState);
    message.setToState(nextState);
    message.setTgtSessionId(sessionId);
    message.setSrcSessionId(manager.getSessionId());
    message.setStateModelDef(stateModelDefName);
    message.setStateModelFactoryName(resource.getStateModelFactoryname());
    message.setBucketSize(resource.getBucketSize());

    if (resource.getResourceGroupName() != null) {
      message.setResourceGroupName(resource.getResourceGroupName());
    }
    if (resource.getResourceTag() != null) {
      message.setResourceTag(resource.getResourceTag());
    }

    return message;
  }
}
