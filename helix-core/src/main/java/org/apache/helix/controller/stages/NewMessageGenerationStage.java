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
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Id;
import org.apache.helix.api.MessageId;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.RebalancerConfig;
import org.apache.helix.api.Resource;
import org.apache.helix.api.ResourceConfig;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.SessionId;
import org.apache.helix.api.State;
import org.apache.helix.api.StateModelDefId;
import org.apache.helix.api.StateModelFactoryId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * Compares the currentState, pendingState with IdealState and generate messages
 */
public class NewMessageGenerationStage extends AbstractBaseStage {
  private static Logger LOG = Logger.getLogger(NewMessageGenerationStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager manager = event.getAttribute("helixmanager");
    Cluster cluster = event.getAttribute("ClusterDataCache");
    Map<StateModelDefId, StateModelDefinition> stateModelDefMap =
        event.getAttribute(AttributeName.STATE_MODEL_DEFINITIONS.toString());
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    NewCurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    NewBestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    if (manager == null || cluster == null || resourceMap == null || currentStateOutput == null
        || bestPossibleStateOutput == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|DataCache|RESOURCES|CURRENT_STATE|BEST_POSSIBLE_STATE");
    }

    NewMessageOutput output = new NewMessageOutput();

    for (ResourceId resourceId : resourceMap.keySet()) {
      ResourceConfig resource = resourceMap.get(resourceId);
      int bucketSize = resource.getRebalancerConfig().getBucketSize();

      StateModelDefinition stateModelDef =
          stateModelDefMap.get(resource.getRebalancerConfig().getStateModelDefId());

      ResourceAssignment resourceAssignment =
          bestPossibleStateOutput.getResourceAssignment(resourceId);
      for (PartitionId partitionId : resource.getPartitionMap().keySet()) {
        Map<ParticipantId, State> instanceStateMap = resourceAssignment.getReplicaMap(partitionId);

        // we should generate message based on the desired-state priority
        // so keep generated messages in a temp map keyed by state
        // desired-state->list of generated-messages
        Map<State, List<Message>> messageMap = new HashMap<State, List<Message>>();

        for (ParticipantId participantId : instanceStateMap.keySet()) {
          State desiredState = instanceStateMap.get(participantId);

          State currentState =
              currentStateOutput.getCurrentState(resourceId, partitionId, participantId);
          if (currentState == null) {
            currentState = stateModelDef.getInitialState();
          }

          if (desiredState.equals(currentState)) {
            continue;
          }

          State pendingState =
              currentStateOutput.getPendingState(resourceId, partitionId, participantId);

          // TODO fix it
          State nextState = stateModelDef.getNextStateForTransition(currentState, desiredState);
          if (nextState == null) {
            LOG.error("Unable to find a next state for partition: " + partitionId
                + " from stateModelDefinition" + stateModelDef.getClass() + " from:" + currentState
                + " to:" + desiredState);
            continue;
          }

          if (pendingState != null) {
            if (nextState.equals(pendingState)) {
              LOG.debug("Message already exists for " + participantId + " to transit "
                  + partitionId + " from " + currentState + " to " + nextState);
            } else if (currentState.equals(pendingState)) {
              LOG.info("Message hasn't been removed for " + participantId + " to transit"
                  + partitionId + " to " + pendingState + ", desiredState: " + desiredState);
            } else {
              LOG.info("IdealState changed before state transition completes for " + partitionId
                  + " on " + participantId + ", pendingState: " + pendingState + ", currentState: "
                  + currentState + ", nextState: " + nextState);
            }
          } else {
            // TODO check if instance is alive
            SessionId sessionId =
                cluster.getLiveParticipantMap().get(participantId).getRunningInstance()
                    .getSessionId();
            Message message =
                createMessage(manager, resourceId, partitionId, participantId, currentState,
                    nextState, sessionId, new StateModelDefId(stateModelDef.getId()), resource
                        .getRebalancerConfig().getStateModelFactoryId(), bucketSize);

            // TODO refactor set timeout logic, it's really messy
            RebalancerConfig rebalancerConfig = resource.getRebalancerConfig();
            if (rebalancerConfig != null
                && rebalancerConfig.getStateModelDefId().stringify()
                    .equalsIgnoreCase(DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
              if (resource.getPartitionMap().size() > 0) {
                // TODO refactor it -- we need a way to read in scheduler tasks a priori
                Resource activeResource = cluster.getResource(resourceId);
                if (activeResource != null) {
                  message.getRecord().setMapField(Message.Attributes.INNER_MESSAGE.toString(),
                      activeResource.getSchedulerTaskConfig().getTaskConfig(partitionId));
                }
              }
            }

            // Set timeout of needed
            String stateTransition =
                currentState + "-" + nextState + "_" + Message.Attributes.TIMEOUT;
            if (resource.getSchedulerTaskConfig() != null) {
              Integer timeout =
                  resource.getSchedulerTaskConfig().getTimeout(stateTransition, partitionId);
              if (timeout != null && timeout > 0) {
                message.setExecutionTimeout(timeout);
              }
            }
            message.getRecord().setSimpleField("ClusterEventName", event.getName());

            if (!messageMap.containsKey(desiredState)) {
              messageMap.put(desiredState, new ArrayList<Message>());
            }
            messageMap.get(desiredState).add(message);
          }
        }

        // add generated messages to output according to state priority
        List<State> statesPriorityList = stateModelDef.getStatesPriorityList();
        for (State state : statesPriorityList) {
          if (messageMap.containsKey(state)) {
            for (Message message : messageMap.get(state)) {
              output.addMessage(resourceId, partitionId, message);
            }
          }
        }

      } // end of for-each-partition
    }
    event.addAttribute(AttributeName.MESSAGES_ALL.toString(), output);
    // System.out.println("output: " + output);
  }

  private Message createMessage(HelixManager manager, ResourceId resourceId,
      PartitionId partitionId, ParticipantId participantId, State currentState, State nextState,
      SessionId participantSessionId, StateModelDefId stateModelDefId,
      StateModelFactoryId stateModelFactoryId, int bucketSize) {
    MessageId uuid = Id.message(UUID.randomUUID().toString());
    Message message = new Message(MessageType.STATE_TRANSITION, uuid);
    message.setSrcName(manager.getInstanceName());
    message.setTgtName(participantId.stringify());
    message.setMsgState(MessageState.NEW);
    message.setPartitionId(partitionId);
    message.setResourceId(resourceId);
    message.setFromState(currentState);
    message.setToState(nextState);
    message.setTgtSessionId(participantSessionId);
    message.setSrcSessionId(Id.session(manager.getSessionId()));
    message.setStateModelDef(stateModelDefId);
    message.setStateModelFactoryName(stateModelFactoryId.stringify());
    message.setBucketSize(bucketSize);

    return message;
  }
}
