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
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.SchedulerTaskConfig;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.api.id.StateModelFactoryId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * Compares the currentState, pendingState with IdealState and generate messages
 */
public class MessageGenerationStage extends AbstractBaseStage {
  private static Logger LOG = Logger.getLogger(MessageGenerationStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager manager = event.getAttribute("helixmanager");
    Cluster cluster = event.getAttribute("Cluster");
    Map<StateModelDefId, StateModelDefinition> stateModelDefMap = cluster.getStateModelMap();
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    ResourceCurrentState currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    if (manager == null || cluster == null || resourceMap == null || currentStateOutput == null
        || bestPossibleStateOutput == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|Cluster|RESOURCES|CURRENT_STATE|BEST_POSSIBLE_STATE");
    }

    MessageOutput output = new MessageOutput();

    for (ResourceId resourceId : resourceMap.keySet()) {
      ResourceConfig resourceConfig = resourceMap.get(resourceId);
      int bucketSize = 0;
      bucketSize = resourceConfig.getIdealState().getBucketSize();

      IdealState idealState = resourceConfig.getIdealState();
      StateModelDefinition stateModelDef = stateModelDefMap.get(idealState.getStateModelDefId());

      ResourceAssignment resourceAssignment =
          bestPossibleStateOutput.getResourceAssignment(resourceId);
      for (PartitionId subUnitId : resourceAssignment.getMappedPartitionIds()) {
        Map<ParticipantId, State> instanceStateMap = resourceAssignment.getReplicaMap(subUnitId);

        // we should generate message based on the desired-state priority
        // so keep generated messages in a temp map keyed by state
        // desired-state->list of generated-messages
        Map<State, List<Message>> messageMap = new HashMap<State, List<Message>>();

        for (ParticipantId participantId : instanceStateMap.keySet()) {
          State desiredState = instanceStateMap.get(participantId);

          State currentState =
              currentStateOutput.getCurrentState(resourceId, subUnitId, participantId);
          if (currentState == null) {
            currentState = stateModelDef.getTypedInitialState();
          }

          if (desiredState.equals(currentState)) {
            continue;
          }

          State pendingState =
              currentStateOutput.getPendingState(resourceId, subUnitId, participantId);

          // TODO fix it
          State nextState = stateModelDef.getNextStateForTransition(currentState, desiredState);
          if (nextState == null) {
            LOG.error("Unable to find a next state for partition: " + subUnitId
                + " from stateModelDefinition" + stateModelDef.getClass() + " from:" + currentState
                + " to:" + desiredState);
            continue;
          }

          if (pendingState != null) {
            if (nextState.equals(pendingState)) {
              LOG.debug("Message already exists for " + participantId + " to transit " + subUnitId
                  + " from " + currentState + " to " + nextState);
            } else if (currentState.equals(pendingState)) {
              LOG.info("Message hasn't been removed for " + participantId + " to transit"
                  + subUnitId + " to " + pendingState + ", desiredState: " + desiredState);
            } else {
              LOG.info("IdealState changed before state transition completes for " + subUnitId
                  + " on " + participantId + ", pendingState: " + pendingState + ", currentState: "
                  + currentState + ", nextState: " + nextState);
            }
          } else {
            // TODO check if instance is alive
            SessionId sessionId =
                SessionId.from(cluster.getLiveParticipantMap().get(participantId).getLiveInstance()
                    .getSessionId());
            Message message =
                createMessage(manager, resourceId, subUnitId, participantId, currentState,
                    nextState, sessionId, StateModelDefId.from(stateModelDef.getId()),
                    idealState.getStateModelFactoryId(), bucketSize);

            // TODO refactor get/set timeout/inner-message
            if (idealState != null
                && idealState.getStateModelDefId().equalsIgnoreCase(
                    StateModelDefId.SchedulerTaskQueue)) {
              if (resourceConfig.getSubUnitSet().size() > 0) {
                // TODO refactor it -- we need a way to read in scheduler tasks a priori
                Message innerMsg =
                    resourceConfig.getSchedulerTaskConfig().getInnerMessage(subUnitId);
                if (innerMsg != null) {
                  message.setInnerMessage(innerMsg);
                }
              }
            }

            // Set timeout if needed
            String stateTransition =
                String.format("%s-%s_%s", currentState, nextState,
                    Message.Attributes.TIMEOUT.name());
            SchedulerTaskConfig schedulerTaskConfig = resourceConfig.getSchedulerTaskConfig();
            if (schedulerTaskConfig != null) {
              int timeout = schedulerTaskConfig.getTimeout(stateTransition, subUnitId);
              if (timeout > 0) {
                message.setExecutionTimeout(timeout);
              }
            }
            message.setClusterEvent(event);

            if (!messageMap.containsKey(desiredState)) {
              messageMap.put(desiredState, new ArrayList<Message>());
            }
            messageMap.get(desiredState).add(message);
          }
        }

        // add generated messages to output according to state priority
        List<State> statesPriorityList = stateModelDef.getTypedStatesPriorityList();
        for (State state : statesPriorityList) {
          if (messageMap.containsKey(state)) {
            for (Message message : messageMap.get(state)) {
              output.addMessage(resourceId, subUnitId, message);
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
    MessageId uuid = MessageId.from(UUID.randomUUID().toString());
    Message message = new Message(MessageType.STATE_TRANSITION, uuid);
    message.setSrcName(manager.getInstanceName());
    message.setTgtName(participantId.stringify());
    message.setMsgState(MessageState.NEW);
    message.setPartitionId(partitionId);
    message.setResourceId(resourceId);
    message.setFromState(currentState);
    message.setToState(nextState);
    message.setTgtSessionId(participantSessionId);
    message.setSrcSessionId(SessionId.from(manager.getSessionId()));
    message.setStateModelDef(stateModelDefId);
    message.setStateModelFactoryId(stateModelFactoryId);
    message.setBucketSize(bucketSize);

    return message;
  }
}
