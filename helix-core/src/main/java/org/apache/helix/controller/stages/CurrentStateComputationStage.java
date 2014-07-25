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

import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;

/**
 * For each LiveInstances select currentState and message whose sessionId matches
 * sessionId from LiveInstance Get Partition,State for all the resources computed in
 * previous State [ResourceComputationStage]
 */
public class CurrentStateComputationStage extends AbstractBaseStage {
  @Override
  public void process(ClusterEvent event) throws Exception {
    Cluster cluster = event.getAttribute("Cluster");
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());

    if (cluster == null || resourceMap == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires Cluster|RESOURCE");
    }

    ResourceCurrentState currentStateOutput = new ResourceCurrentState();

    for (Participant liveParticipant : cluster.getLiveParticipantMap().values()) {
      ParticipantId participantId = liveParticipant.getId();

      // add pending messages
      Map<MessageId, Message> instanceMsgs = liveParticipant.getMessageMap();
      for (Message message : instanceMsgs.values()) {
        if (!MessageType.STATE_TRANSITION.toString().equalsIgnoreCase(message.getMsgType())) {
          continue;
        }

        if (!liveParticipant.getLiveInstance().getSessionId().equals(message.getTgtSessionId())) {
          continue;
        }

        ResourceId resourceId = message.getResourceId();
        ResourceConfig resource = resourceMap.get(resourceId);
        if (resource == null) {
          continue;
        }
        IdealState idealState = resource.getIdealState();

        if (!message.getBatchMessageMode()) {
          PartitionId partitionId = message.getPartitionId();
          if (idealState.getPartitionIdSet().contains(partitionId)) {
            currentStateOutput.setPendingState(resourceId, partitionId, participantId,
                message.getTypedToState());
          } else {
            // log
          }
        } else {
          List<PartitionId> partitionNames = message.getPartitionIds();
          if (!partitionNames.isEmpty()) {
            for (PartitionId partitionId : partitionNames) {
              if (idealState.getPartitionIdSet().contains(partitionId)) {
                currentStateOutput.setPendingState(resourceId, partitionId, participantId,
                    message.getTypedToState());
              } else {
                // log
              }
            }
          }
        }
      }

      // add current state
      SessionId sessionId = SessionId.from(liveParticipant.getLiveInstance().getSessionId());
      Map<ResourceId, CurrentState> curStateMap = liveParticipant.getCurrentStateMap();
      for (CurrentState curState : curStateMap.values()) {
        if (!sessionId.equals(curState.getTypedSessionId())) {
          continue;
        }

        ResourceId resourceId = curState.getResourceId();
        StateModelDefId stateModelDefId = curState.getStateModelDefId();
        ResourceConfig resource = resourceMap.get(resourceId);
        if (resource == null) {
          continue;
        }

        if (stateModelDefId != null) {
          currentStateOutput.setResourceStateModelDef(resourceId, stateModelDefId);
        }

        currentStateOutput.setBucketSize(resourceId, curState.getBucketSize());

        Map<PartitionId, State> partitionStateMap = curState.getTypedPartitionStateMap();
        for (PartitionId partitionId : partitionStateMap.keySet()) {
          currentStateOutput.setCurrentState(resourceId, partitionId, participantId,
              curState.getState(partitionId));

          currentStateOutput.setRequestedState(resourceId, partitionId, participantId,
              curState.getRequestedState(partitionId));

          currentStateOutput.setInfo(resourceId, partitionId, participantId,
              curState.getInfo(partitionId));
        }
      }
    }
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);
  }
}
