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
import org.apache.helix.api.MessageId;
import org.apache.helix.api.Participant;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.Partition;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.ResourceConfig;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.SessionId;
import org.apache.helix.api.State;
import org.apache.helix.api.StateModelDefId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;

/**
 * For each LiveInstances select currentState and message whose sessionId matches
 * sessionId from LiveInstance Get Partition,State for all the resources computed in
 * previous State [ResourceComputationStage]
 */
public class NewCurrentStateComputationStage extends AbstractBaseStage {
  @Override
  public void process(ClusterEvent event) throws Exception {
    Cluster cluster = event.getAttribute("ClusterDataCache");
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());

    if (cluster == null || resourceMap == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires DataCache|RESOURCE");
    }

    NewCurrentStateOutput currentStateOutput = new NewCurrentStateOutput();

    for (Participant liveParticipant : cluster.getLiveParticipantMap().values()) {
      ParticipantId participantId = liveParticipant.getId();

      // add pending messages
      Map<MessageId, Message> instanceMsgs = liveParticipant.getMessageMap();
      for (Message message : instanceMsgs.values()) {
        if (!MessageType.STATE_TRANSITION.toString().equalsIgnoreCase(message.getMsgType())) {
          continue;
        }

        if (!liveParticipant.getRunningInstance().getSessionId().equals(message.getTgtSessionId())) {
          continue;
        }

        ResourceId resourceId = message.getResourceId();
        ResourceConfig resource = resourceMap.get(resourceId);
        if (resource == null) {
          continue;
        }

        if (!message.getBatchMessageMode()) {
          PartitionId partitionId = message.getPartitionId();
          Partition partition = resource.getPartition(partitionId);
          if (partition != null) {
            currentStateOutput.setPendingState(resourceId, partitionId, participantId,
                message.getToState());
          } else {
            // log
          }
        } else {
          List<PartitionId> partitionNames = message.getPartitionIds();
          if (!partitionNames.isEmpty()) {
            for (PartitionId partitionId : partitionNames) {
              Partition partition = resource.getPartition(partitionId);
              if (partition != null) {
                currentStateOutput.setPendingState(resourceId, partitionId, participantId,
                    message.getToState());
              } else {
                // log
              }
            }
          }
        }
      }

      // add current state
      SessionId sessionId = liveParticipant.getRunningInstance().getSessionId();
      Map<ResourceId, CurrentState> curStateMap = liveParticipant.getCurrentStateMap();
      for (CurrentState curState : curStateMap.values()) {
        if (!sessionId.equals(curState.getSessionId())) {
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

        Map<PartitionId, State> partitionStateMap = curState.getPartitionStateMap();
        for (PartitionId partitionId : partitionStateMap.keySet()) {
          Partition partition = resource.getPartition(partitionId);
          if (partition != null) {
            currentStateOutput.setCurrentState(resourceId, partitionId, participantId,
                curState.getState(partitionId));

          } else {
            // log
          }
        }
      }
    }

    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);
  }
}
