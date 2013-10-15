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

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Message;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestCurrentStateComputationStage extends BaseStageTest {

  @Test
  public void testEmptyCS() {
    String[] resources = new String[] {
      "testResourceName"
    };
    List<IdealState> idealStates = setupIdealState(5, resources, 10, 1, RebalanceMode.SEMI_AUTO);
    Map<ResourceId, ResourceConfig> resourceMap = getResourceMap(idealStates);
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    CurrentStateComputationStage stage = new CurrentStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    ResourceCurrentState output = event.getAttribute(AttributeName.CURRENT_STATE.toString());
    AssertJUnit.assertEquals(
        output.getCurrentStateMap(ResourceId.from("testResourceName"),
            PartitionId.from("testResourceName_0")).size(), 0);
  }

  @Test
  public void testSimpleCS() {
    // setup resource
    String[] resources = new String[] {
      "testResourceName"
    };
    List<IdealState> idealStates = setupIdealState(5, resources, 10, 1, RebalanceMode.SEMI_AUTO);
    Map<ResourceId, ResourceConfig> resourceMap = getResourceMap(idealStates);

    setupLiveInstances(5);

    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    CurrentStateComputationStage stage = new CurrentStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    ResourceCurrentState output1 = event.getAttribute(AttributeName.CURRENT_STATE.toString());
    AssertJUnit.assertEquals(
        output1.getCurrentStateMap(ResourceId.from("testResourceName"),
            PartitionId.from("testResourceName_0")).size(), 0);

    // Add a state transition messages
    Message message = new Message(Message.MessageType.STATE_TRANSITION, MessageId.from("msg1"));
    message.setFromState(State.from("OFFLINE"));
    message.setToState(State.from("SLAVE"));
    message.setResourceId(ResourceId.from("testResourceName"));
    message.setPartitionId(PartitionId.from("testResourceName_1"));
    message.setTgtName("localhost_3");
    message.setTgtSessionId(SessionId.from("session_3"));

    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.message("localhost_" + 3, message.getId()), message);

    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    ResourceCurrentState output2 = event.getAttribute(AttributeName.CURRENT_STATE.toString());
    State pendingState =
        output2.getPendingState(ResourceId.from("testResourceName"),
            PartitionId.from("testResourceName_1"), ParticipantId.from("localhost_3"));
    AssertJUnit.assertEquals(pendingState, State.from("SLAVE"));

    ZNRecord record1 = new ZNRecord("testResourceName");
    // Add a current state that matches sessionId and one that does not match
    CurrentState stateWithLiveSession = new CurrentState(record1);
    stateWithLiveSession.setSessionId(SessionId.from("session_3"));
    stateWithLiveSession.setStateModelDefRef("MasterSlave");
    stateWithLiveSession.setState(PartitionId.from("testResourceName_1"), State.from("OFFLINE"));
    ZNRecord record2 = new ZNRecord("testResourceName");
    CurrentState stateWithDeadSession = new CurrentState(record2);
    stateWithDeadSession.setSessionId(SessionId.from("session_dead"));
    stateWithDeadSession.setStateModelDefRef("MasterSlave");
    stateWithDeadSession.setState(PartitionId.from("testResourceName_1"), State.from("MASTER"));

    accessor.setProperty(keyBuilder.currentState("localhost_3", "session_3", "testResourceName"),
        stateWithLiveSession);
    accessor.setProperty(
        keyBuilder.currentState("localhost_3", "session_dead", "testResourceName"),
        stateWithDeadSession);
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    ResourceCurrentState output3 = event.getAttribute(AttributeName.CURRENT_STATE.toString());
    State currentState =
        output3.getCurrentState(ResourceId.from("testResourceName"),
            PartitionId.from("testResourceName_1"), ParticipantId.from("localhost_3"));
    AssertJUnit.assertEquals(currentState, State.from("OFFLINE"));

  }

}
