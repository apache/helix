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

import java.util.Map;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Id;
import org.apache.helix.api.ResourceConfig;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.State;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestCurrentStateComputationStage extends BaseStageTest {

  @Test
  public void testEmptyCS() {
    Map<ResourceId, ResourceConfig> resourceMap = getResourceMap();
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    NewCurrentStateComputationStage stage = new NewCurrentStateComputationStage();
    runStage(event, new NewReadClusterDataStage());
    runStage(event, stage);
    NewCurrentStateOutput output = event.getAttribute(AttributeName.CURRENT_STATE.toString());
    AssertJUnit.assertEquals(
        output.getCurrentStateMap(Id.resource("testResourceName"),
            Id.partition("testResourceName_0")).size(), 0);
  }

  @Test
  public void testSimpleCS() {
    // setup resource
    Map<ResourceId, ResourceConfig> resourceMap = getResourceMap();

    setupLiveInstances(5);

    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    NewCurrentStateComputationStage stage = new NewCurrentStateComputationStage();
    runStage(event, new NewReadClusterDataStage());
    runStage(event, stage);
    NewCurrentStateOutput output1 = event.getAttribute(AttributeName.CURRENT_STATE.toString());
    AssertJUnit.assertEquals(
        output1.getCurrentStateMap(Id.resource("testResourceName"),
            Id.partition("testResourceName_0")).size(), 0);

    // Add a state transition messages
    Message message = new Message(Message.MessageType.STATE_TRANSITION, Id.message("msg1"));
    message.setFromState(State.from("OFFLINE"));
    message.setToState(State.from("SLAVE"));
    message.setResourceId(Id.resource("testResourceName"));
    message.setPartitionId(Id.partition("testResourceName_1"));
    message.setTgtName("localhost_3");
    message.setTgtSessionId(Id.session("session_3"));

    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.message("localhost_" + 3, message.getId()), message);

    runStage(event, new NewReadClusterDataStage());
    runStage(event, stage);
    NewCurrentStateOutput output2 = event.getAttribute(AttributeName.CURRENT_STATE.toString());
    State pendingState =
        output2.getPendingState(Id.resource("testResourceName"),
            Id.partition("testResourceName_1"), Id.participant("localhost_3"));
    AssertJUnit.assertEquals(pendingState, State.from("SLAVE"));

    ZNRecord record1 = new ZNRecord("testResourceName");
    // Add a current state that matches sessionId and one that does not match
    CurrentState stateWithLiveSession = new CurrentState(record1);
    stateWithLiveSession.setSessionId(Id.session("session_3"));
    stateWithLiveSession.setStateModelDefRef("MasterSlave");
    stateWithLiveSession.setState(Id.partition("testResourceName_1"), State.from("OFFLINE"));
    ZNRecord record2 = new ZNRecord("testResourceName");
    CurrentState stateWithDeadSession = new CurrentState(record2);
    stateWithDeadSession.setSessionId(Id.session("session_dead"));
    stateWithDeadSession.setStateModelDefRef("MasterSlave");
    stateWithDeadSession.setState(Id.partition("testResourceName_1"), State.from("MASTER"));

    accessor.setProperty(keyBuilder.currentState("localhost_3", "session_3", "testResourceName"),
        stateWithLiveSession);
    accessor.setProperty(
        keyBuilder.currentState("localhost_3", "session_dead", "testResourceName"),
        stateWithDeadSession);
    runStage(event, new NewReadClusterDataStage());
    runStage(event, stage);
    NewCurrentStateOutput output3 = event.getAttribute(AttributeName.CURRENT_STATE.toString());
    State currentState =
        output3.getCurrentState(Id.resource("testResourceName"),
            Id.partition("testResourceName_1"), Id.participant("localhost_3"));
    AssertJUnit.assertEquals(currentState, State.from("OFFLINE"));

  }

}
