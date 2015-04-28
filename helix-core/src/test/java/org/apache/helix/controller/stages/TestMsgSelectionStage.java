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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.TestHelper;
import org.apache.helix.api.Participant;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.stages.MessageSelectionStage.Bounds;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMsgSelectionStage {
  @Test
  public void testMasterXfer() {
    System.out.println("START testMasterXfer at " + new Date(System.currentTimeMillis()));

    Map<ParticipantId, Participant> liveParticipants = new HashMap<ParticipantId, Participant>();
    Map<ResourceId, CurrentState> currentStateMap = Collections.emptyMap();
    Map<MessageId, Message> messageMap = Collections.emptyMap();
    ParticipantId participantId0 = ParticipantId.from("localhost_0");
    ParticipantId participantId1 = ParticipantId.from("localhost_1");
    LiveInstance[] liveInstances = new LiveInstance[2];
    for (int i = 0; i < 2; i++) {
      liveInstances[i] = new LiveInstance("localhost_" + i);
      liveInstances[i].setSessionId("session_" + i);
      liveInstances[i].setHelixVersion("1.2.3.4");
      liveInstances[i].setLiveInstance(i + "@localhost");
    }
    liveParticipants.put(participantId0, new Participant(new ParticipantConfig.Builder(
        participantId0).hostName("localhost").port(0).build(), liveInstances[0], currentStateMap,
        messageMap, null));
    liveParticipants.put(participantId1, new Participant(new ParticipantConfig.Builder(
        participantId1).hostName("localhost").port(1).build(), liveInstances[1], currentStateMap,
        messageMap, null));

    Map<ParticipantId, State> currentStates = new HashMap<ParticipantId, State>();
    currentStates.put(ParticipantId.from("localhost_0"), State.from("SLAVE"));
    currentStates.put(ParticipantId.from("localhost_1"), State.from("MASTER"));

    Map<ParticipantId, State> pendingStates = new HashMap<ParticipantId, State>();

    List<Message> messages = new ArrayList<Message>();
    messages.add(TestHelper.createMessage(MessageId.from("msgId_0"), "SLAVE", "MASTER",
        "localhost_0", "TestDB", "TestDB_0"));
    messages.add(TestHelper.createMessage(MessageId.from("msgId_1"), "MASTER", "SLAVE",
        "localhost_1", "TestDB", "TestDB_0"));

    Map<State, Bounds> stateConstraints = new HashMap<State, Bounds>();
    stateConstraints.put(State.from("MASTER"), new Bounds(0, 1));
    stateConstraints.put(State.from("SLAVE"), new Bounds(0, 2));

    Map<String, Integer> stateTransitionPriorities = new HashMap<String, Integer>();
    stateTransitionPriorities.put("MASTER-SLAVE", 0);
    stateTransitionPriorities.put("SLAVE-MASTER", 1);

    List<Message> selectedMsg =
        new MessageSelectionStage().selectMessages(liveParticipants, currentStates, pendingStates,
            messages, stateConstraints, stateTransitionPriorities, State.from("OFFLINE"));

    Assert.assertEquals(selectedMsg.size(), 1);
    Assert.assertEquals(selectedMsg.get(0).getMessageId(), MessageId.from("msgId_1"));
    System.out.println("END testMasterXfer at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMasterXferAfterMasterResume() {
    System.out.println("START testMasterXferAfterMasterResume at "
        + new Date(System.currentTimeMillis()));

    Map<ParticipantId, Participant> liveParticipants = new HashMap<ParticipantId, Participant>();
    Map<ResourceId, CurrentState> currentStateMap = Collections.emptyMap();
    Map<MessageId, Message> messageMap = Collections.emptyMap();
    ParticipantId participantId0 = ParticipantId.from("localhost_0");
    ParticipantId participantId1 = ParticipantId.from("localhost_1");
    LiveInstance[] liveInstances = new LiveInstance[2];
    for (int i = 0; i < 2; i++) {
      liveInstances[i] = new LiveInstance("localhost_" + i);
      liveInstances[i].setSessionId("session_" + i);
      liveInstances[i].setHelixVersion("1.2.3.4");
      liveInstances[i].setLiveInstance(i + "@localhost");
    }
    liveParticipants.put(participantId0, new Participant(new ParticipantConfig.Builder(
        participantId0).hostName("localhost").port(0).build(), liveInstances[0], currentStateMap,
        messageMap, null));
    liveParticipants.put(participantId1, new Participant(new ParticipantConfig.Builder(
        participantId1).hostName("localhost").port(1).build(), liveInstances[1], currentStateMap,
        messageMap, null));

    Map<ParticipantId, State> currentStates = new HashMap<ParticipantId, State>();
    currentStates.put(ParticipantId.from("localhost_0"), State.from("SLAVE"));
    currentStates.put(ParticipantId.from("localhost_1"), State.from("SLAVE"));

    Map<ParticipantId, State> pendingStates = new HashMap<ParticipantId, State>();
    pendingStates.put(ParticipantId.from("localhost_1"), State.from("MASTER"));

    List<Message> messages = new ArrayList<Message>();
    messages.add(TestHelper.createMessage(MessageId.from("msgId_0"), "SLAVE", "MASTER",
        "localhost_0", "TestDB", "TestDB_0"));

    Map<State, Bounds> stateConstraints = new HashMap<State, Bounds>();
    stateConstraints.put(State.from("MASTER"), new Bounds(0, 1));
    stateConstraints.put(State.from("SLAVE"), new Bounds(0, 2));

    Map<String, Integer> stateTransitionPriorities = new HashMap<String, Integer>();
    stateTransitionPriorities.put("MASTER-SLAVE", 0);
    stateTransitionPriorities.put("SLAVE-MASTER", 1);

    List<Message> selectedMsg =
        new MessageSelectionStage().selectMessages(liveParticipants, currentStates, pendingStates,
            messages, stateConstraints, stateTransitionPriorities, State.from("OFFLINE"));

    Assert.assertEquals(selectedMsg.size(), 0);
    System.out.println("END testMasterXferAfterMasterResume at "
        + new Date(System.currentTimeMillis()));
  }
}
