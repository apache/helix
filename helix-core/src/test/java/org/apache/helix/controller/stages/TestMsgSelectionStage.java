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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.UUID;

import org.apache.helix.TestHelper;
import org.apache.helix.controller.stages.MessageSelectionStage.Bounds;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMsgSelectionStage {
  private Message newMessage(String resourceName, String partitionName, String instanceName,
      String fromState, String toState) {
    String uuid = UUID.randomUUID().toString();
    Message message = new Message(MessageType.STATE_TRANSITION, uuid);
    message.setSrcName("controller");
    message.setTgtName(instanceName);
    message.setMsgState(MessageState.NEW);
    message.setResourceName(resourceName);
    message.setPartitionName(partitionName);
    message.setFromState(fromState);
    message.setToState(toState);
    message.setTgtSessionId("sessionId");
    message.setSrcSessionId("sessionId");
    message.setStateModelDef("MasterSlave");
    message.setStateModelFactoryName("DEFAULT");
    message.setBucketSize(0);
    return message;
  }

  @Test
  public void testMasterXfer() {
    System.out.println("START testMasterXfer at " + new Date(System.currentTimeMillis()));

    Map<String, LiveInstance> liveInstances = new HashMap<String, LiveInstance>();
    liveInstances.put("localhost_0", new LiveInstance("localhost_0"));
    liveInstances.put("localhost_1", new LiveInstance("localhost_1"));

    Map<String, String> currentStates = new HashMap<String, String>();
    currentStates.put("localhost_0", "SLAVE");
    currentStates.put("localhost_1", "MASTER");

    Map<String, Message> pendingMessages = new HashMap<String, Message>();

    List<Message> messages = new ArrayList<Message>();
    messages.add(TestHelper.createMessage("msgId_0", "SLAVE", "MASTER", "localhost_0", "TestDB",
        "TestDB_0"));
    messages.add(TestHelper.createMessage("msgId_1", "MASTER", "SLAVE", "localhost_1", "TestDB",
        "TestDB_0"));

    Map<String, Bounds> stateConstraints = new HashMap<String, Bounds>();
    stateConstraints.put("MASTER", new Bounds(0, 1));
    stateConstraints.put("SLAVE", new Bounds(0, 2));

    Map<String, Integer> stateTransitionPriorities = new HashMap<String, Integer>();
    stateTransitionPriorities.put("MASTER-SLAVE", 0);
    stateTransitionPriorities.put("SLAVE-MASTER", 1);

    List<Message> selectedMsg =
        new MessageSelectionStage().selectMessages(liveInstances, currentStates, pendingMessages,
            messages, Collections.<Message>emptyList(), stateConstraints, stateTransitionPriorities,
            BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition(), false);

    Assert.assertEquals(selectedMsg.size(), 1);
    Assert.assertEquals(selectedMsg.get(0).getMsgId(), "msgId_1");
    System.out.println("END testMasterXfer at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMasterXferAfterMasterResume() {
    System.out.println("START testMasterXferAfterMasterResume at "
        + new Date(System.currentTimeMillis()));

    Map<String, LiveInstance> liveInstances = new HashMap<String, LiveInstance>();
    liveInstances.put("localhost_0", new LiveInstance("localhost_0"));
    liveInstances.put("localhost_1", new LiveInstance("localhost_1"));

    Map<String, String> currentStates = new HashMap<String, String>();
    currentStates.put("localhost_0", "SLAVE");
    currentStates.put("localhost_1", "SLAVE");

    Map<String, Message> pendingMessages = new HashMap<String, Message>();
    pendingMessages.put("localhost_1", newMessage("TestDB", "TestDB_0", "localhost_1", "SLAVE", "MASTER"));

    List<Message> messages = new ArrayList<Message>();
    messages.add(TestHelper.createMessage("msgId_0", "SLAVE", "MASTER", "localhost_0", "TestDB",
        "TestDB_0"));

    Map<String, Bounds> stateConstraints = new HashMap<String, Bounds>();
    stateConstraints.put("MASTER", new Bounds(0, 1));
    stateConstraints.put("SLAVE", new Bounds(0, 2));

    Map<String, Integer> stateTransitionPriorities = new HashMap<String, Integer>();
    stateTransitionPriorities.put("MASTER-SLAVE", 0);
    stateTransitionPriorities.put("SLAVE-MASTER", 1);

    List<Message> selectedMsg =
        new MessageSelectionStage().selectMessages(liveInstances, currentStates, pendingMessages,
            messages, Collections.<Message>emptyList(), stateConstraints, stateTransitionPriorities,
            BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition(), false);

    Assert.assertEquals(selectedMsg.size(), 0);
    System.out.println("END testMasterXferAfterMasterResume at "
        + new Date(System.currentTimeMillis()));
  }
}
