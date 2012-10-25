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

import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestCurrentStateComputationStage extends BaseStageTest
{

  @Test
  public void testEmptyCS()
  {
    Map<String, Resource> resourceMap = getResourceMap();
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    CurrentStateComputationStage stage = new CurrentStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    AssertJUnit.assertEquals(output.getCurrentStateMap("testResourceName",
                                                       new Partition("testResourceName_0"))
                                   .size(),
                             0);
  }

  @Test
  public void testSimpleCS()
  {
    // setup resource
    Map<String, Resource> resourceMap = getResourceMap();

    setupLiveInstances(5);

    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    CurrentStateComputationStage stage = new CurrentStateComputationStage();
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output1 =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    AssertJUnit.assertEquals(output1.getCurrentStateMap("testResourceName",
                                                        new Partition("testResourceName_0"))
                                    .size(),
                             0);

    // Add a state transition messages
    Message message = new Message(Message.MessageType.STATE_TRANSITION, "msg1");
    message.setFromState("OFFLINE");
    message.setToState("SLAVE");
    message.setResourceName("testResourceName");
    message.setPartitionName("testResourceName_1");
    message.setTgtName("localhost_3");
    message.setTgtSessionId("session_3");

    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.message("localhost_" + 3, message.getId()), message);

    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output2 =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    String pendingState =
        output2.getPendingState("testResourceName",
                                new Partition("testResourceName_1"),
                                "localhost_3");
    AssertJUnit.assertEquals(pendingState, "SLAVE");

    ZNRecord record1 = new ZNRecord("testResourceName");
    // Add a current state that matches sessionId and one that does not match
    CurrentState stateWithLiveSession = new CurrentState(record1);
    stateWithLiveSession.setSessionId("session_3");
    stateWithLiveSession.setStateModelDefRef("MasterSlave");
    stateWithLiveSession.setState("testResourceName_1", "OFFLINE");
    ZNRecord record2 = new ZNRecord("testResourceName");
    CurrentState stateWithDeadSession = new CurrentState(record2);
    stateWithDeadSession.setSessionId("session_dead");
    stateWithDeadSession.setStateModelDefRef("MasterSlave");
    stateWithDeadSession.setState("testResourceName_1", "MASTER");

    accessor.setProperty(keyBuilder.currentState("localhost_3",
                                                 "session_3",
                                                 "testResourceName"),
                         stateWithLiveSession);
    accessor.setProperty(keyBuilder.currentState("localhost_3",
                                                 "session_dead",
                                                 "testResourceName"),
                         stateWithDeadSession);
    runStage(event, new ReadClusterDataStage());
    runStage(event, stage);
    CurrentStateOutput output3 =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    String currentState =
        output3.getCurrentState("testResourceName",
                                new Partition("testResourceName_1"),
                                "localhost_3");
    AssertJUnit.assertEquals(currentState, "OFFLINE");

  }

}
