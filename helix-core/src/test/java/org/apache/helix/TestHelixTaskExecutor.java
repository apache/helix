package org.apache.helix;

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

import org.apache.helix.Mocks.MockHelixTaskExecutor;
import org.apache.helix.Mocks.MockManager;
import org.apache.helix.Mocks.MockStateModel;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.State;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.messaging.handling.AsyncCallbackService;
import org.apache.helix.messaging.handling.HelixStateTransitionHandler;
import org.apache.helix.messaging.handling.HelixTask;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestHelixTaskExecutor {

  @Test()
  public void testCMTaskExecutor() throws Exception {
    System.out.println("START TestCMTaskExecutor");
    MessageId msgId = MessageId.from("TestMessageId");
    Message message = new Message(MessageType.TASK_REPLY, msgId);

    message.setMessageId(msgId);
    message.setSrcName("cm-instance-0");
    message.setTgtName("cm-instance-1");
    message.setTgtSessionId(SessionId.from("1234"));
    message.setFromState(State.from("Offline"));
    message.setToState(State.from("Slave"));
    message.setPartitionId(PartitionId.from("TestDB_0"));
    message.setResourceId(ResourceId.from("TestDB"));
    message.setStateModelDef(StateModelDefId.from("MasterSlave"));

    MockManager manager = new MockManager("clusterName");
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef("MasterSlave"), stateModelDef);

    MockHelixTaskExecutor executor = new MockHelixTaskExecutor();
    MockStateModel stateModel = new MockStateModel();
    executor.registerMessageHandlerFactory(MessageType.TASK_REPLY.toString(),
        new AsyncCallbackService());

    NotificationContext context = new NotificationContext(manager);
    CurrentState currentStateDelta = new CurrentState("TestDB");
    currentStateDelta.setState(PartitionId.from("TestDB_0"), State.from("OFFLINE"));

    StateTransitionHandlerFactory<MockStateModel> stateModelFactory = new StateTransitionHandlerFactory<MockStateModel>() {

      @Override
      public MockStateModel createStateTransitionHandler(ResourceId resource, PartitionId partition) {
        // TODO Auto-generated method stub
        return new MockStateModel();
      }

    };
    HelixStateTransitionHandler handler =
        new HelixStateTransitionHandler(stateModelFactory, stateModel, message, context,
            currentStateDelta);

    HelixTask task = new HelixTask(message, context, handler, executor);
    executor.scheduleTask(task);
    for (int i = 0; i < 10; i++) {
      if (!executor.isDone(task.getTaskId())) {
        Thread.sleep(500);
      }
    }
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskExecutor");
  }

}
