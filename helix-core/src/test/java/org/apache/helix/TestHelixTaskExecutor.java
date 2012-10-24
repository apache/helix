/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.Mocks.MockHelixTaskExecutor;
import org.apache.helix.Mocks.MockManager;
import org.apache.helix.Mocks.MockStateModel;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.messaging.handling.AsyncCallbackService;
import org.apache.helix.messaging.handling.HelixStateTransitionHandler;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestHelixTaskExecutor
{

  @Test()
  public void testCMTaskExecutor() throws Exception
  {
    System.out.println("START TestCMTaskExecutor");
    String msgId = "TestMessageId";
    Message message = new Message(MessageType.TASK_REPLY, msgId);

    message.setMsgId(msgId);
    message.setSrcName("cm-instance-0");
    message.setTgtName("cm-instance-1");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setPartitionName("TestDB_0");
    message.setResourceName("TestDB");
    message.setStateModelDef("MasterSlave");

    MockManager manager = new MockManager("clusterName");
    // DataAccessor accessor = manager.getDataAccessor();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    StateModelDefinition stateModelDef =
        new StateModelDefinition(generator.generateConfigForMasterSlave());
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef("MasterSlave"), stateModelDef);

    MockHelixTaskExecutor executor = new MockHelixTaskExecutor();
    MockStateModel stateModel = new MockStateModel();
    NotificationContext context;
    executor.registerMessageHandlerFactory(MessageType.TASK_REPLY.toString(),
                                           new AsyncCallbackService());
    // String clusterName =" testcluster";
    context = new NotificationContext(manager);
    CurrentState currentStateDelta = new CurrentState("TestDB");
    currentStateDelta.setState("TestDB_0", "OFFLINE");
    HelixStateTransitionHandler handler =
        new HelixStateTransitionHandler(stateModel,
                                        message,
                                        context,
                                        currentStateDelta,
                                        executor);

    executor.scheduleTask(message, handler, context);
    while (!executor.isDone(msgId + "/" + message.getPartitionName()))
    {
      Thread.sleep(500);
    }
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskExecutor");
  }

}
