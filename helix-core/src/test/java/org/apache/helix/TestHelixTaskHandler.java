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

import java.util.Date;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.Mocks.MockManager;
import org.apache.helix.Mocks.MockStateModel;
import org.apache.helix.Mocks.MockStateModelAnnotated;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.messaging.handling.HelixStateTransitionHandler;
import org.apache.helix.messaging.handling.HelixTask;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestHelixTaskHandler
{
  @Test()
  public void testInvocation() throws Exception
  {
    HelixTaskExecutor executor = new HelixTaskExecutor();
    System.out.println("START TestCMTaskHandler.testInvocation()");
    Message message = new Message(MessageType.STATE_TRANSITION, "Some unique id");

    message.setSrcName("cm-instance-0");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setPartitionName("TestDB_0");
    message.setMsgId("Some unique message id");
    message.setResourceName("TestDB");
    message.setTgtName("localhost");
    message.setStateModelDef("MasterSlave");
    message.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
    MockStateModel stateModel = new MockStateModel();
    NotificationContext context;
    MockManager manager = new MockManager("clusterName");
//    DataAccessor accessor = manager.getDataAccessor();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    StateModelDefinition stateModelDef = new StateModelDefinition(
        generator.generateConfigForMasterSlave());
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef("MasterSlave"), stateModelDef);

    context = new NotificationContext(manager);
    CurrentState currentStateDelta = new CurrentState("TestDB");
    currentStateDelta.setState("TestDB_0", "OFFLINE");

    HelixStateTransitionHandler stHandler = new HelixStateTransitionHandler(stateModel, message,
        context, currentStateDelta, executor);
    HelixTask handler;
    handler = new HelixTask(message, context, stHandler, executor);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskHandler.testInvocation() at "
        + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testInvocationAnnotated() throws Exception
  {
    System.out.println("START TestCMTaskHandler.testInvocationAnnotated() at "
        + new Date(System.currentTimeMillis()));
    HelixTaskExecutor executor = new HelixTaskExecutor();
    Message message = new Message(MessageType.STATE_TRANSITION, "Some unique id");
    message.setSrcName("cm-instance-0");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setPartitionName("TestDB_0");
    message.setMsgId("Some unique message id");
    message.setResourceName("TestDB");
    message.setTgtName("localhost");
    message.setStateModelDef("MasterSlave");
    message.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
    MockStateModelAnnotated stateModel = new MockStateModelAnnotated();
    NotificationContext context;

    MockManager manager = new MockManager("clusterName");
//    DataAccessor accessor = manager.getDataAccessor();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    StateModelDefinition stateModelDef = new StateModelDefinition(
        generator.generateConfigForMasterSlave());
//    accessor.setProperty(PropertyType.STATEMODELDEFS, stateModelDef, "MasterSlave");
    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef("MasterSlave"), stateModelDef);


    context = new NotificationContext(manager);
    
    CurrentState currentStateDelta = new CurrentState("TestDB");
    currentStateDelta.setState("TestDB_0", "OFFLINE");

    HelixStateTransitionHandler stHandler = new HelixStateTransitionHandler(stateModel, message,
        context, currentStateDelta, executor);

    HelixTask handler = new HelixTask(message, context, stHandler, executor);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskHandler.testInvocationAnnotated() at "
        + new Date(System.currentTimeMillis()));
  }

}
