package com.linkedin.helix;

import java.util.Date;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.Mocks.MockManager;
import com.linkedin.helix.Mocks.MockStateModel;
import com.linkedin.helix.Mocks.MockStateModelAnnotated;
import com.linkedin.helix.messaging.handling.CMStateTransitionHandler;
import com.linkedin.helix.messaging.handling.CMTask;
import com.linkedin.helix.messaging.handling.CMTaskExecutor;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.tools.StateModelConfigGenerator;

public class TestCMTaskHandler
{
  @Test()
  public void testInvocation() throws Exception
  {
    CMTaskExecutor executor = new CMTaskExecutor();
    System.out.println("START TestCMTaskHandler.testInvocation()");
    Message message = new Message(MessageType.STATE_TRANSITION,"Some unique id");

    message.setSrcName("cm-instance-0");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setStateUnitKey("Teststateunitkey");
    message.setMsgId("Some unique message id");
    message.setStateUnitGroup("TeststateunitGroup");
    message.setTgtName("localhost");
    message.setStateModelDef("MasterSlave");
    MockStateModel stateModel = new MockStateModel();
    NotificationContext context;
    MockManager manager = new MockManager("clusterName");
    DataAccessor accessor = manager.getDataAccessor();
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    StateModelDefinition stateModelDef = new StateModelDefinition(generator.generateConfigForMasterSlave());
    accessor.setProperty(PropertyType.STATEMODELDEFS, stateModelDef, "MasterSlave");

    context = new NotificationContext(manager);
    CMStateTransitionHandler stHandler = new CMStateTransitionHandler(stateModel, message, context);
    CMTask handler;
    handler = new CMTask(message, context, stHandler, executor);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskHandler.testInvocation() at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testInvocationAnnotated() throws Exception
  {
    System.out.println("START TestCMTaskHandler.testInvocationAnnotated() at " + new Date(System.currentTimeMillis()));
    CMTaskExecutor executor = new CMTaskExecutor();
    Message message = new Message(MessageType.STATE_TRANSITION, "Some unique id");
    message.setSrcName("cm-instance-0");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setStateUnitKey("Teststateunitkey");
    message.setMsgId("Some unique message id");
    message.setStateUnitGroup("TeststateunitGroup");
    message.setTgtName("localhost");
    message.setStateModelDef("MasterSlave");
    MockStateModelAnnotated stateModel = new MockStateModelAnnotated();
    NotificationContext context;

    MockManager manager = new MockManager("clusterName");
    DataAccessor accessor = manager.getDataAccessor();
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    StateModelDefinition stateModelDef = new StateModelDefinition(generator.generateConfigForMasterSlave());
    accessor.setProperty(PropertyType.STATEMODELDEFS, stateModelDef, "MasterSlave");

    context = new NotificationContext(manager);
    CMTask handler;
    CMStateTransitionHandler stHandler = new CMStateTransitionHandler(stateModel, message, context);

    handler = new CMTask(message, context, stHandler, executor);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskHandler.testInvocationAnnotated() at "+ new Date(System.currentTimeMillis()));
  }

}
