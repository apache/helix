package com.linkedin.clustermanager;

import java.util.Date;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.Mocks.MockManager;
import com.linkedin.clustermanager.Mocks.MockStateModel;
import com.linkedin.clustermanager.Mocks.MockStateModelAnnotated;
import com.linkedin.clustermanager.messaging.handling.CMStateTransitionHandler;
import com.linkedin.clustermanager.messaging.handling.CMTask;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.tools.StateModelConfigGenerator;

public class TestCMTaskHandler
{
  @Test()
  public void testInvocation() throws Exception
  {
    System.out.println("START TestCMTaskHandler.testInvocation() at "+ new Date(System.currentTimeMillis()));
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
    MockStateModel stateModel = new MockStateModel();
    NotificationContext context;
    CMStateTransitionHandler stHandler = new CMStateTransitionHandler(stateModel);
    MockManager manager = new MockManager("clusterName");
    ClusterDataAccessor accessor = manager.getDataAccessor();
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    StateModelDefinition stateModelDef = new StateModelDefinition(generator.generateConfigForMasterSlave());
    accessor.setProperty(PropertyType.STATEMODELDEFS, stateModelDef, "MasterSlave");

    context = new NotificationContext(manager);
    CMTask handler;
    handler = new CMTask(message, context, stHandler, null);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskHandler.testInvocation() at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testInvocationAnnotated() throws Exception
  {
    System.out.println("START TestCMTaskHandler.testInvocationAnnotated() at " + new Date(System.currentTimeMillis()));
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
    ClusterDataAccessor accessor = manager.getDataAccessor();
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    StateModelDefinition stateModelDef = new StateModelDefinition(generator.generateConfigForMasterSlave());
    accessor.setProperty(PropertyType.STATEMODELDEFS, stateModelDef, "MasterSlave");

    context = new NotificationContext(manager);
    CMTask handler;
    CMStateTransitionHandler stHandler = new CMStateTransitionHandler(stateModel);

    handler = new CMTask(message, context, stHandler, null);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskHandler.testInvocationAnnotated() at "+ new Date(System.currentTimeMillis()));
  }

}
