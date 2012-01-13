package com.linkedin.clustermanager;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.Mocks.MockManager;
import com.linkedin.clustermanager.Mocks.MockStateModel;
import com.linkedin.clustermanager.Mocks.MockStateModelAnnotated;
import com.linkedin.clustermanager.messaging.handling.CMStateTransitionHandler;
import com.linkedin.clustermanager.messaging.handling.CMTask;
import com.linkedin.clustermanager.messaging.handling.CMTaskExecutor;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;

public class TestCMTaskHandler
{
  @Test ()
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
    String clusterName="clusterName";
    context = new NotificationContext(new MockManager(clusterName));
    CMStateTransitionHandler stHandler = new CMStateTransitionHandler(stateModel, message, context);
    CMTask handler;
    handler = new CMTask(message, context, stHandler, executor);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskHandler.testInvocation()");
  }

  @Test ()
  public void testInvocationAnnotated() throws Exception
  {
    System.out.println("START TestCMTaskHandler.testInvocationAnnotated()");
    CMTaskExecutor executor = new CMTaskExecutor();
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
    MockStateModelAnnotated stateModel = new MockStateModelAnnotated();
    NotificationContext context;
    context = new NotificationContext(new MockManager());
    CMTask handler;
    CMStateTransitionHandler stHandler = new CMStateTransitionHandler(stateModel, message, context);

    handler = new CMTask(message, context, stHandler, executor);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
    System.out.println("END TestCMTaskHandler.testInvocationAnnotated()");
  }

}
