package com.linkedin.clustermanager;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.Mocks.MockManager;
import com.linkedin.clustermanager.Mocks.MockStateModel;
import com.linkedin.clustermanager.Mocks.MockStateModelAnnotated;
import com.linkedin.clustermanager.messaging.handling.CMStateTransitionHandler;
import com.linkedin.clustermanager.messaging.handling.CMTask;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;

public class TestCMTaskHandler
{
  @Test (groups = {"unitTest"})
  public void testCMTaskHandler() throws Exception
  {
    System.out.println("TestCMTaskHandler.testInvocation()");
    Message message = new Message(MessageType.STATE_TRANSITION,"Some unique id");
    message.setSrcName("cm-instance-0");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setStateUnitKey("Teststateunitkey");
    message.setMsgId("Some unique message id");
    message.setStateUnitGroup("TeststateunitGroup");
    message.setTgtName("localhost");
    MockStateModel stateModel = new MockStateModel();
    NotificationContext context;
    CMStateTransitionHandler stHandler = new CMStateTransitionHandler(stateModel);
    String clusterName="clusterName";
    context = new NotificationContext(new MockManager(clusterName));
    CMTask handler;
    handler = new CMTask(message, context, stHandler, null);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
  }

  @Test (groups = {"unitTest"})
  public void testInvocationAnnotated() throws Exception
  {
    System.out.println("TestCMTaskHandler.testInvocationAnnotated()");
    Message message = new Message(MessageType.STATE_TRANSITION,"Some unique id");
    message.setSrcName("cm-instance-0");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setStateUnitKey("Teststateunitkey");
    message.setMsgId("Some unique message id");
    message.setTgtName("localhost");
    MockStateModelAnnotated stateModel = new MockStateModelAnnotated();
    NotificationContext context;
    context = new NotificationContext(new MockManager());
    CMTask handler;
    CMStateTransitionHandler stHandler = new CMStateTransitionHandler(stateModel);

    handler = new CMTask(message, context, stHandler, null);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
  }

}
