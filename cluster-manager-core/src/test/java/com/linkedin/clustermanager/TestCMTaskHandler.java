package com.linkedin.clustermanager;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.Mocks.MockManager;
import com.linkedin.clustermanager.Mocks.MockStateModel;
import com.linkedin.clustermanager.Mocks.MockStateModelAnnotated;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.statemachine.CMTaskHandler;

public class TestCMTaskHandler
{
  @Test
  public void testInvocation() throws Exception
  {
    System.out.println("TestCMTaskHandler.testInvocation()");
    Message message = new Message(MessageType.STATE_TRANSITION);
    message.setSrcName("cm-instance-0");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setStateUnitKey("Teststateunitkey");
    message.setId("Some unique id");
    message.setMsgId("Some unique message id");
    message.setStateUnitGroup("TeststateunitGroup");
    MockStateModel stateModel = new MockStateModel();
    NotificationContext context;

    context = new NotificationContext(new MockManager());
    CMTaskHandler handler;
    handler = new CMTaskHandler(context, message, stateModel, null);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
  }

  @Test
  public void testInvocationAnnotated() throws Exception
  {
    System.out.println("TestCMTaskHandler.testInvocationAnnotated()");
    Message message = new Message(MessageType.STATE_TRANSITION);
    message.setSrcName("cm-instance-0");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setStateUnitKey("Teststateunitkey");
    message.setId("Some unique id");
    message.setMsgId("Some unique message id");
    MockStateModelAnnotated stateModel = new MockStateModelAnnotated();
    NotificationContext context;

    context = new NotificationContext(new MockManager());
    CMTaskHandler handler;
    handler = new CMTaskHandler(context, message, stateModel, null);
    handler.call();
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
  }

}
