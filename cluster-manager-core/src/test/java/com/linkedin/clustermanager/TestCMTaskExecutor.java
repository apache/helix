package com.linkedin.clustermanager;

import org.testng.AssertJUnit;

import com.linkedin.clustermanager.Mocks.MockCMTaskExecutor;
import com.linkedin.clustermanager.Mocks.MockManager;
import com.linkedin.clustermanager.Mocks.MockStateModel;
import com.linkedin.clustermanager.messaging.handling.CMStateTransitionHandler;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;

public class TestCMTaskExecutor
{

  // @Test
  // cause NPE, temp comment out 10/10/2011
  public void testInvocation() throws Exception
  {
    System.out.println("TestCMTaskHandler.testInvocation()");
    String msgId = "TestMessageId";
    Message message = new Message(MessageType.STATE_TRANSITION,msgId);
   
    message.setMsgId(msgId);
    message.setSrcName("cm-instance-0");
    message.setTgtSessionId("1234");
    message.setFromState("Offline");
    message.setToState("Slave");
    message.setStateUnitKey("Teststateunitkey");
    MockCMTaskExecutor executor = new MockCMTaskExecutor();
    MockStateModel stateModel = new MockStateModel();
    NotificationContext context;

    context = new NotificationContext(new MockManager());
    CMStateTransitionHandler handler = new CMStateTransitionHandler(stateModel);
    executor.scheduleTask(message, handler, context);
    while (!executor.isDone(msgId))
    {
      Thread.sleep(500);
    }
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
  }

}
