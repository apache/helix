package com.linkedin.clustermanager;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Assert;
import org.testng.annotations.*;

import com.linkedin.clustermanager.Mocks.MockCMTaskExecutor;
import com.linkedin.clustermanager.Mocks.MockManager;
import com.linkedin.clustermanager.Mocks.MockStateModel;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.CMTaskExecutor;
import com.linkedin.clustermanager.participant.statemachine.CMTaskHandler;
import com.linkedin.clustermanager.participant.statemachine.StateModel;

public class TestCMTaskExecutor
{

  @Test
  public void testInvocation() throws Exception
  {
    System.out.println("TestCMTaskHandler.testInvocation()");
    Message message = new Message();
    String msgId = "TestMessageId";
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
    CMTaskHandler handler;
    executor.executeTask(message, stateModel, context);
    while (!executor.isDone(msgId))
    {
      Thread.sleep(500);
    }
    AssertJUnit.assertTrue(stateModel.stateModelInvoked);
  }

}
