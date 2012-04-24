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
package com.linkedin.helix.integration;

import java.util.HashSet;
import java.util.UUID;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.messaging.AsyncCallback;
import com.linkedin.helix.messaging.handling.HelixTaskResult;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.messaging.handling.MessageHandlerFactory;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;
import com.linkedin.helix.model.Message.MessageType;

public class TestMessagingService extends ZkStandAloneCMTestBase
{
  public static class TestMessagingHandlerFactory implements
      MessageHandlerFactory
  {
    public static HashSet<String> _processedMsgIds = new HashSet<String>();

    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      return new TestMessagingHandler(message, context);
    }

    @Override
    public String getMessageType()
    {
      return "TestExtensibility";
    }

    @Override
    public void reset()
    {
      // TODO Auto-generated method stub

    }

    public static class TestMessagingHandler extends MessageHandler
    {
      public TestMessagingHandler(Message message, NotificationContext context)
      {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException
      {
        HelixTaskResult result = new HelixTaskResult();
        result.setSuccess(true);
        Thread.sleep(1000);
        System.out.println("TestMessagingHandler " + _message.getMsgId());
        _processedMsgIds.add(_message.getRecord().getSimpleField(
            "TestMessagingPara"));
        result.getTaskResultMap().put("ReplyMessage", "TestReplyMessage");
        return result;
      }


      @Override
      public void onError( Exception e, ErrorCode code, ErrorType type)
      {
        // TODO Auto-generated method stub

      }
    }
  }

  @Test()
  public void TestMessageSimpleSend() throws Exception
  {
    String hostSrc = "localhost_" + START_PORT;
    String hostDest = "localhost_" + (START_PORT + 1);

    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _startCMResultMap.get(hostDest)._manager.getMessagingService()
        .registerMessageHandlerFactory(factory.getMessageType(), factory);

    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(factory.getMessageType(),msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(hostSrc);
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);

    int nMsgs = _startCMResultMap.get(hostSrc)._manager.getMessagingService().send(cr, msg);
    AssertJUnit.assertTrue(nMsgs == 1);
    Thread.sleep(2500);
    // Thread.currentThread().join();
    AssertJUnit.assertTrue(TestMessagingHandlerFactory._processedMsgIds
        .contains(para));

  }

  public static class MockAsyncCallback extends AsyncCallback
  {

    public MockAsyncCallback()
    {
    }

    @Override
    public void onTimeOut()
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void onReplyMessage(Message message)
    {
      // TODO Auto-generated method stub

    }

  }

  public static class TestAsyncCallback extends AsyncCallback
  {
    public TestAsyncCallback(long timeout)
    {
      super(timeout);
    }

    static HashSet<String> _replyedMessageContents = new HashSet<String>();
    public boolean timeout = false;

    @Override
    public void onTimeOut()
    {
      timeout = true;
    }

    @Override
    public void onReplyMessage(Message message)
    {
      // TODO Auto-generated method stub
      System.out.println("OnreplyMessage: "
          + message.getRecord()
              .getMapField(Message.Attributes.MESSAGE_RESULT.toString())
              .get("ReplyMessage"));
      if(message.getRecord()
          .getMapField(Message.Attributes.MESSAGE_RESULT.toString())
          .get("ReplyMessage") == null)
      {
        int x = 0;
      }
      _replyedMessageContents.add(message.getRecord()
          .getMapField(Message.Attributes.MESSAGE_RESULT.toString())
          .get("ReplyMessage"));
    }

  }

  @Test()
  public void TestMessageSimpleSendReceiveAsync() throws Exception
  {
    String hostSrc = "localhost_" + START_PORT;
    String hostDest = "localhost_" + (START_PORT + 1);

    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _startCMResultMap.get(hostDest)._manager.getMessagingService()
        .registerMessageHandlerFactory(factory.getMessageType(), factory);

    _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .registerMessageHandlerFactory(factory.getMessageType(), factory);

    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(factory.getMessageType(),msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);

    TestAsyncCallback callback = new TestAsyncCallback(60000);

    _startCMResultMap.get(hostSrc)._manager.getMessagingService().send(cr, msg, callback, 60000);

    Thread.sleep(2000);
    // Thread.currentThread().join();
    AssertJUnit.assertTrue(TestAsyncCallback._replyedMessageContents
        .contains("TestReplyMessage"));
    AssertJUnit.assertTrue(callback.getMessageReplied().size() == 1);

    TestAsyncCallback callback2 = new TestAsyncCallback(500);
    _startCMResultMap.get(hostSrc)._manager.getMessagingService().send(cr, msg, callback2, 500);

    Thread.sleep(3000);
    // Thread.currentThread().join();
    AssertJUnit.assertTrue(callback2.isTimedOut());

  }

  @Test()
  public void TestBlockingSendReceive() throws Exception
  {
    String hostSrc = "localhost_" + START_PORT;
    String hostDest = "localhost_" + (START_PORT + 1);

    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _startCMResultMap.get(hostDest)._manager.getMessagingService()
        .registerMessageHandlerFactory(factory.getMessageType(), factory);

    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(factory.getMessageType(),msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);

    AsyncCallback asyncCallback = new MockAsyncCallback();
    int messagesSent = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, asyncCallback, 60000);

    AssertJUnit.assertTrue(asyncCallback.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString())
        .get("ReplyMessage").equals("TestReplyMessage"));
    AssertJUnit.assertTrue(asyncCallback.getMessageReplied().size() == 1);


    AsyncCallback asyncCallback2 = new MockAsyncCallback();
    messagesSent = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, asyncCallback2, 500);
    AssertJUnit.assertTrue(asyncCallback2.isTimedOut());

  }

  @Test()
  public void TestMultiMessageCriteria() throws Exception
  {
    String hostSrc = "localhost_" + START_PORT;

    for (int i = 0; i < NODE_NR; i++)
    {
      TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService()
          .registerMessageHandlerFactory(factory.getMessageType(), factory);
    }
    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(
        new TestMessagingHandlerFactory().getMessageType(),msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName("%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    AsyncCallback callback1 = new MockAsyncCallback();
    int messageSent1 = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, callback1, 2000);

    AssertJUnit.assertTrue(callback1.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString())
        .get("ReplyMessage").equals("TestReplyMessage"));
    AssertJUnit.assertTrue(callback1.getMessageReplied().size() == NODE_NR - 1);

    AsyncCallback callback2 = new MockAsyncCallback();
    int messageSent2 = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, callback2, 500);
    AssertJUnit.assertTrue(callback2.isTimedOut());

    cr.setPartition("TestDB_17");
    AsyncCallback callback3 = new MockAsyncCallback();
    int messageSent3 = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, callback3, 2000);
    AssertJUnit.assertTrue(callback3.getMessageReplied().size() == 3);

    cr.setPartitionState("SLAVE");
    AsyncCallback callback4 = new MockAsyncCallback();
    int messageSent4 = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, callback4, 2000);
    AssertJUnit.assertTrue(callback4.getMessageReplied().size() == 2);
  }

  @Test()
  public void sendSelfMsg()
  {
    String hostSrc = "localhost_" + START_PORT;

    for (int i = 0; i < NODE_NR; i++)
    {
      TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService()
          .registerMessageHandlerFactory(factory.getMessageType(), factory);
    }
    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(
        new TestMessagingHandlerFactory().getMessageType(),msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName("%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setSelfExcluded(false);
    AsyncCallback callback1 = new MockAsyncCallback();
    int messageSent1 = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, callback1, 3000);

    AssertJUnit.assertTrue(callback1.getMessageReplied().size() == NODE_NR);
    AssertJUnit.assertTrue(callback1.getMessageReplied().get(0).getRecord()
                           .getMapField(Message.Attributes.MESSAGE_RESULT.toString())
                           .get("ReplyMessage").equals("TestReplyMessage"));
  }

  @Test()
  public void TestControllerMessage() throws Exception
  {
    String hostSrc = "localhost_" + START_PORT;

    for (int i = 0; i < NODE_NR; i++)
    {
      TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService()
          .registerMessageHandlerFactory(factory.getMessageType(), factory);
    }
    String msgId = new UUID(123, 456).toString();
    Message msg = new Message(MessageType.CONTROLLER_MSG,msgId);
    msg.setMsgId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName("*");
    cr.setRecipientInstanceType(InstanceType.CONTROLLER);
    cr.setSessionSpecific(false);

    AsyncCallback callback1 = new MockAsyncCallback();
    int messagesSent = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, callback1, 2000);

    AssertJUnit.assertTrue(callback1.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString())
        .get("ControllerResult").indexOf(hostSrc) != -1);
    AssertJUnit.assertTrue(callback1.getMessageReplied().size() == 1);

    msgId = UUID.randomUUID().toString();
    msg.setMsgId(msgId);
    cr.setPartition("TestDB_17");
    AsyncCallback callback2 = new MockAsyncCallback();
    messagesSent = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, callback2, 2000);
    AssertJUnit.assertTrue(callback2.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString())
        .get("ControllerResult").indexOf(hostSrc) != -1);

    AssertJUnit.assertTrue(callback2.getMessageReplied().size() == 1);

    msgId = UUID.randomUUID().toString();
    msg.setMsgId(msgId);
    cr.setPartitionState("SLAVE");
    AsyncCallback callback3 = new MockAsyncCallback();
    messagesSent = _startCMResultMap.get(hostSrc)._manager.getMessagingService()
        .sendAndWait(cr, msg, callback3, 2000);
    AssertJUnit.assertTrue(callback3.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString())
        .get("ControllerResult").indexOf(hostSrc) != -1);

    AssertJUnit.assertTrue(callback3.getMessageReplied().size() == 1);
  }
}
