package org.apache.helix.integration;

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

import java.util.HashSet;
import java.util.UUID;

import org.apache.helix.Criteria;
import org.apache.helix.Criteria.DataSource;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestMessagingService extends ZkStandAloneCMTestBase {
  public static class TestMessagingHandlerFactory implements MessageHandlerFactory {
    public static HashSet<String> _processedMsgIds = new HashSet<String>();

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new TestMessagingHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return "TestExtensibility";
    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub

    }

    public static class TestMessagingHandler extends MessageHandler {
      public TestMessagingHandler(Message message, NotificationContext context) {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        HelixTaskResult result = new HelixTaskResult();
        result.setSuccess(true);
        Thread.sleep(1000);
        System.out.println("TestMessagingHandler " + _message.getMessageId());
        _processedMsgIds.add(_message.getRecord().getSimpleField("TestMessagingPara"));
        result.getTaskResultMap().put("ReplyMessage", "TestReplyMessage");
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        // TODO Auto-generated method stub

      }
    }
  }

  @Test()
  public void testMessageSimpleSend() throws Exception {
    String hostSrc = "localhost_" + START_PORT;
    String hostDest = "localhost_" + (START_PORT + 1);

    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _participants[1].getMessagingService().registerMessageHandlerFactory(factory.getMessageType(),
        factory);

    MessageId msgId = MessageId.from(new UUID(123, 456).toString());
    Message msg = new Message(factory.getMessageType(), msgId);
    msg.setMessageId(msgId);
    msg.setSrcName(hostSrc);
    msg.setTgtSessionId(SessionId.from("*"));
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);

    // int nMsgs = _startCMResultMap.get(hostSrc)._manager.getMessagingService().send(cr, msg);
    int nMsgs = _participants[0].getMessagingService().send(cr, msg);
    AssertJUnit.assertTrue(nMsgs == 1);
    Thread.sleep(2500);
    // Thread.currentThread().join();
    AssertJUnit.assertTrue(TestMessagingHandlerFactory._processedMsgIds.contains(para));

    cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setDataSource(DataSource.IDEALSTATES);

    // nMsgs = _startCMResultMap.get(hostSrc)._manager.getMessagingService().send(cr, msg);
    nMsgs = _participants[0].getMessagingService().send(cr, msg);
    AssertJUnit.assertTrue(nMsgs == 1);
    Thread.sleep(2500);
    // Thread.currentThread().join();
    AssertJUnit.assertTrue(TestMessagingHandlerFactory._processedMsgIds.contains(para));

  }

  public static class MockAsyncCallback extends AsyncCallback {

    public MockAsyncCallback() {
    }

    @Override
    public void onTimeOut() {
      // TODO Auto-generated method stub

    }

    @Override
    public void onReplyMessage(Message message) {
      // TODO Auto-generated method stub

    }

  }

  public static class TestAsyncCallback extends AsyncCallback {
    public TestAsyncCallback(long timeout) {
      super(timeout);
    }

    static HashSet<String> _replyedMessageContents = new HashSet<String>();
    public boolean timeout = false;

    @Override
    public void onTimeOut() {
      timeout = true;
    }

    @Override
    public void onReplyMessage(Message message) {
      // TODO Auto-generated method stub
      System.out.println("OnreplyMessage: "
          + message.getRecord().getMapField(Message.Attributes.MESSAGE_RESULT.toString())
              .get("ReplyMessage"));
      if (message.getRecord().getMapField(Message.Attributes.MESSAGE_RESULT.toString())
          .get("ReplyMessage") == null) {
      }
      _replyedMessageContents.add(message.getRecord()
          .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ReplyMessage"));
    }

  }

  @Test()
  public void testMessageSimpleSendReceiveAsync() throws Exception {
    String hostSrc = "localhost_" + START_PORT;
    String hostDest = "localhost_" + (START_PORT + 1);

    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _participants[1].getMessagingService().registerMessageHandlerFactory(factory.getMessageType(),
        factory);

    _participants[0].getMessagingService().registerMessageHandlerFactory(factory.getMessageType(),
        factory);

    MessageId msgId = MessageId.from(new UUID(123, 456).toString());
    Message msg = new Message(factory.getMessageType(), msgId);
    msg.setMessageId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId(SessionId.from("*"));
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);

    TestAsyncCallback callback = new TestAsyncCallback(60000);

    _participants[0].getMessagingService().send(cr, msg, callback, 60000);

    Thread.sleep(2000);
    // Thread.currentThread().join();
    AssertJUnit.assertTrue(TestAsyncCallback._replyedMessageContents.contains("TestReplyMessage"));
    AssertJUnit.assertTrue(callback.getMessageReplied().size() == 1);

    TestAsyncCallback callback2 = new TestAsyncCallback(500);
    _participants[0].getMessagingService().send(cr, msg, callback2, 500);

    Thread.sleep(3000);
    // Thread.currentThread().join();
    AssertJUnit.assertTrue(callback2.isTimedOut());

    cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setDataSource(DataSource.IDEALSTATES);

    callback = new TestAsyncCallback(60000);

    _participants[0].getMessagingService().send(cr, msg, callback, 60000);

    Thread.sleep(2000);
    // Thread.currentThread().join();
    AssertJUnit.assertTrue(TestAsyncCallback._replyedMessageContents.contains("TestReplyMessage"));
    AssertJUnit.assertTrue(callback.getMessageReplied().size() == 1);

    callback2 = new TestAsyncCallback(500);
    _participants[0].getMessagingService().send(cr, msg, callback2, 500);

    Thread.sleep(3000);
    // Thread.currentThread().join();
    AssertJUnit.assertTrue(callback2.isTimedOut());

  }

  @Test()
  public void testBlockingSendReceive() throws Exception {
    String hostSrc = "localhost_" + START_PORT;
    String hostDest = "localhost_" + (START_PORT + 1);

    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _participants[1].getMessagingService().registerMessageHandlerFactory(factory.getMessageType(),
        factory);

    MessageId msgId = MessageId.from(new UUID(123, 456).toString());
    Message msg = new Message(factory.getMessageType(), msgId);
    msg.setMessageId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId(SessionId.from("*"));
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);

    AsyncCallback asyncCallback = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, asyncCallback, 60000);

    AssertJUnit.assertTrue(asyncCallback.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ReplyMessage")
        .equals("TestReplyMessage"));
    AssertJUnit.assertTrue(asyncCallback.getMessageReplied().size() == 1);

    AsyncCallback asyncCallback2 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, asyncCallback2, 500);
    AssertJUnit.assertTrue(asyncCallback2.isTimedOut());

  }

  @Test()
  public void testMultiMessageCriteria() throws Exception {
    String hostSrc = "localhost_" + START_PORT;

    for (int i = 0; i < NODE_NR; i++) {
      TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
      _participants[0].getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);

    }
    MessageId msgId = MessageId.from(new UUID(123, 456).toString());
    Message msg = new Message(new TestMessagingHandlerFactory().getMessageType(), msgId);
    msg.setMessageId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId(SessionId.from("*"));
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName("%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    AsyncCallback callback1 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback1, 10000);

    AssertJUnit.assertTrue(callback1.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ReplyMessage")
        .equals("TestReplyMessage"));
    AssertJUnit.assertTrue(callback1.getMessageReplied().size() == NODE_NR - 1);

    AsyncCallback callback2 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback2, 500);

    AssertJUnit.assertTrue(callback2.isTimedOut());

    cr.setPartition("TestDB_17");
    AsyncCallback callback3 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback3, 10000);
    AssertJUnit.assertTrue(callback3.getMessageReplied().size() == _replica - 1);

    cr.setPartition("TestDB_15");
    AsyncCallback callback4 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback4, 10000);
    AssertJUnit.assertTrue(callback4.getMessageReplied().size() == _replica);

    cr.setPartitionState("SLAVE");
    AsyncCallback callback5 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback5, 10000);
    AssertJUnit.assertTrue(callback5.getMessageReplied().size() == _replica - 1);

    cr.setDataSource(DataSource.IDEALSTATES);
    AsyncCallback callback6 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback6, 10000);
    AssertJUnit.assertTrue(callback6.getMessageReplied().size() == _replica - 1);
  }

  @Test()
  public void sendSelfMsg() {
    String hostSrc = "localhost_" + START_PORT;

    for (int i = 0; i < NODE_NR; i++) {
      TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);

    }
    MessageId msgId = MessageId.from(new UUID(123, 456).toString());
    Message msg = new Message(new TestMessagingHandlerFactory().getMessageType(), msgId);
    msg.setMessageId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId(SessionId.from("*"));
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName("%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setSelfExcluded(false);
    AsyncCallback callback1 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback1, 10000);

    AssertJUnit.assertTrue(callback1.getMessageReplied().size() == NODE_NR);
    AssertJUnit.assertTrue(callback1.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ReplyMessage")
        .equals("TestReplyMessage"));
  }

  @Test()
  public void testControllerMessage() throws Exception {
    String hostSrc = "localhost_" + START_PORT;

    for (int i = 0; i < NODE_NR; i++) {
      TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);

    }
    MessageId msgId = MessageId.from(new UUID(123, 456).toString());
    Message msg = new Message(MessageType.CONTROLLER_MSG, msgId);
    msg.setMessageId(msgId);
    msg.setSrcName(hostSrc);

    msg.setTgtSessionId(SessionId.from("*"));
    msg.setMsgState(MessageState.NEW);
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);

    Criteria cr = new Criteria();
    cr.setInstanceName("*");
    cr.setRecipientInstanceType(InstanceType.CONTROLLER);
    cr.setSessionSpecific(false);

    AsyncCallback callback1 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback1, 10000);

    AssertJUnit.assertTrue(callback1.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ControllerResult")
        .indexOf(hostSrc) != -1);
    AssertJUnit.assertTrue(callback1.getMessageReplied().size() == 1);

    msgId = MessageId.from(UUID.randomUUID().toString());
    msg.setMessageId(msgId);
    cr.setPartition("TestDB_17");
    AsyncCallback callback2 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback2, 10000);

    AssertJUnit.assertTrue(callback2.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ControllerResult")
        .indexOf(hostSrc) != -1);

    AssertJUnit.assertTrue(callback2.getMessageReplied().size() == 1);

    msgId = MessageId.from(UUID.randomUUID().toString());
    msg.setMessageId(msgId);
    cr.setPartitionState("SLAVE");
    AsyncCallback callback3 = new MockAsyncCallback();
    _participants[0].getMessagingService().sendAndWait(cr, msg, callback3, 10000);
    AssertJUnit.assertTrue(callback3.getMessageReplied().get(0).getRecord()
        .getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ControllerResult")
        .indexOf(hostSrc) != -1);

    AssertJUnit.assertTrue(callback3.getMessageReplied().size() == 1);
  }
}
