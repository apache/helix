package org.apache.helix.messaging.handling;

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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.Mocks;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestHelixTaskExecutor {
  public static class MockClusterManager extends Mocks.MockManager {
    @Override
    public String getSessionId() {
      return "123";
    }
  }

  class TestMessageHandlerFactory implements MessageHandlerFactory {
    int _handlersCreated = 0;
    ConcurrentHashMap<String, String> _processedMsgIds = new ConcurrentHashMap<String, String>();

    class TestMessageHandler extends MessageHandler {
      public TestMessageHandler(Message message, NotificationContext context) {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        HelixTaskResult result = new HelixTaskResult();
        _processedMsgIds.put(_message.getMessageId().stringify(), _message.getMessageId()
            .stringify());
        Thread.sleep(100);
        result.setSuccess(true);
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        // TODO Auto-generated method stub

      }
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      // TODO Auto-generated method stub
      if (message.getMsgSubType() != null && message.getMsgSubType().equals("EXCEPTION")) {
        throw new HelixException("Test Message handler exception, can ignore");
      }
      _handlersCreated++;
      return new TestMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      // TODO Auto-generated method stub
      return "TestingMessageHandler";
    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub

    }
  }

  class TestMessageHandlerFactory2 extends TestMessageHandlerFactory {
    @Override
    public String getMessageType() {
      // TODO Auto-generated method stub
      return "TestingMessageHandler2";
    }

  }

  class CancellableHandlerFactory implements MessageHandlerFactory {

    int _handlersCreated = 0;
    ConcurrentHashMap<String, String> _processedMsgIds = new ConcurrentHashMap<String, String>();
    ConcurrentHashMap<String, String> _processingMsgIds = new ConcurrentHashMap<String, String>();
    ConcurrentHashMap<String, String> _timedOutMsgIds = new ConcurrentHashMap<String, String>();

    class CancellableHandler extends MessageHandler {
      public CancellableHandler(Message message, NotificationContext context) {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      public boolean _interrupted = false;

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        HelixTaskResult result = new HelixTaskResult();
        int sleepTimes = 15;
        if (_message.getRecord().getSimpleFields().containsKey("Cancelcount")) {
          sleepTimes = 10;
        }
        _processingMsgIds.put(_message.getMessageId().stringify(), _message.getMessageId()
            .stringify());
        try {
          for (int i = 0; i < sleepTimes; i++) {
            Thread.sleep(100);
          }
        } catch (InterruptedException e) {
          _interrupted = true;
          _timedOutMsgIds.put(_message.getMessageId().stringify(), "");
          result.setInterrupted(true);
          if (!_message.getRecord().getSimpleFields().containsKey("Cancelcount")) {
            _message.getRecord().setSimpleField("Cancelcount", "1");
          } else {
            int c = Integer.parseInt(_message.getRecord().getSimpleField("Cancelcount"));
            _message.getRecord().setSimpleField("Cancelcount", "" + (c + 1));
          }
          throw e;
        }
        _processedMsgIds.put(_message.getMessageId().stringify(), _message.getMessageId()
            .stringify());
        result.setSuccess(true);
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        // TODO Auto-generated method stub
        _message.getRecord().setSimpleField("exception", e.getMessage());
      }
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      // TODO Auto-generated method stub
      _handlersCreated++;
      return new CancellableHandler(message, context);
    }

    @Override
    public String getMessageType() {
      // TODO Auto-generated method stub
      return "Cancellable";
    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub
      _handlersCreated = 0;
      _processedMsgIds.clear();
      _processingMsgIds.clear();
      _timedOutMsgIds.clear();
    }
  }

  @Test()
  public void testNormalMsgExecution() throws InterruptedException {
    System.out.println("START TestCMTaskExecutor.testNormalMsgExecution()");
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);

    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
    executor.registerMessageHandlerFactory(factory2.getMessageType(), factory2);

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg =
          new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from(manager.getSessionId()));
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      msgList.add(msg);
    }

    int nMsgs2 = 6;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg =
          new Message(factory2.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from(manager.getSessionId()));
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      msgList.add(msg);
    }
    executor.onMessage("someInstance", msgList, changeContext);

    Thread.sleep(1000);

    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory2._processedMsgIds.size() == nMsgs2);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1);
    AssertJUnit.assertTrue(factory2._handlersCreated == nMsgs2);

    for (Message record : msgList) {
      AssertJUnit.assertTrue(factory._processedMsgIds.containsKey(record.getId())
          || factory2._processedMsgIds.containsKey(record.getId()));
      AssertJUnit.assertFalse(factory._processedMsgIds.containsKey(record.getId())
          && factory2._processedMsgIds.containsKey(record.getId()));

    }
    System.out.println("END TestCMTaskExecutor.testNormalMsgExecution()");
  }

  @Test()
  public void testUnknownTypeMsgExecution() throws InterruptedException {
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);

    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg =
          new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from(manager.getSessionId()));
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }

    int nMsgs2 = 4;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg =
          new Message(factory2.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from(manager.getSessionId()));
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }
    executor.onMessage("someInstance", msgList, changeContext);

    Thread.sleep(1000);

    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory2._processedMsgIds.size() == 0);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1);
    AssertJUnit.assertTrue(factory2._handlersCreated == 0);

    for (Message message : msgList) {
      if (message.getMsgType().equalsIgnoreCase(factory.getMessageType())) {
        AssertJUnit.assertTrue(factory._processedMsgIds.containsKey(message.getId()));
      }
    }
  }

  @Test()
  public void testMsgSessionId() throws InterruptedException {
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);

    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
    executor.registerMessageHandlerFactory(factory2.getMessageType(), factory2);

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg =
          new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from("*"));
      msg.setTgtName("");
      msgList.add(msg);
    }

    int nMsgs2 = 4;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg =
          new Message(factory2.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from("some other session id"));
      msg.setTgtName("");
      msgList.add(msg);
    }
    executor.onMessage("someInstance", msgList, changeContext);

    Thread.sleep(1000);

    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory2._processedMsgIds.size() == 0);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1);
    AssertJUnit.assertTrue(factory2._handlersCreated == 0);

    for (Message message : msgList) {
      if (message.getMsgType().equalsIgnoreCase(factory.getMessageType())) {
        AssertJUnit.assertTrue(factory._processedMsgIds.containsKey(message.getId()));
      }
    }
  }

  @Test()
  public void testCreateHandlerException() throws InterruptedException {
    System.out.println("START TestCMTaskExecutor.testCreateHandlerException()");
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg =
          new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from(manager.getSessionId()));
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      msgList.add(msg);
    }
    Message exceptionMsg =
        new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
    exceptionMsg.setTgtSessionId(SessionId.from(manager.getSessionId()));
    exceptionMsg.setMsgSubType("EXCEPTION");
    exceptionMsg.setTgtName("Localhost_1123");
    exceptionMsg.setSrcName("127.101.1.23_2234");
    exceptionMsg.setCorrelationId(UUID.randomUUID().toString());
    msgList.add(exceptionMsg);

    executor.onMessage("someInstance", msgList, changeContext);

    Thread.sleep(1000);

    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1);

    AssertJUnit.assertTrue(exceptionMsg.getMsgState() == MessageState.UNPROCESSABLE);
    System.out.println("END TestCMTaskExecutor.testCreateHandlerException()");
  }

  @Test()
  public void testTaskCancellation() throws InterruptedException {
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    CancellableHandlerFactory factory = new CancellableHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 0;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg =
          new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from("*"));
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }

    List<Message> msgListToCancel = new ArrayList<Message>();
    int nMsgs2 = 4;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg =
          new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from("*"));
      msgList.add(msg);
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgListToCancel.add(msg);
    }
    executor.onMessage("someInstance", msgList, changeContext);
    Thread.sleep(500);
    for (int i = 0; i < nMsgs2; i++) {
      // executor.cancelTask(msgListToCancel.get(i), changeContext);
      HelixTask task = new HelixTask(msgListToCancel.get(i), changeContext, null, null);
      executor.cancelTask(task);
    }
    Thread.sleep(1500);

    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1 + nMsgs2);

    AssertJUnit.assertTrue(factory._processingMsgIds.size() == nMsgs1 + nMsgs2);

    for (Message message : msgList) {
      if (message.getMsgType().equalsIgnoreCase(factory.getMessageType())) {
        AssertJUnit.assertTrue(factory._processingMsgIds.containsKey(message.getId()));
      }
    }
  }

  @Test()
  public void testShutdown() throws InterruptedException {
    System.out.println("START TestCMTaskExecutor.testShutdown()");
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);

    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
    executor.registerMessageHandlerFactory(factory2.getMessageType(), factory2);

    CancellableHandlerFactory factory3 = new CancellableHandlerFactory();
    executor.registerMessageHandlerFactory(factory3.getMessageType(), factory3);
    int nMsg1 = 10, nMsg2 = 10, nMsg3 = 10;
    List<Message> msgList = new ArrayList<Message>();

    for (int i = 0; i < nMsg1; i++) {
      Message msg =
          new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from("*"));
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }

    for (int i = 0; i < nMsg2; i++) {
      Message msg =
          new Message(factory2.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from("*"));
      msgList.add(msg);
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }

    for (int i = 0; i < nMsg3; i++) {
      Message msg =
          new Message(factory3.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from("*"));
      msgList.add(msg);
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }
    NotificationContext changeContext = new NotificationContext(manager);
    executor.onMessage("some", msgList, changeContext);
    Thread.sleep(500);
    for (ExecutorService svc : executor._executorMap.values()) {
      Assert.assertFalse(svc.isShutdown());
    }
    Assert.assertTrue(factory._processedMsgIds.size() > 0);
    executor.shutdown();
    for (ExecutorService svc : executor._executorMap.values()) {
      Assert.assertTrue(svc.isShutdown());
    }
    System.out.println("END TestCMTaskExecutor.testShutdown()");
  }

  @Test()
  public void testNoRetry() throws InterruptedException {
    // String p = "test_";
    // System.out.println(p.substring(p.lastIndexOf('_')+1));
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    CancellableHandlerFactory factory = new CancellableHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);

    NotificationContext changeContext = new NotificationContext(manager);

    List<Message> msgList = new ArrayList<Message>();
    int nMsgs2 = 4;
    // Test the case in which retry = 0
    for (int i = 0; i < nMsgs2; i++) {
      Message msg =
          new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from("*"));
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setExecutionTimeout((i + 1) * 600);
      msgList.add(msg);
    }
    executor.onMessage("someInstance", msgList, changeContext);

    Thread.sleep(4000);

    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs2);
    AssertJUnit.assertEquals(factory._timedOutMsgIds.size(), 2);
    // AssertJUnit.assertFalse(msgList.get(0).getRecord().getSimpleFields().containsKey("TimeOut"));
    for (int i = 0; i < nMsgs2 - 2; i++) {
      if (msgList.get(i).getMsgType().equalsIgnoreCase(factory.getMessageType())) {
        AssertJUnit.assertTrue(msgList.get(i).getRecord().getSimpleFields()
            .containsKey("Cancelcount"));
        AssertJUnit.assertTrue(factory._timedOutMsgIds.containsKey(msgList.get(i).getId()));
      }
    }
  }

  @Test()
  public void testRetryOnce() throws InterruptedException {
    // Logger.getRootLogger().setLevel(Level.INFO);

    // String p = "test_";
    // System.out.println(p.substring(p.lastIndexOf('_')+1));
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    CancellableHandlerFactory factory = new CancellableHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);

    NotificationContext changeContext = new NotificationContext(manager);

    List<Message> msgList = new ArrayList<Message>();

    // factory.reset();
    // msgList.clear();
    // Test the case that the message are executed for the second time
    int nMsgs2 = 4;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg =
          new Message(factory.getMessageType(), MessageId.from(UUID.randomUUID().toString()));
      msg.setTgtSessionId(SessionId.from("*"));
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setExecutionTimeout((i + 1) * 600);
      msg.setRetryCount(1);
      msgList.add(msg);
    }
    executor.onMessage("someInstance", msgList, changeContext);
    Thread.sleep(3500);
    AssertJUnit.assertEquals(factory._processedMsgIds.size(), 3);
    AssertJUnit.assertTrue(msgList.get(0).getRecord().getSimpleField("Cancelcount").equals("2"));
    AssertJUnit.assertTrue(msgList.get(1).getRecord().getSimpleField("Cancelcount").equals("1"));
    AssertJUnit.assertEquals(factory._timedOutMsgIds.size(), 2);
    AssertJUnit.assertTrue(executor._taskMap.size() == 0);

  }
}
