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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.mock.MockManager;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestHelixTaskExecutor {
  public static class MockClusterManager extends MockManager {

    @Override
    public String getSessionId() {
      return "123";
    }
  }

  class TestMessageHandlerFactory implements MultiTypeMessageHandlerFactory {
    int _handlersCreated = 0;
    ConcurrentHashMap<String, String> _processedMsgIds = new ConcurrentHashMap<>();

    class TestMessageHandler extends MessageHandler {
      public TestMessageHandler(Message message, NotificationContext context) {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        HelixTaskResult result = new HelixTaskResult();
        _processedMsgIds.put(_message.getMsgId(), _message.getMsgId());
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
    public List<String> getMessageTypes() {
      return Collections.singletonList("TestingMessageHandler");
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

    @Override
    public List<String> getMessageTypes() {
      // TODO Auto-generated method stub
      return ImmutableList.of("TestingMessageHandler2");
    }

  }

  class CancellableHandlerFactory implements MultiTypeMessageHandlerFactory {

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
        _processingMsgIds.put(_message.getMsgId(), _message.getMsgId());
        try {
          for (int i = 0; i < sleepTimes; i++) {
            Thread.sleep(100);
          }
        } catch (InterruptedException e) {
          _interrupted = true;
          _timedOutMsgIds.put(_message.getMsgId(), "");
          result.setInterrupted(true);
          if (!_message.getRecord().getSimpleFields().containsKey("Cancelcount")) {
            _message.getRecord().setSimpleField("Cancelcount", "1");
          } else {
            int c = Integer.parseInt(_message.getRecord().getSimpleField("Cancelcount"));
            _message.getRecord().setSimpleField("Cancelcount", "" + (c + 1));
          }
          throw e;
        }
        _processedMsgIds.put(_message.getMsgId(), _message.getMsgId());
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

    @Override public List<String> getMessageTypes() {
      return ImmutableList.of("Cancellable");
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

  class TestStateTransitionHandlerFactory implements MultiTypeMessageHandlerFactory {
    ConcurrentHashMap<String, String> _processedMsgIds = new ConcurrentHashMap<String, String>();
    private final String _msgType;
    private final long _delay;
    public TestStateTransitionHandlerFactory(String msgType) {
      this(msgType, -1);
    }

    public TestStateTransitionHandlerFactory(String msgType, long delay) {
      _msgType = msgType;
      _delay = delay;
    }

    class TestStateTransitionMessageHandler extends MessageHandler {
      public TestStateTransitionMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        HelixTaskResult result = new HelixTaskResult();
        _processedMsgIds.put(_message.getMsgId(), _message.getMsgId());
        if (_delay > 0) {
          System.out.println("Sleeping..." + _delay);
          Thread.sleep(_delay);
        }
        result.setSuccess(true);
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
      }
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new TestStateTransitionMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return _msgType;
    }

    @Override
    public List<String> getMessageTypes() {
      return ImmutableList.of(_msgType);
    }

    @Override
    public void reset() {

    }
  }

  @Test()
  public void testNormalMsgExecution() throws InterruptedException {
    System.out.println("START TestCMTaskExecutor.testNormalMsgExecution()");
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }

    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
    for (String type : factory2.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory2);
    }

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      msgList.add(msg);
    }

    int nMsgs2 = 6;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg = new Message(factory2.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      msgList.add(msg);
    }
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
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
  public void testDuplicatedMessage() throws InterruptedException {
    System.out.println("START TestHelixTaskExecutor.testDuplicatedMessage()");
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();
    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    TestStateTransitionHandlerFactory stateTransitionFactory =
        new TestStateTransitionHandlerFactory(Message.MessageType.STATE_TRANSITION.name(), 1000);
    executor.registerMessageHandlerFactory(Message.MessageType.STATE_TRANSITION.name(),
        stateTransitionFactory);

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs = 3;
    String instanceName = manager.getInstanceName();
    for (int i = 0; i < nMsgs; i++) {
      Message msg =
          new Message(Message.MessageType.STATE_TRANSITION.name(), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setCreateTimeStamp((long) i);
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setPartitionName("Partition");
      msg.setResourceName("Resource");
      msg.setStateModelDef("DummyMasterSlave");
      msg.setFromState("SLAVE");
      msg.setToState("MASTER");
      dataAccessor.setProperty(msg.getKey(keyBuilder, instanceName), msg);
      msgList.add(msg);
    }

    AssertJUnit
        .assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName)).size(), nMsgs);

    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage(instanceName, msgList, changeContext);

    Thread.sleep(200);

    // only 1 message is left over - state transition takes 1sec
    Assert.assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName)).size(), 1);

    // While a state transition message is going on, another state transition message for same
    // resource / partition comes in, it should be discarded by message handler

    // Mock accessor is modifying message state in memory so we set it back to NEW
    msgList.get(2).setMsgState(MessageState.NEW);
    dataAccessor.setProperty(msgList.get(2).getKey(keyBuilder, instanceName), msgList.get(2));
    executor.onMessage(instanceName, Arrays.asList(msgList.get(2)), changeContext);
    Thread.sleep(200);
    Assert.assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName)).size(), 1);

    Thread.sleep(1000);
    Assert.assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName)).size(), 0);
    System.out.println("END TestHelixTaskExecutor.testDuplicatedMessage()");
  }

  @Test()
  public void testUnknownTypeMsgExecution() throws InterruptedException {
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }

    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }

    int nMsgs2 = 4;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg = new Message(factory2.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage("someInstance", msgList, changeContext);

    Thread.sleep(1000);

    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory2._processedMsgIds.size() == 0);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1);
    AssertJUnit.assertTrue(factory2._handlersCreated == 0);

    for (Message message : msgList) {
      if (factory.getMessageTypes().contains(message.getMsgType())) {
        AssertJUnit.assertTrue(factory._processedMsgIds.containsKey(message.getId()));
      }
    }
  }

  @Test()
  public void testMsgSessionId() throws InterruptedException {
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }

    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
    for (String type : factory2.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory2);
    }

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msg.setTgtName("");
      msgList.add(msg);
    }

    int nMsgs2 = 4;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg = new Message(factory2.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId("some other session id");
      msg.setTgtName("");
      msgList.add(msg);
    }
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage("someInstance", msgList, changeContext);

    Thread.sleep(1000);

    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory2._processedMsgIds.size() == 0);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1);
    AssertJUnit.assertTrue(factory2._handlersCreated == 0);

    for (Message message : msgList) {
      if (factory.getMessageTypes().contains(message.getMsgType())) {
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
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }

    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      msgList.add(msg);
    }
    Message exceptionMsg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
    exceptionMsg.setTgtSessionId(manager.getSessionId());
    exceptionMsg.setMsgSubType("EXCEPTION");
    exceptionMsg.setTgtName("Localhost_1123");
    exceptionMsg.setSrcName("127.101.1.23_2234");
    exceptionMsg.setCorrelationId(UUID.randomUUID().toString());
    msgList.add(exceptionMsg);

    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
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
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }
    NotificationContext changeContext = new NotificationContext(manager);
    List<Message> msgList = new ArrayList<Message>();

    int nMsgs1 = 0;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }

    List<Message> msgListToCancel = new ArrayList<Message>();
    int nMsgs2 = 4;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msgList.add(msg);
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgListToCancel.add(msg);
    }
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
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
      if (factory.getMessageTypes().contains(message.getMsgType())) {
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
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }
    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
    for (String type : factory2.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory2);
    }
    CancellableHandlerFactory factory3 = new CancellableHandlerFactory();
    for (String type : factory3.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory3);
    }

    int nMsg1 = 10, nMsg2 = 10, nMsg3 = 10;
    List<Message> msgList = new ArrayList<Message>();

    for (int i = 0; i < nMsg1; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }

    for (int i = 0; i < nMsg2; i++) {
      Message msg = new Message(factory2.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msgList.add(msg);
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }

    for (int i = 0; i < nMsg3; i++) {
      Message msg = new Message(factory3.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msgList.add(msg);
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg);
    }
    NotificationContext changeContext = new NotificationContext(manager);
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
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
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }
    NotificationContext changeContext = new NotificationContext(manager);

    List<Message> msgList = new ArrayList<Message>();
    int nMsgs2 = 4;
    // Test the case in which retry = 0
    for (int i = 0; i < nMsgs2; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setExecutionTimeout((i + 1) * 600);
      msgList.add(msg);
    }
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage("someInstance", msgList, changeContext);

    Thread.sleep(4000);

    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs2);
    AssertJUnit.assertEquals(factory._timedOutMsgIds.size(), 2);
    // AssertJUnit.assertFalse(msgList.get(0).getRecord().getSimpleFields().containsKey("TimeOut"));
    for (int i = 0; i < nMsgs2 - 2; i++) {
      if (factory.getMessageTypes().contains(msgList.get(i).getMsgType())) {
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
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }
    NotificationContext changeContext = new NotificationContext(manager);

    List<Message> msgList = new ArrayList<Message>();

    // factory.reset();
    // msgList.clear();
    // Test the case that the message are executed for the second time
    int nMsgs2 = 4;
    for (int i = 0; i < nMsgs2; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setExecutionTimeout((i + 1) * 600);
      msg.setRetryCount(1);
      msgList.add(msg);
    }
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage("someInstance", msgList, changeContext);
    Thread.sleep(3500);
    AssertJUnit.assertEquals(factory._processedMsgIds.size(), 3);
    AssertJUnit.assertTrue(msgList.get(0).getRecord().getSimpleField("Cancelcount").equals("2"));
    AssertJUnit.assertTrue(msgList.get(1).getRecord().getSimpleField("Cancelcount").equals("1"));
    AssertJUnit.assertEquals(factory._timedOutMsgIds.size(), 2);
    AssertJUnit.assertTrue(executor._taskMap.size() == 0);

  }

  @Test
  public void testStateTransitionCancellationMsg() throws InterruptedException {
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestStateTransitionHandlerFactory stateTransitionFactory = new TestStateTransitionHandlerFactory(Message.MessageType.STATE_TRANSITION.name());
    TestStateTransitionHandlerFactory cancelFactory = new TestStateTransitionHandlerFactory(Message.MessageType.STATE_TRANSITION_CANCELLATION
        .name());
    executor.registerMessageHandlerFactory(Message.MessageType.STATE_TRANSITION.name(), stateTransitionFactory);
    executor.registerMessageHandlerFactory(Message.MessageType.STATE_TRANSITION_CANCELLATION.name(), cancelFactory);


    NotificationContext changeContext = new NotificationContext(manager);

    List<Message> msgList = new ArrayList<Message>();
    Message msg1 = new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
    msg1.setTgtSessionId("*");
    msg1.setPartitionName("P1");
    msg1.setResourceName("R1");
    msg1.setTgtName("Localhost_1123");
    msg1.setSrcName("127.101.1.23_2234");
    msg1.setFromState("SLAVE");
    msg1.setToState("MASTER");
    msgList.add(msg1);

    Message msg2 = new Message(Message.MessageType.STATE_TRANSITION_CANCELLATION, UUID.randomUUID().toString());
    msg2.setTgtSessionId("*");
    msg2.setPartitionName("P1");
    msg2.setResourceName("R1");
    msg2.setTgtName("Localhost_1123");
    msg2.setSrcName("127.101.1.23_2234");
    msg2.setFromState("SLAVE");
    msg2.setToState("MASTER");
    msgList.add(msg2);

    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage("someInstance", msgList, changeContext);

    Thread.sleep(3000);
    AssertJUnit.assertEquals(cancelFactory._processedMsgIds.size(), 0);
    AssertJUnit.assertEquals(stateTransitionFactory._processedMsgIds.size(), 0);
  }

  @Test
  public void testMessageReadOptimization() throws InterruptedException {
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }

    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    List<String> messageIds = new ArrayList<>();
    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      accessor.setProperty(keyBuilder.message("someInstance", msg.getId()), msg);
      messageIds.add(msg.getId());
    }

    NotificationContext changeContext = new NotificationContext(manager);
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);

    // Simulate read message already, then processing message. Should read and handle no message.
    executor._knownMessageIds.addAll(messageIds);
    executor.onMessage("someInstance", Collections.EMPTY_LIST, changeContext);
    Thread.sleep(3000);
    AssertJUnit.assertEquals(0, factory._processedMsgIds.size());
    executor._knownMessageIds.clear();

    // Processing message normally
    executor.onMessage("someInstance", Collections.EMPTY_LIST, changeContext);
    Thread.sleep(3000);
    AssertJUnit.assertEquals(nMsgs1, factory._processedMsgIds.size());
    // After all messages are processed, _knownMessageIds should be empty.
    Assert.assertTrue(executor._knownMessageIds.isEmpty());
  }
}
