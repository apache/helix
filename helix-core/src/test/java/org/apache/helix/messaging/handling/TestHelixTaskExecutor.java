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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.mock.MockManager;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestHelixTaskExecutor {
  @BeforeClass
  public void beforeClass() {
    System.out.println("START " + TestHelper.getTestClassName());
  }

  @AfterClass
  public void afterClass() {
    System.out.println("End " + TestHelper.getTestClassName());
  }

  public static class MockClusterManager extends MockManager {
    @Override
    public String getSessionId() {
      return "123";
    }
  }

  class TestMessageHandlerFactory implements MultiTypeMessageHandlerFactory {
    final int _messageDelay;
    int _handlersCreated = 0;
    ConcurrentHashMap<String, String> _processedMsgIds = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> _completedMsgIds = new ConcurrentSkipListSet<>();

    TestMessageHandlerFactory(int messageDelay) {
      _messageDelay = messageDelay;
    }

    TestMessageHandlerFactory() {
      _messageDelay = 100;
    }

    class TestMessageHandler extends MessageHandler {
      public TestMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        HelixTaskResult result = new HelixTaskResult();
        _processedMsgIds.put(_message.getMsgId(), _message.getMsgId());
        Thread.sleep(_messageDelay);
        result.setSuccess(true);
        _completedMsgIds.add(_message.getMsgId());
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {

      }
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      if (message.getMsgSubType() != null && message.getMsgSubType().equals("EXCEPTION")) {
        throw new HelixException("Test Message handler exception, can ignore");
      }
      _handlersCreated++;
      return new TestMessageHandler(message, context);
    }

    @Override
    public List<String> getMessageTypes() {
      return Collections.singletonList("TestingMessageHandler");
    }

    @Override
    public void reset() {

    }
  }

  class TestMessageHandlerFactory2 extends TestMessageHandlerFactory {
    @Override
    public List<String> getMessageTypes() {
      return ImmutableList.of("TestingMessageHandler2");
    }
  }

  private class TestMessageHandlerFactory3 extends TestMessageHandlerFactory {
    private boolean _resetDone = false;

    @Override
    public List<String> getMessageTypes() {
      return ImmutableList.of("msgType1", "msgType2", "msgType3");
    }

    @Override
    public void reset() {
      Assert.assertFalse(_resetDone, "reset() should only be triggered once in TestMessageHandlerFactory3");
      _resetDone = true;
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
        _message.getRecord().setSimpleField("exception", e.getMessage());
      }
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      _handlersCreated++;
      return new CancellableHandler(message, context);
    }

    @Override public List<String> getMessageTypes() {
      return ImmutableList.of("Cancellable");
    }

    @Override
    public void reset() {
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

    class TestStateTransitionMessageHandler extends HelixStateTransitionHandler {

      public TestStateTransitionMessageHandler(Message message, NotificationContext context,
          CurrentState currentStateDelta) {
        super(new StateModelFactory<StateModel>() {
          // Empty no-op state model factory is good enough for the test.
        }, new StateModel() {
          // Empty no-op state model is good enough for the test.
        }, message, context, currentStateDelta);
      }

      @Override
      public HelixTaskResult handleMessage() {
        HelixTaskResult result = new HelixTaskResult();
        _processedMsgIds.put(_message.getMsgId(), _message.getMsgId());
        if (_delay > 0) {
          System.out.println("Sleeping..." + _delay);
          try {
            Thread.sleep(_delay);
          } catch (Exception e) {
            assert (false);
          }
        }
        result.setSuccess(true);
        return result;
      }

      @Override
      public StaleMessageValidateResult staleMessageValidator() {
        return super.staleMessageValidator();
      }
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      CurrentState currentStateDelta = new CurrentState(message.getResourceName());
      currentStateDelta.setSessionId(message.getTgtSessionId());
      currentStateDelta.setStateModelDefRef(message.getStateModelDef());
      currentStateDelta.setStateModelFactoryName(message.getStateModelFactoryName());
      currentStateDelta.setBucketSize(message.getBucketSize());
      if (!message.getResourceName().equals("testStaledMessageResource")) {
        // set the current state same as from state in the message in test testStaledMessage.
        currentStateDelta.setState(message.getPartitionName(), "SLAVE");
      } else {
        // set the current state same as to state in the message in test testStaledMessage.
        currentStateDelta.setState(message.getPartitionName(), "MASTER");
      }
      return new TestStateTransitionMessageHandler(message, context, currentStateDelta);
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
        .assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName), true).size(),
            nMsgs);

    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage(instanceName, msgList, changeContext);

    Thread.sleep(200);

    // only 1 message is left over - state transition takes 1sec
    Assert.assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName), true).size(),
        1);

    // While a state transition message is going on, another state transition message for same
    // resource / partition comes in, it should be discarded by message handler

    // Mock accessor is modifying message state in memory so we set it back to NEW
    msgList.get(2).setMsgState(MessageState.NEW);
    dataAccessor.setProperty(msgList.get(2).getKey(keyBuilder, instanceName), msgList.get(2));
    executor.onMessage(instanceName, Arrays.asList(msgList.get(2)), changeContext);
    Thread.sleep(200);
    Assert.assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName), true).size(),
        1);

    Thread.sleep(1000);
    Assert.assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName), true).size(),
        0);
    System.out.println("END TestHelixTaskExecutor.testDuplicatedMessage()");
  }

  @Test()
  public void testStaledMessage() throws InterruptedException {
    System.out.println("START TestHelixTaskExecutor.testStaledMessage()");
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

    int nMsgs = 1;
    String instanceName = manager.getInstanceName();
    for (int i = 0; i < nMsgs; i++) {
      Message msg =
          new Message(Message.MessageType.STATE_TRANSITION.name(), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setCreateTimeStamp((long) i);
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setPartitionName("Partition");
      msg.setResourceName("testStaledMessageResource");
      msg.setStateModelDef("DummyMasterSlave");
      msg.setFromState("SLAVE");
      msg.setToState("MASTER");
      dataAccessor.setProperty(msg.getKey(keyBuilder, instanceName), msg);
      msgList.add(msg);
    }

    Assert.assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName), true).size(),
        nMsgs);

    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage(instanceName, msgList, changeContext);

    Thread.sleep(200);

    // The message should be ignored since toState is the same as current state.
    Assert.assertEquals(dataAccessor.getChildValues(keyBuilder.messages(instanceName), true).size(),
        0);

    System.out.println("END TestHelixTaskExecutor.testStaledMessage()");
  }

  @Test()
  public void testUnknownTypeMsgExecution() throws InterruptedException {
    System.out.println("START " + TestHelper.getTestMethodName());
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
    System.out.println("END " + TestHelper.getTestMethodName());
  }

  @Test()
  public void testMsgSessionId() throws InterruptedException {
    System.out.println("START " + TestHelper.getTestMethodName());
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
    System.out.println("END " + TestHelper.getTestMethodName());
  }

  @Test()
  public void testCreateHandlerException() throws Exception {
    System.out.println("START TestCMTaskExecutor.testCreateHandlerException()");
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();
    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    NotificationContext changeContext = new NotificationContext(manager);
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();

    // Sending message without registering the factory.
    // The message won't be processed since creating handler returns null.
    int nMsgs1 = 5;
    List<Message> msgList = new ArrayList<>();
    for (int i = 0; i < nMsgs1; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      dataAccessor.setProperty(keyBuilder.message(manager.getInstanceName(), msg.getMsgId()), msg);
      msgList.add(msg);
    }

    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage(manager.getInstanceName(), Collections.emptyList(), changeContext);

    for (Message message : msgList) {
      message = dataAccessor
          .getProperty(keyBuilder.message(manager.getInstanceName(), message.getMsgId()));
      Assert.assertNotNull(message);
      Assert.assertEquals(message.getMsgState(), MessageState.NEW);
      Assert.assertEquals(message.getRetryCount(), 0);
    }

    // Test with a factory that throws Exception on certain message. The invalid message will be
    // remain UNPROCESSABLE due to the Exception.
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }

    Message exceptionMsg =
        new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
    exceptionMsg.setTgtSessionId(manager.getSessionId());
    exceptionMsg.setMsgSubType("EXCEPTION");
    exceptionMsg.setTgtName("Localhost_1123");
    exceptionMsg.setSrcName("127.101.1.23_2234");
    exceptionMsg.setCorrelationId(UUID.randomUUID().toString());
    dataAccessor.setProperty(keyBuilder.message(manager.getInstanceName(), exceptionMsg.getMsgId()),
        exceptionMsg);

    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage(manager.getInstanceName(), Collections.emptyList(), changeContext);

    Assert.assertTrue(TestHelper.verify(() -> {
          Message tmpExceptionMsg = dataAccessor
              .getProperty(keyBuilder.message(manager.getInstanceName(), exceptionMsg.getMsgId()));
          if (tmpExceptionMsg == null || !tmpExceptionMsg.getMsgState()
              .equals(MessageState.UNPROCESSABLE) || tmpExceptionMsg.getRetryCount() != -1) {
            return false;
          }
          return true;
        }, TestHelper.WAIT_DURATION),
        "The exception message should be retied once and in UNPROCESSABLE state.");

    Assert.assertTrue(TestHelper.verify(() -> {
      for (Message message : msgList) {
        message = dataAccessor
            .getProperty(keyBuilder.message(manager.getInstanceName(), message.getMsgId()));
        if (message != null) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION), "The normal messages should be all processed normally.");
    Assert.assertEquals(factory._processedMsgIds.size(), nMsgs1);
    Assert.assertEquals(factory._handlersCreated, nMsgs1);

    System.out.println("END TestCMTaskExecutor.testCreateHandlerException()");
  }

  @Test()
  public void testTaskCancellation() throws InterruptedException {
    System.out.println("START " + TestHelper.getTestMethodName());
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
    System.out.println("END " + TestHelper.getTestMethodName());
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

  @Test(dependsOnMethods = "testShutdown")
  public void testHandlerResetTimeout() throws Exception {
    System.out.println("START TestCMTaskExecutor.testHandlerResetTimeout()");
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    int messageDelay = 2 * 1000; // 2 seconds
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory(messageDelay);

    // Execute a message with short reset timeout
    int shortTimeout = 100; // 100 ms
    executor.registerMessageHandlerFactory(factory, HelixTaskExecutor.DEFAULT_PARALLEL_TASKS, shortTimeout);

    final Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
    msg.setTgtSessionId("*");
    msg.setTgtName("Localhost_1123");
    msg.setSrcName("127.101.1.23_2234");

    NotificationContext changeContext = new NotificationContext(manager);
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage("some", Arrays.asList(msg), changeContext);
    Assert.assertTrue(
        TestHelper.verify(() -> factory._processedMsgIds.containsKey(msg.getMsgId()), TestHelper.WAIT_DURATION));
    executor.shutdown();
    for (ExecutorService svc : executor._executorMap.values()) {
      Assert.assertTrue(svc.isShutdown());
    }
    Assert.assertEquals(factory._completedMsgIds.size(), 0);

    // Execute a message with proper reset timeout, so it will wait enough time until the message is processed.
    executor = new HelixTaskExecutor();
    int longTimeout = messageDelay * 2; // 4 seconds
    executor.registerMessageHandlerFactory(factory, HelixTaskExecutor.DEFAULT_PARALLEL_TASKS, longTimeout);

    final Message msg2 = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
    msg2.setTgtSessionId("*");
    msg2.setTgtName("Localhost_1123");
    msg2.setSrcName("127.101.1.23_2234");
    executor.onMessage("some", Arrays.asList(msg2), changeContext);

    Assert.assertTrue(
        TestHelper.verify(() -> factory._processedMsgIds.containsKey(msg2.getMsgId()), TestHelper.WAIT_DURATION));
    executor.shutdown();
    for (ExecutorService svc : executor._executorMap.values()) {
      Assert.assertTrue(svc.isShutdown());
    }
    Assert.assertEquals(factory._completedMsgIds.size(), 1);
    Assert.assertTrue(factory._completedMsgIds.contains(msg2.getMsgId()));

    System.out.println("END TestCMTaskExecutor.testHandlerResetTimeout()");
  }

  @Test
  public void testMsgHandlerRegistryAndShutdown() {
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    TestMessageHandlerFactory3 factoryMulti = new TestMessageHandlerFactory3();
    executor.registerMessageHandlerFactory(factory, HelixTaskExecutor.DEFAULT_PARALLEL_TASKS, 200);
    executor.registerMessageHandlerFactory(factoryMulti, HelixTaskExecutor.DEFAULT_PARALLEL_TASKS, 200);

    final Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
    msg.setTgtSessionId("*");
    msg.setTgtName("Localhost_1123");
    msg.setSrcName("127.101.1.23_2234");

    NotificationContext changeContext = new NotificationContext(manager);
    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage("some", Collections.singletonList(msg), changeContext);
    Assert.assertEquals(executor._hdlrFtyRegistry.size(), 4);
    // Ensure TestMessageHandlerFactory3 instance is reset and reset exactly once
    executor.shutdown();
    Assert.assertTrue(factoryMulti._resetDone, "TestMessageHandlerFactory3 should be reset");
  }

  @Test()
  public void testNoRetry() throws InterruptedException {
    System.out.println("START " + TestHelper.getTestMethodName());
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
    System.out.println("END " + TestHelper.getTestMethodName());
  }

  @Test()
  public void testRetryOnce() throws InterruptedException {
    System.out.println("START " + TestHelper.getTestMethodName());
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();

    CancellableHandlerFactory factory = new CancellableHandlerFactory();
    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }
    NotificationContext changeContext = new NotificationContext(manager);

    List<Message> msgList = new ArrayList<Message>();

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
    System.out.println("END " + TestHelper.getTestMethodName());
  }

  @Test
  public void testStateTransitionCancellationMsg() throws InterruptedException {
    System.out.println("START " + TestHelper.getTestMethodName());
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
    System.out.println("END " + TestHelper.getTestMethodName());
  }

  @Test
  public void testMessageReadOptimization() throws InterruptedException {
    System.out.println("START " + TestHelper.getTestMethodName());
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
    System.out.println("END " + TestHelper.getTestMethodName());
  }

  @Test
  public void testNoWriteReadStateForRemovedMessage()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    System.out.println("START " + TestHelper.getTestMethodName());
    HelixTaskExecutor executor = new HelixTaskExecutor();
    HelixManager manager = new MockClusterManager();
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();

    for (String type : factory.getMessageTypes()) {
      executor.registerMessageHandlerFactory(type, factory);
    }

    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    String instanceName = "someInstance";

    List<String> messageIds = new ArrayList<>();
    List<Message> messages = new ArrayList<>();
    int nMsgs1 = 5;
    for (int i = 0; i < nMsgs1; i++) {
      Message msg = new Message(factory.getMessageTypes().get(0), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      accessor.setProperty(keyBuilder.message(instanceName, msg.getId()), msg);

      messageIds.add(msg.getId());
      // Set for testing the update operation later
      msg.setMsgState(MessageState.READ);
      messages.add(msg);
    }

    Method updateMessageState = HelixTaskExecutor.class
        .getDeclaredMethod("updateMessageState", Collection.class, HelixDataAccessor.class,
            String.class);
    updateMessageState.setAccessible(true);

    updateMessageState.invoke(executor, messages, accessor, instanceName);
    Assert.assertEquals(accessor.getChildNames(keyBuilder.messages(instanceName)).size(), nMsgs1);

    accessor.removeProperty(keyBuilder.message(instanceName, messageIds.get(0)));
    System.out.println(accessor.getChildNames(keyBuilder.messages(instanceName)).size());

    for (Message message : messages) {
      // Mock a change to ensure there will be some delta on the message node after update
      message.setCorrelationId(UUID.randomUUID().toString());
    }
    updateMessageState.invoke(executor, messages, accessor, instanceName);
    Assert
        .assertEquals(accessor.getChildNames(keyBuilder.messages(instanceName)).size(), nMsgs1 - 1);
    System.out.println("END " + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testStateTransitionCancellationMsg")
  public void testStateTransitionMsgScheduleFailure() {
    System.out.println("START " + TestHelper.getTestMethodName());

    // Create a mock executor that fails the task scheduling.
    HelixTaskExecutor executor = new HelixTaskExecutor() {
      @Override
      public boolean scheduleTask(MessageTask task) {
        return false;
      }
    };
    HelixManager manager = new MockClusterManager();
    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    TestStateTransitionHandlerFactory stateTransitionFactory =
        new TestStateTransitionHandlerFactory(Message.MessageType.STATE_TRANSITION.name());
    executor.registerMessageHandlerFactory(Message.MessageType.STATE_TRANSITION.name(),
        stateTransitionFactory);

    NotificationContext changeContext = new NotificationContext(manager);

    Message msg = new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
    msg.setTgtSessionId(manager.getSessionId());
    msg.setPartitionName("P1");
    msg.setResourceName("R1");
    msg.setTgtName("Localhost_1123");
    msg.setSrcName("127.101.1.23_2234");
    msg.setFromState("SLAVE");
    msg.setToState("MASTER");
    dataAccessor.setProperty(keyBuilder.message(manager.getInstanceName(), msg.getMsgId()), msg);

    changeContext.setChangeType(HelixConstants.ChangeType.MESSAGE);
    executor.onMessage(manager.getInstanceName(), Collections.emptyList(), changeContext);

    Assert.assertEquals(stateTransitionFactory._processedMsgIds.size(), 0);
    // Message should have been removed
    Assert.assertNull(
        dataAccessor.getProperty(keyBuilder.message(manager.getInstanceName(), msg.getMsgId())));
    // Current state would be ERROR due to the failure of task scheduling.
    CurrentState currentState = dataAccessor.getProperty(keyBuilder
        .currentState(manager.getInstanceName(), manager.getSessionId(), msg.getResourceName()));
    Assert.assertNotNull(currentState);
    Assert.assertEquals(currentState.getState(msg.getPartitionName()),
        HelixDefinedState.ERROR.toString());
    System.out.println("END " + TestHelper.getTestMethodName());
  }
}
