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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StatusUpdate;
import org.apache.helix.monitoring.ZKPathDataDumpTask;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchedulerMessage extends ZkStandAloneCMTestBase {

  class MockAsyncCallback extends AsyncCallback {
    Message _message;

    public MockAsyncCallback() {
    }

    @Override
    public void onTimeOut() {
      // TODO Auto-generated method stub

    }

    @Override
    public void onReplyMessage(Message message) {
      _message = message;
    }
  }

  final TestMessagingHandlerFactory _factory = new TestMessagingHandlerFactory();

  public static class TestMessagingHandlerFactory implements MessageHandlerFactory {
    public Map<String, Set<String>> _results = new ConcurrentHashMap<String, Set<String>>();

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new TestMessagingHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return "TestParticipant";
    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub

    }

    public class TestMessagingHandler extends MessageHandler {
      public TestMessagingHandler(Message message, NotificationContext context) {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        HelixTaskResult result = new HelixTaskResult();
        result.setSuccess(true);
        String partitionName = _message.getPartitionName();
        result.getTaskResultMap().put("Message", _message.getMsgId());
        synchronized (_results) {
          if (!_results.containsKey(partitionName)) {
            _results.put(partitionName, new HashSet<String>());
          }
          _results.get(partitionName).add(_message.getMsgId());
        }
        // System.err.println("handle msg: " + _message.getPartitionName() + ", from: "
        // + _message.getFromState() + ", to: " + _message.getToState());
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        // TODO Auto-generated method stub
      }
    }
  }

  public static class TestMessagingHandlerFactoryLatch implements MessageHandlerFactory {
    public volatile CountDownLatch _latch = new CountDownLatch(1);
    public int _messageCount = 0;
    public Map<String, Set<String>> _results = new ConcurrentHashMap<String, Set<String>>();

    @Override
    public synchronized MessageHandler createHandler(Message message, NotificationContext context) {
      _messageCount++;
      return new TestMessagingHandlerLatch(message, context);
    }

    public synchronized void signal() {
      _latch.countDown();
      _latch = new CountDownLatch(1);
    }

    @Override
    public String getMessageType() {
      return "TestMessagingHandlerLatch";
    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub
    }

    public class TestMessagingHandlerLatch extends MessageHandler {
      public TestMessagingHandlerLatch(Message message, NotificationContext context) {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        _latch.await();
        HelixTaskResult result = new HelixTaskResult();
        result.setSuccess(true);
        result.getTaskResultMap().put("Message", _message.getMsgId());
        String destName = _message.getTgtName();
        synchronized (_results) {
          if (!_results.containsKey(_message.getPartitionName())) {
            _results.put(_message.getPartitionName(), new ConcurrentSkipListSet<String>());
          }
        }
        _results.get(_message.getPartitionName()).add(destName);
        // System.err.println("Message " + _message.getMsgId() + " executed");
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        // TODO Auto-generated method stub
      }
    }
  }

  @Test()
  public void testSchedulerMsgUsingQueue() throws Exception {
    Logger.getRootLogger().setLevel(Level.INFO);
    _factory._results.clear();
    Thread.sleep(2000);
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);

      manager = _participants[i]; // _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");
    schedulerMessage.getRecord().setSimpleField(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, "TestSchedulerMsg");
    // Template for the individual message sent to each participant
    Message msg = new Message(_factory.getMessageType(), "Template");
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);

    // Criteria to send individual messages
    Criteria cr = new Criteria();
    cr.setInstanceName("localhost_%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setResource("%");
    cr.setPartition("%");

    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, cr);

    String crString = sw.toString();

    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    schedulerMessage.getRecord().setMapField("MessageTemplate", msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    helixDataAccessor.createProperty(keyBuilder.controllerMessage(schedulerMessage.getMsgId()),
        schedulerMessage);

    for (int i = 0; i < 30; i++) {
      Thread.sleep(2000);
      if (_PARTITIONS == _factory._results.size()) {
        break;
      }
    }

    Assert.assertEquals(_PARTITIONS, _factory._results.size());
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(),
            schedulerMessage.getMsgId());

    int messageResultCount = 0;
    for (int i = 0; i < 10; i++) {
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount")
          .equals("" + (_PARTITIONS * 3)));
      for (String key : statusUpdate.getMapFields().keySet()) {
        if (key.startsWith("MessageResult ")) {
          messageResultCount++;
        }
      }
      if (messageResultCount == _PARTITIONS * 3) {
        break;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.assertEquals(messageResultCount, _PARTITIONS * 3);
    int count = 0;
    for (Set<String> val : _factory._results.values()) {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS * 3);

  }

  @Test()
  public void testSchedulerMsg() throws Exception {
    Logger.getRootLogger().setLevel(Level.INFO);
    _factory._results.clear();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);

      manager = _participants[i]; // _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");
    // schedulerMessage.getRecord().setSimpleField(DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE,
    // "TestSchedulerMsg");
    // Template for the individual message sent to each participant
    Message msg = new Message(_factory.getMessageType(), "Template");
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);

    // Criteria to send individual messages
    Criteria cr = new Criteria();
    cr.setInstanceName("localhost_%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setResource("%");
    cr.setPartition("%");

    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, cr);

    String crString = sw.toString();

    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    schedulerMessage.getRecord().setMapField("MessageTemplate", msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    helixDataAccessor.createProperty(keyBuilder.controllerMessage(schedulerMessage.getMsgId()),
        schedulerMessage);

    for (int i = 0; i < 30; i++) {
      Thread.sleep(2000);
      if (_PARTITIONS == _factory._results.size()) {
        break;
      }
    }

    Assert.assertEquals(_PARTITIONS, _factory._results.size());
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(),
            schedulerMessage.getMsgId());

    int messageResultCount = 0;
    for (int i = 0; i < 10; i++) {
      Thread.sleep(1000);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount")
          .equals("" + (_PARTITIONS * 3)));
      for (String key : statusUpdate.getMapFields().keySet()) {
        if (key.startsWith("MessageResult ")) {
          messageResultCount++;
          Assert.assertTrue(statusUpdate.getMapField(key).size() > 1);
        }
      }
      if (messageResultCount == _PARTITIONS * 3) {
        break;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.assertEquals(messageResultCount, _PARTITIONS * 3);
    int count = 0;
    for (Set<String> val : _factory._results.values()) {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS * 3);

    // test the ZkPathDataDumpTask
    String controllerStatusPath =
        HelixUtil.getControllerPropertyPath(manager.getClusterName(),
            PropertyType.STATUSUPDATES_CONTROLLER);
    List<String> subPaths = _zkclient.getChildren(controllerStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for (String subPath : subPaths) {
      String nextPath = controllerStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkclient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() > 0);
    }

    String instanceStatusPath =
        HelixUtil.getInstancePropertyPath(manager.getClusterName(), "localhost_" + (START_PORT),
            PropertyType.STATUSUPDATES);

    subPaths = _zkclient.getChildren(instanceStatusPath);
    Assert.assertTrue(subPaths.size() == 0);
    for (String subPath : subPaths) {
      String nextPath = instanceStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkclient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() > 0);
      for (String subsubPath : subsubPaths) {
        String nextnextPath = nextPath + "/" + subsubPath;
        Assert.assertTrue(_zkclient.getChildren(nextnextPath).size() > 0);
      }
    }
    Thread.sleep(3000);
    ZKPathDataDumpTask dumpTask = new ZKPathDataDumpTask(manager, 0L, 0L, Integer.MAX_VALUE);
    dumpTask.run();

    subPaths = _zkclient.getChildren(controllerStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for (String subPath : subPaths) {
      String nextPath = controllerStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkclient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() == 0);
    }

    subPaths = _zkclient.getChildren(instanceStatusPath);
    Assert.assertTrue(subPaths.size() == 0);
    for (String subPath : subPaths) {
      String nextPath = instanceStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkclient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() > 0);
      for (String subsubPath : subsubPaths) {
        String nextnextPath = nextPath + "/" + subsubPath;
        Assert.assertTrue(_zkclient.getChildren(nextnextPath).size() == 0);
      }
    }
  }

  @Test()
  public void testSchedulerMsg2() throws Exception {
    _factory._results.clear();
    Thread.sleep(2000);
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);

      manager = _participants[i]; // _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msg = new Message(_factory.getMessageType(), "Template");
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);

    // Criteria to send individual messages
    Criteria cr = new Criteria();
    cr.setInstanceName("localhost_%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setResource("%");
    cr.setPartition("%");

    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, cr);

    String crString = sw.toString();

    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    schedulerMessage.getRecord().setMapField("MessageTemplate", msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");
    schedulerMessage.getRecord().setSimpleField("WAIT_ALL", "true");

    Criteria cr2 = new Criteria();
    cr2.setRecipientInstanceType(InstanceType.CONTROLLER);
    cr2.setInstanceName("*");
    cr2.setSessionSpecific(false);

    schedulerMessage.getRecord().setSimpleField(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, "TestSchedulerMsg2");
    MockAsyncCallback callback = new MockAsyncCallback();
    manager.getMessagingService().sendAndWait(cr2, schedulerMessage, callback, -1);
    String msgId =
        callback._message.getResultMap()
            .get(DefaultSchedulerMessageHandlerFactory.SCHEDULER_MSG_ID);

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    for (int i = 0; i < 10; i++) {
      Thread.sleep(200);
      PropertyKey controllerTaskStatus =
          keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      if (statusUpdate.getMapFields().containsKey("Summary")) {
        break;
      }
    }

    Assert.assertEquals(_PARTITIONS, _factory._results.size());
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
    ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
    Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount")
        .equals("" + (_PARTITIONS * 3)));
    int messageResultCount = 0;
    for (String key : statusUpdate.getMapFields().keySet()) {
      if (key.startsWith("MessageResult ")) {
        messageResultCount++;
      }
    }
    Assert.assertEquals(messageResultCount, _PARTITIONS * 3);

    int count = 0;
    for (Set<String> val : _factory._results.values()) {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS * 3);
  }

  @Test()
  public void testSchedulerZeroMsg() throws Exception {
    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);

      manager = _participants[i]; // _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msg = new Message(factory.getMessageType(), "Template");
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);

    // Criteria to send individual messages
    Criteria cr = new Criteria();
    cr.setInstanceName("localhost_DOESNOTEXIST");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setResource("%");
    cr.setPartition("%");

    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, cr);

    String crString = sw.toString();

    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    schedulerMessage.getRecord().setMapField("MessageTemplate", msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    PropertyKey controllerMessageKey = keyBuilder.controllerMessage(schedulerMessage.getMsgId());
    helixDataAccessor.setProperty(controllerMessageKey, schedulerMessage);

    Thread.sleep(3000);

    Assert.assertEquals(0, factory._results.size());
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(),
            schedulerMessage.getMsgId());
    for (int i = 0; i < 10; i++) {
      StatusUpdate update = helixDataAccessor.getProperty(controllerTaskStatus);
      if (update == null || update.getRecord().getMapField("SentMessageCount") == null) {
        Thread.sleep(1000);
      }
    }
    ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
    Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount").equals("0"));
    int count = 0;
    for (Set<String> val : factory._results.values()) {
      count += val.size();
    }
    Assert.assertEquals(count, 0);
  }

  @Test()
  public void testSchedulerMsg3() throws Exception {
    _factory._results.clear();
    Thread.sleep(2000);
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);

      // _participants[i].getMessagingService().registerMessageHandlerFactory(
      // _factory.getMessageType(), _factory);

      manager = _participants[i]; // _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msg = new Message(_factory.getMessageType(), "Template");
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);

    // Criteria to send individual messages
    Criteria cr = new Criteria();
    cr.setInstanceName("localhost_%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setResource("%");
    cr.setPartition("%");

    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, cr);

    String crString = sw.toString();

    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    schedulerMessage.getRecord().setMapField("MessageTemplate", msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");
    schedulerMessage.getRecord().setSimpleField("WAIT_ALL", "true");

    schedulerMessage.getRecord().setSimpleField(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, "TestSchedulerMsg3");
    Criteria cr2 = new Criteria();
    cr2.setRecipientInstanceType(InstanceType.CONTROLLER);
    cr2.setInstanceName("*");
    cr2.setSessionSpecific(false);

    MockAsyncCallback callback = new MockAsyncCallback();
    cr.setInstanceName("localhost_%");
    mapper = new ObjectMapper();
    serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    sw = new StringWriter();
    mapper.writeValue(sw, cr);

    crString = sw.toString();
    schedulerMessage.getRecord().setSimpleField("Criteria", crString);

    for (int i = 0; i < 4; i++) {
      callback = new MockAsyncCallback();
      cr.setInstanceName("localhost_" + (START_PORT + i));
      mapper = new ObjectMapper();
      serializationConfig = mapper.getSerializationConfig();
      serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

      sw = new StringWriter();
      mapper.writeValue(sw, cr);
      schedulerMessage.setMsgId(UUID.randomUUID().toString());
      crString = sw.toString();
      schedulerMessage.getRecord().setSimpleField("Criteria", crString);
      manager.getMessagingService().sendAndWait(cr2, schedulerMessage, callback, -1);
      String msgId =
          callback._message.getResultMap().get(
              DefaultSchedulerMessageHandlerFactory.SCHEDULER_MSG_ID);

      HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
      Builder keyBuilder = helixDataAccessor.keyBuilder();

      for (int j = 0; j < 100; j++) {
        Thread.sleep(200);
        PropertyKey controllerTaskStatus =
            keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
        ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
        if (statusUpdate.getMapFields().containsKey("Summary")) {
          break;
        }
      }

      PropertyKey controllerTaskStatus =
          keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount")
          .equals("" + (_PARTITIONS * 3 / 5)));
      int messageResultCount = 0;
      for (String key : statusUpdate.getMapFields().keySet()) {
        if (key.startsWith("MessageResult")) {
          messageResultCount++;
        }
      }
      Assert.assertEquals(messageResultCount, _PARTITIONS * 3 / 5);

      boolean success = false;
      for (int j = 0; j < 6; j++) {
        int count = 0;
        // System.out.println(i);
        for (Set<String> val : _factory._results.values()) {
          // System.out.println(val);
          count += val.size();
        }
        // System.out.println(count);
        // Assert.assertEquals(count, _PARTITIONS * 3 / 5 * (i + 1));
        success = count == _PARTITIONS * 3 / 5 * (i + 1);
        if (success) {
          break;
        }
        Thread.sleep(500);
      }
      Assert.assertTrue(success);
    }
  }

  private int count(TestMessagingHandlerFactory factory) {
    int cnt = 0;
    for (Set<String> val : factory._results.values()) {
      cnt += val.size();
    }
    return cnt;
  }

  @Test()
  public void testSchedulerMsg4() throws Exception {
    _factory._results.clear();
    Thread.sleep(2000);
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);

      // _participants[i].getMessagingService().registerMessageHandlerFactory(
      // _factory.getMessageType(), _factory);

      manager = _participants[i]; // _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msgTemplate = new Message(_factory.getMessageType(), "Template");
    msgTemplate.setTgtSessionId("*");
    msgTemplate.setMsgState(MessageState.NEW);

    // Criteria to send individual messages
    Criteria cr = new Criteria();
    cr.setInstanceName("localhost_%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setResource("TestDB");
    cr.setPartition("%");

    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, cr);

    String crString = sw.toString();

    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    schedulerMessage.getRecord().setMapField("MessageTemplate",
        msgTemplate.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");
    schedulerMessage.getRecord().setSimpleField("WAIT_ALL", "true");

    schedulerMessage.getRecord().setSimpleField(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, "TestSchedulerMsg4");
    Criteria cr2 = new Criteria();
    cr2.setRecipientInstanceType(InstanceType.CONTROLLER);
    cr2.setInstanceName("*");
    cr2.setSessionSpecific(false);

    Map<String, String> constraints = new TreeMap<String, String>();
    constraints.put("MESSAGE_TYPE", "STATE_TRANSITION");
    constraints.put("TRANSITION", "OFFLINE-COMPLETED");
    constraints.put("CONSTRAINT_VALUE", "1");
    constraints.put("INSTANCE", ".*");
    manager.getClusterManagmentTool().setConstraint(manager.getClusterName(),
        ConstraintType.MESSAGE_CONSTRAINT, "constraint1", new ConstraintItem(constraints));

    MockAsyncCallback callback = new MockAsyncCallback();
    cr.setInstanceName("localhost_%");
    mapper = new ObjectMapper();
    serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    sw = new StringWriter();
    mapper.writeValue(sw, cr);

    crString = sw.toString();
    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    manager.getMessagingService().sendAndWait(cr2, schedulerMessage, callback, -1);
    String msgIdPrime =
        callback._message.getResultMap()
            .get(DefaultSchedulerMessageHandlerFactory.SCHEDULER_MSG_ID);

    boolean success = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        return count(_factory) == 60; // TestDB number_of_partitions x replicas
      }
    }, 10 * 1000);
    Assert.assertTrue(success, "If not specifying participant, controller will send 60 messages");

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    ArrayList<String> msgIds = new ArrayList<String>();
    for (int i = 0; i < NODE_NR; i++) {
      callback = new MockAsyncCallback();
      cr.setInstanceName("localhost_" + (START_PORT + i));
      mapper = new ObjectMapper();
      serializationConfig = mapper.getSerializationConfig();
      serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

      sw = new StringWriter();
      mapper.writeValue(sw, cr);
      schedulerMessage.setMsgId(UUID.randomUUID().toString());
      crString = sw.toString();
      schedulerMessage.getRecord().setSimpleField("Criteria", crString);
      manager.getMessagingService().sendAndWait(cr2, schedulerMessage, callback, -1);

      Thread.sleep(5000);
      System.err.println("count: " + count(_factory));

      String msgId =
          callback._message.getResultMap().get(
              DefaultSchedulerMessageHandlerFactory.SCHEDULER_MSG_ID);
      msgIds.add(msgId);
    }

    // System.err.println("count: " + count(_factory));

    for (int i = 0; i < NODE_NR; i++) {
      String msgId = msgIds.get(i);
      for (int j = 0; j < 100; j++) {
        Thread.sleep(200);
        PropertyKey controllerTaskStatus =
            keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
        ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
        if (statusUpdate.getMapFields().containsKey("Summary")) {
          // System.err.println(msgId+" done");
          break;
        }
      }

      PropertyKey controllerTaskStatus =
          keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount")
          .equals("" + (_PARTITIONS * 3 / 5)));
      int messageResultCount = 0;
      for (String key : statusUpdate.getMapFields().keySet()) {
        if (key.startsWith("MessageResult")) {
          messageResultCount++;
        }
      }
      if (messageResultCount != _PARTITIONS * 3 / 5) {
        int x = 10;
        x = x + messageResultCount;
      }
      Assert.assertEquals(messageResultCount, _PARTITIONS * 3 / 5);
    }

    for (int j = 0; j < 100; j++) {
      Thread.sleep(200);
      PropertyKey controllerTaskStatus =
          keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgIdPrime);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      if (statusUpdate.getMapFields().containsKey("Summary")) {
        break;
      }
    }

    // Thread.sleep(5000);
    int count = 0;
    for (Set<String> val : _factory._results.values()) {
      // System.out.println(val);
      count += val.size();
    }
    // System.out.println(count);
    Assert.assertEquals(count, _PARTITIONS * 3 * 2);
  }

  @Test
  public void testSchedulerMsgContraints() throws JsonGenerationException, JsonMappingException,
      IOException, InterruptedException {
    TestMessagingHandlerFactoryLatch factory = new TestMessagingHandlerFactoryLatch();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);

      _participants[i].getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);

      manager = _participants[i]; // _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msg = new Message(factory.getMessageType(), "Template");
    msg.setTgtSessionId("*");
    msg.setMsgState(MessageState.NEW);

    // Criteria to send individual messages
    Criteria cr = new Criteria();
    cr.setInstanceName("localhost_%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setResource("%");
    cr.setPartition("%");

    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, cr);

    String crString = sw.toString();

    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    schedulerMessage.getRecord().setMapField("MessageTemplate", msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");
    schedulerMessage.getRecord().setSimpleField("WAIT_ALL", "true");
    schedulerMessage.getRecord().setSimpleField(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, "TestSchedulerMsgContraints");

    Criteria cr2 = new Criteria();
    cr2.setRecipientInstanceType(InstanceType.CONTROLLER);
    cr2.setInstanceName("*");
    cr2.setSessionSpecific(false);

    MockAsyncCallback callback = new MockAsyncCallback();
    mapper = new ObjectMapper();
    serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    sw = new StringWriter();
    mapper.writeValue(sw, cr);

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();

    // Set contraints that only 1 msg per participant
    Map<String, String> constraints = new TreeMap<String, String>();
    constraints.put("MESSAGE_TYPE", "STATE_TRANSITION");
    constraints.put("TRANSITION", "OFFLINE-COMPLETED");
    constraints.put("CONSTRAINT_VALUE", "1");
    constraints.put("INSTANCE", ".*");
    manager.getClusterManagmentTool().setConstraint(manager.getClusterName(),
        ConstraintType.MESSAGE_CONSTRAINT, "constraint1", new ConstraintItem(constraints));

    // Send scheduler message
    crString = sw.toString();
    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    manager.getMessagingService().sendAndWait(cr2, schedulerMessage, callback, -1);
    String msgId =
        callback._message.getResultMap()
            .get(DefaultSchedulerMessageHandlerFactory.SCHEDULER_MSG_ID);

    for (int j = 0; j < 10; j++) {
      Thread.sleep(200);
      PropertyKey controllerTaskStatus =
          keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      if (statusUpdate.getMapFields().containsKey("SentMessageCount")) {
        Assert.assertEquals(
            statusUpdate.getMapFields().get("SentMessageCount").get("MessageCount"), ""
                + (_PARTITIONS * 3));
        break;
      }
    }

    for (int i = 0; i < _PARTITIONS * 3 / 5; i++) {
      for (int j = 0; j < 10; j++) {
        Thread.sleep(300);
        if (factory._messageCount == 5 * (i + 1))
          break;
      }
      Thread.sleep(300);
      Assert.assertEquals(factory._messageCount, 5 * (i + 1));
      factory.signal();
      // System.err.println(i);
    }

    for (int j = 0; j < 10; j++) {
      Thread.sleep(200);
      PropertyKey controllerTaskStatus =
          keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      if (statusUpdate.getMapFields().containsKey("Summary")) {
        break;
      }
    }

    Assert.assertEquals(_PARTITIONS, factory._results.size());
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
    ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
    Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount")
        .equals("" + (_PARTITIONS * 3)));
    int messageResultCount = 0;
    for (String key : statusUpdate.getMapFields().keySet()) {
      if (key.startsWith("MessageResult ")) {
        messageResultCount++;
      }
    }
    Assert.assertEquals(messageResultCount, _PARTITIONS * 3);

    int count = 0;
    for (Set<String> val : factory._results.values()) {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS * 3);

    manager.getClusterManagmentTool().removeConstraint(manager.getClusterName(),
        ConstraintType.MESSAGE_CONSTRAINT, "constraint1");

  }
}
