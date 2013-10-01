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

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.SessionId;
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
import org.apache.helix.monitoring.ZKPathDataDumpTask;
import org.apache.helix.util.HelixUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchedulerMessage extends ZkStandAloneCMTestBaseWithPropertyServerCheck {

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

  TestMessagingHandlerFactory _factory = new TestMessagingHandlerFactory();

  public static class TestMessagingHandlerFactory implements MessageHandlerFactory {
    int cnt;
    public Map<String, Set<String>> _results = new ConcurrentHashMap<String, Set<String>>();

    public TestMessagingHandlerFactory() {
      super();
      cnt = 0;
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      // System.out.println("\t create-hdlr: " + message.getId());
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
        // String tgtName = _message.getTgtName();
        String messageId = _message.getMsgId().stringify();
        String partitionId = _message.getPartitionId().stringify();

        result.getTaskResultMap().put("Message", messageId);
        synchronized (_results) {
          if (!_results.containsKey(partitionId)) {
            _results.put(partitionId, new HashSet<String>());
          }
          _results.get(partitionId).add(messageId);
        }
        cnt++;
        // System.err.println(cnt + ": message " + messageId + ", tgtName: " + tgtName
        // + ", partitionId: " + partitionId);
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        // TODO Auto-generated method stub
      }
    }
  }

  @Test()
  public void testSchedulerMsg() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    _factory._results.clear();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", MessageId.from(UUID.randomUUID().toString()));
    schedulerMessage.setTgtSessionId(SessionId.from("*"));
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");
    // schedulerMessage.getRecord().setSimpleField(DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE,
    // "TestSchedulerMsg");
    // Template for the individual message sent to each participant
    Message msg = new Message(_factory.getMessageType(), MessageId.from("Template"));
    msg.setTgtSessionId(SessionId.from("*"));
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
    helixDataAccessor.createProperty(
        keyBuilder.controllerMessage(schedulerMessage.getMsgId().stringify()), schedulerMessage);

    for (int i = 0; i < 30; i++) {
      Thread.sleep(2000);
      if (_PARTITIONS == _factory._results.size()) {
        break;
      }
    }

    Assert.assertEquals(_PARTITIONS, _factory._results.size());
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), schedulerMessage
            .getMsgId().stringify());

    int messageResultCount = 0;
    for (int i = 0; i < 10; i++) {
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
    List<String> subPaths = _zkClient.getChildren(controllerStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for (String subPath : subPaths) {
      String nextPath = controllerStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkClient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() > 0);
    }

    String instanceStatusPath =
        HelixUtil.getInstancePropertyPath(manager.getClusterName(), "localhost_" + (START_PORT),
            PropertyType.STATUSUPDATES);

    subPaths = _zkClient.getChildren(instanceStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for (String subPath : subPaths) {
      String nextPath = instanceStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkClient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() > 0);
      for (String subsubPath : subsubPaths) {
        String nextnextPath = nextPath + "/" + subsubPath;
        Assert.assertTrue(_zkClient.getChildren(nextnextPath).size() > 0);
      }
    }
    Thread.sleep(3000);
    ZKPathDataDumpTask dumpTask = new ZKPathDataDumpTask(manager, _zkClient, 0);
    dumpTask.run();

    subPaths = _zkClient.getChildren(controllerStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for (String subPath : subPaths) {
      String nextPath = controllerStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkClient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() == 0);
    }

    subPaths = _zkClient.getChildren(instanceStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for (String subPath : subPaths) {
      String nextPath = instanceStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkClient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() > 0);
      for (String subsubPath : subsubPaths) {
        String nextnextPath = nextPath + "/" + subsubPath;
        Assert.assertTrue(_zkClient.getChildren(nextnextPath).size() == 0);
      }
    }
  }

  @Test()
  public void testSchedulerMsg2() throws Exception {
    _factory._results.clear();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", MessageId.from(UUID.randomUUID().toString()));
    schedulerMessage.setTgtSessionId(SessionId.from("*"));
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msg = new Message(_factory.getMessageType(), MessageId.from("Template"));
    msg.setTgtSessionId(SessionId.from("*"));
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
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", MessageId.from(UUID.randomUUID().toString()));
    schedulerMessage.setTgtSessionId(SessionId.from("*"));
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msg = new Message(factory.getMessageType(), MessageId.from("Template"));
    msg.setTgtSessionId(SessionId.from("*"));
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
    PropertyKey controllerMessageKey =
        keyBuilder.controllerMessage(schedulerMessage.getMsgId().stringify());
    helixDataAccessor.setProperty(controllerMessageKey, schedulerMessage);

    Thread.sleep(3000);

    Assert.assertEquals(0, factory._results.size());

    waitMessageUpdate("SentMessageCount", schedulerMessage.getMsgId().stringify(),
        helixDataAccessor);
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), schedulerMessage
            .getMsgId().stringify());
    waitMessageUpdate("SentMessageCount", schedulerMessage.getMsgId().stringify(),
        helixDataAccessor);
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
    final int avgReplicas = _PARTITIONS * 3 / 5;

    _factory._results.clear();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);

      _startCMResultMap.get(hostDest)._manager.getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", MessageId.from(UUID.randomUUID().toString()));
    schedulerMessage.setTgtSessionId(SessionId.from("*"));
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msg = new Message(_factory.getMessageType(), MessageId.from("Template"));
    msg.setTgtSessionId(SessionId.from("*"));
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
      schedulerMessage.setMsgId(MessageId.from(UUID.randomUUID().toString()));
      crString = sw.toString();
      schedulerMessage.getRecord().setSimpleField("Criteria", crString);
      manager.getMessagingService().sendAndWait(cr2, schedulerMessage, callback, -1);
      String msgId =
          callback._message.getResultMap().get(
              DefaultSchedulerMessageHandlerFactory.SCHEDULER_MSG_ID);

      Thread.sleep(1000);
      HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
      Builder keyBuilder = helixDataAccessor.keyBuilder();

      waitMessageUpdate("Summary", msgId, helixDataAccessor);

      PropertyKey controllerTaskStatus =
          keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), msgId);
      ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus).getRecord();
      Assert.assertTrue(statusUpdate.getMapField("SentMessageCount").get("MessageCount")
          .equals("" + avgReplicas));
      int messageResultCount = 0;
      for (String key : statusUpdate.getMapFields().keySet()) {
        if (key.startsWith("MessageResult")) {
          messageResultCount++;
        }
      }
      Assert.assertEquals(messageResultCount, avgReplicas);

      int count = 0;
      // System.out.println(i);
      for (Set<String> val : _factory._results.values()) {
        // System.out.println(val);
        count += val.size();
      }
      // System.out.println(count);

      Assert.assertEquals(count, avgReplicas * (i + 1));
    }
  }

  @Test()
  public void testSchedulerMsg4() throws Exception {
    _factory._results.clear();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);
      _startCMResultMap.get(hostDest)._manager.getMessagingService().registerMessageHandlerFactory(
          _factory.getMessageType(), _factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage =
        new Message(MessageType.SCHEDULER_MSG + "", MessageId.from(UUID.randomUUID().toString()));
    schedulerMessage.setTgtSessionId(SessionId.from("*"));
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");

    // Template for the individual message sent to each participant
    Message msg = new Message(_factory.getMessageType(), MessageId.from("Template"));
    msg.setTgtSessionId(SessionId.from("*"));
    msg.setMsgState(MessageState.NEW);

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
    schedulerMessage.getRecord().setMapField("MessageTemplate", msg.getRecord().getSimpleFields());
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

    final HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    final Builder keyBuilder = helixDataAccessor.keyBuilder();
    ArrayList<String> msgIds = new ArrayList<String>();
    for (int i = 0; i < NODE_NR; i++) {
      callback = new MockAsyncCallback();
      cr.setInstanceName("localhost_" + (START_PORT + i));
      mapper = new ObjectMapper();
      serializationConfig = mapper.getSerializationConfig();
      serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

      sw = new StringWriter();
      mapper.writeValue(sw, cr);
      schedulerMessage.setMsgId(MessageId.from(UUID.randomUUID().toString()));

      // need to use a different name for scheduler_task_queue task resource
      schedulerMessage.getRecord().setSimpleField(
          DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, "TestSchedulerMsg4_" + i);

      crString = sw.toString();
      schedulerMessage.getRecord().setSimpleField("Criteria", crString);
      manager.getMessagingService().sendAndWait(cr2, schedulerMessage, callback, -1);
      String msgId =
          callback._message.getResultMap().get(
              DefaultSchedulerMessageHandlerFactory.SCHEDULER_MSG_ID);
      msgIds.add(msgId);
    }

    for (int i = 0; i < NODE_NR; i++) {
      final String msgId = msgIds.get(i);

      waitMessageUpdate("Summary", msgId, helixDataAccessor);

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
    }

    waitMessageUpdate("Summary", msgIdPrime, helixDataAccessor);

    int count = 0;
    for (Set<String> val : _factory._results.values()) {
      // System.out.println(val);
      count += val.size();
    }
    // System.out.println(count);
    Assert.assertEquals(count, _PARTITIONS * 3 * 2);
  }

  /**
   * wait message summary to appear in controller-message-status-update
   * @param msgId
   * @param accessor
   * @return
   * @throws Exception
   */
  private boolean waitMessageUpdate(final String mapKey, final String msgId,
      final HelixDataAccessor accessor) throws Exception {
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        PropertyKey key = keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.name(), msgId);
        HelixProperty statusUpdate = accessor.getProperty(key);
        if (statusUpdate == null) {
          return false;
        }

        if (statusUpdate.getRecord().getMapField(mapKey) == null) {
          return false;
        }
        return true;
      }
    }, 20 * 1000);
  }
}
