package org.apache.helix.integration;

import java.io.StringWriter;
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
import org.apache.helix.ZNRecord;
import org.apache.helix.api.MessageId;
import org.apache.helix.api.SessionId;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchedulerMsgContraints extends ZkStandAloneCMTestBaseWithPropertyServerCheck {

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
        result.getTaskResultMap().put("Message", _message.getMsgId().stringify());
        String destName = _message.getTgtName();
        synchronized (_results) {
          if (!_results.containsKey(_message.getPartitionId().stringify())) {
            _results
                .put(_message.getPartitionId().stringify(), new ConcurrentSkipListSet<String>());
          }
        }
        _results.get(_message.getPartitionId().stringify()).add(destName);
        // System.err.println("Message " + _message.getMsgId() + " executed");
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        // TODO Auto-generated method stub
      }
    }
  }

  @Test
  public void testSchedulerMsgContraints() throws Exception {
    TestMessagingHandlerFactoryLatch factory = new TestMessagingHandlerFactoryLatch();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++) {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService().registerMessageHandlerFactory(
          factory.getMessageType(), factory);
      //
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
    PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();

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
