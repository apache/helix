package org.apache.helix.integration;

import java.io.StringWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchedulerMsgUsingQueue extends ZkStandAloneCMTestBaseWithPropertyServerCheck {
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
        String messageId = _message.getMessageId().stringify();
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
  public void testSchedulerMsgUsingQueue() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    // _factory._results.clear();
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
    schedulerMessage.getRecord().setSimpleField(
        DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, "TestSchedulerMsgUsingQueue");
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

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
    helixDataAccessor.createProperty(
        keyBuilder.controllerMessage(schedulerMessage.getMessageId().stringify()), schedulerMessage);

    for (int i = 0; i < 30; i++) {
      Thread.sleep(2000);
      if (_PARTITIONS == factory._results.size()) {
        break;
      }
    }

    Assert.assertEquals(_PARTITIONS, factory._results.size());
    PropertyKey controllerTaskStatus =
        keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), schedulerMessage
            .getMessageId().stringify());

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
    for (Set<String> val : factory._results.values()) {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS * 3);

  }
}
