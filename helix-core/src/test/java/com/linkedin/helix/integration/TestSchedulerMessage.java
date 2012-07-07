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

import java.io.StringWriter;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.messaging.handling.HelixTaskResult;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.messaging.handling.MessageHandlerFactory;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;
import com.linkedin.helix.model.Message.MessageType;

public class TestSchedulerMessage extends ZkStandAloneCMTestBase
{
  public static class TestMessagingHandlerFactory implements
      MessageHandlerFactory
  {
    public Map<String, Set<String>> _results = new ConcurrentHashMap<String, Set<String>>();

    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      return new TestMessagingHandler(message, context);
    }

    @Override
    public String getMessageType()
    {
      return "TestParticipant";
    }

    @Override
    public void reset()
    {
      // TODO Auto-generated method stub

    }

    public class TestMessagingHandler extends MessageHandler
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
        String destName = _message.getTgtName();
        synchronized (_results)
        {
          if (!_results.containsKey(_message.getPartitionName()))
          {
            _results.put(_message.getPartitionName(),
                new ConcurrentSkipListSet<String>());
          }
        }
        _results.get(_message.getPartitionName()).add(destName);

        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type)
      {
        // TODO Auto-generated method stub

      }
    }
  }

  @Test()
  public void TestSchedulerMsg() throws Exception
  {
    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++)
    {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService()
          .registerMessageHandlerFactory(factory.getMessageType(), factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage = new Message(MessageType.SCHEDULER_MSG + "", UUID
        .randomUUID().toString());
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
    schedulerMessage.getRecord().setMapField("MessageTemplate",
        msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    helixDataAccessor.createProperty(
        keyBuilder.controllerMessage(schedulerMessage.getMsgId()),
        schedulerMessage);

    Thread.sleep(15000);

    Assert.assertEquals(_PARTITIONS, factory._results.size());
    PropertyKey controllerTaskStatus = keyBuilder.controllerTaskStatus(
        MessageType.SCHEDULER_MSG.toString(), schedulerMessage.getMsgId());
    ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus)
        .getRecord();
    Assert.assertTrue(statusUpdate.getMapField("SentMessageCount")
        .get("MessageCount").equals("" + (_PARTITIONS * 3)));
    int messageResultCount = 0;
    for(String key : statusUpdate.getMapFields().keySet())
    {
      if(key.startsWith("MessageResult "))
      {
        messageResultCount ++;
      }
    }
    Assert.assertEquals(messageResultCount, _PARTITIONS * 3);
    
    int count = 0;
    for (Set<String> val : factory._results.values())
    {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS * 3);
  }

  @Test()
  public void TestSchedulerZeroMsg() throws Exception
  {
    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++)
    {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService()
          .registerMessageHandlerFactory(factory.getMessageType(), factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage = new Message(MessageType.SCHEDULER_MSG + "", UUID
        .randomUUID().toString());
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
    schedulerMessage.getRecord().setMapField("MessageTemplate",
        msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    PropertyKey controllerMessageKey = keyBuilder
        .controllerMessage(schedulerMessage.getMsgId());
    helixDataAccessor.setProperty(controllerMessageKey, schedulerMessage);

    Thread.sleep(3000);

    Assert.assertEquals(0, factory._results.size());
    PropertyKey controllerTaskStatus = keyBuilder.controllerTaskStatus(
        MessageType.SCHEDULER_MSG.toString(), schedulerMessage.getMsgId());
    ZNRecord statusUpdate = helixDataAccessor.getProperty(controllerTaskStatus)
        .getRecord();
    Assert.assertTrue(statusUpdate.getMapField("SentMessageCount")
        .get("MessageCount").equals("0"));
    int count = 0;
    for (Set<String> val : factory._results.values())
    {
      count += val.size();
    }
    Assert.assertEquals(count, 0);
  }
}
