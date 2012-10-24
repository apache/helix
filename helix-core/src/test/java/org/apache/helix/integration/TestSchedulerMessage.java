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
package org.apache.helix.integration;

import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.monitoring.ZKPathDataDumpTask;
import org.apache.helix.util.HelixUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSchedulerMessage extends ZkStandAloneCMTestBaseWithPropertyServerCheck
{
  TestMessagingHandlerFactory _factory = new TestMessagingHandlerFactory();
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
    _factory._results.clear();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++)
    {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService()
          .registerMessageHandlerFactory(_factory.getMessageType(), _factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage = new Message(MessageType.SCHEDULER_MSG + "", UUID
        .randomUUID().toString());
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
    schedulerMessage.getRecord().setMapField("MessageTemplate",
        msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");

    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    helixDataAccessor.createProperty(
        keyBuilder.controllerMessage(schedulerMessage.getMsgId()),
        schedulerMessage);

    Thread.sleep(15000);

    Assert.assertEquals(_PARTITIONS, _factory._results.size());
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
    for (Set<String> val : _factory._results.values())
    {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS * 3);
    
    // test the ZkPathDataDumpTask
    String controllerStatusPath = HelixUtil.getControllerPropertyPath(manager.getClusterName(),
        PropertyType.STATUSUPDATES_CONTROLLER);
    List<String> subPaths = _zkClient.getChildren(controllerStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for(String subPath : subPaths)
    {
      String nextPath = controllerStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkClient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() > 0);
    }
    
    String instanceStatusPath = HelixUtil.getInstancePropertyPath(manager.getClusterName(), "localhost_" + (START_PORT),
        PropertyType.STATUSUPDATES);
    
    subPaths = _zkClient.getChildren(instanceStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for(String subPath : subPaths)
    {
      String nextPath = instanceStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkClient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() > 0);
      for(String subsubPath : subsubPaths)
      {
        String nextnextPath = nextPath + "/" + subsubPath;
        Assert.assertTrue(_zkClient.getChildren(nextnextPath).size() > 0);
      }
    }
    
    ZKPathDataDumpTask dumpTask = new ZKPathDataDumpTask(manager, _zkClient, 0);
    dumpTask.run();
    
    subPaths = _zkClient.getChildren(controllerStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for(String subPath : subPaths)
    {
      String nextPath = controllerStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkClient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() == 0);
    }
    
    subPaths = _zkClient.getChildren(instanceStatusPath);
    Assert.assertTrue(subPaths.size() > 0);
    for(String subPath : subPaths)
    {
      String nextPath = instanceStatusPath + "/" + subPath;
      List<String> subsubPaths = _zkClient.getChildren(nextPath);
      Assert.assertTrue(subsubPaths.size() > 0);
      for(String subsubPath : subsubPaths)
      {
        String nextnextPath = nextPath + "/" + subsubPath;
        Assert.assertTrue(_zkClient.getChildren(nextnextPath).size() == 0);
      }
    }
  }
  

  @Test()
  public void TestSchedulerMsg2() throws Exception
  {
    _factory._results.clear();
    HelixManager manager = null;
    for (int i = 0; i < NODE_NR; i++)
    {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService()
          .registerMessageHandlerFactory(_factory.getMessageType(), _factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }

    Message schedulerMessage = new Message(MessageType.SCHEDULER_MSG + "", UUID
        .randomUUID().toString());
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
    schedulerMessage.getRecord().setMapField("MessageTemplate",
        msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", "-1");
    schedulerMessage.getRecord().setSimpleField("WAIT_ALL", "true");
    
    Criteria cr2 = new Criteria();
    cr2.setRecipientInstanceType(InstanceType.CONTROLLER);
    cr2.setInstanceName("*");
    cr2.setSessionSpecific(false);
    
    class MockAsyncCallback extends AsyncCallback
    {
      Message _message;
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
        _message = message;
      }

    }
    MockAsyncCallback callback = new MockAsyncCallback();
    manager.getMessagingService().sendAndWait(cr2, schedulerMessage, callback, -1);
    String msgId = callback._message.getResultMap().get(DefaultSchedulerMessageHandlerFactory.SCHEDULER_MSG_ID);
    
    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();

    Assert.assertEquals(_PARTITIONS, _factory._results.size());
    PropertyKey controllerTaskStatus = keyBuilder.controllerTaskStatus(
        MessageType.SCHEDULER_MSG.toString(), msgId);
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
    for (Set<String> val : _factory._results.values())
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
