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

import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.messaging.handling.CMTaskResult;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.messaging.handling.MessageHandlerFactory;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;

public class TestSchedulerMessage extends ZkStandAloneCMTestBase
{
  public static class TestMessagingHandlerFactory implements
      MessageHandlerFactory
  {
    public Map<String, Set<String>> 
        _results = new ConcurrentHashMap<String, Set<String>>();
    
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
      public CMTaskResult handleMessage() throws InterruptedException
      {
        CMTaskResult result = new CMTaskResult();
        result.setSuccess(true);
        String destName = _message.getTgtName();
        synchronized(_results)
        {
          if(!_results.containsKey(_message.getResourceKey()))
          {
            _results.put(_message.getResourceKey(), new ConcurrentSkipListSet<String>());
          }
        }
        _results.get(_message.getResourceKey()).add(destName);
        
        return result;
      }

      @Override
      public void onError( Exception e, ErrorCode code, ErrorType type)
      {
        // TODO Auto-generated method stub
        
      }
    }
  }

  @Test()
  public void TestSchedulerMsg() throws Exception
  {
    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    ClusterManager manager = null;
    for(int i = 0; i < NODE_NR; i++)
    {
      String hostDest = "localhost_" + (START_PORT + i);
      _startCMResultMap.get(hostDest)._manager.getMessagingService()
        .registerMessageHandlerFactory(factory.getMessageType(), factory);
      manager = _startCMResultMap.get(hostDest)._manager;
    }
    
    Message schedulerMessage = new Message(MessageType.SCHEDULER_MSG+"", UUID.randomUUID().toString());
    schedulerMessage.setTgtSessionId("*");
    schedulerMessage.setTgtName("CONTROLLER");
    // TODO: change it to "ADMIN" ?
    schedulerMessage.setSrcName("CONTROLLER");
    
    // Template for the individual message sent to each participant
    Message msg = new Message(factory.getMessageType(), "Template");
    msg.setTgtSessionId("*");
    msg.setMsgState("new");

    // Criteria to send individual messages
    Criteria cr = new Criteria();
    cr.setInstanceName("localhost_%");
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    cr.setResourceGroup("%");
    cr.setResourceKey("%");
    
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, cr);

    String crString = sw.toString();
    
    schedulerMessage.getRecord().setSimpleField("Criteria", crString);
    schedulerMessage.getRecord().setMapField("MessageTemplate", msg.getRecord().getSimpleFields());
    schedulerMessage.getRecord().setSimpleField("TIMEOUT", 5000+"");
    
    manager.getDataAccessor().setProperty(PropertyType.MESSAGES_CONTROLLER, schedulerMessage, schedulerMessage.getMsgId());
    
    Thread.sleep(10000);
    
    Assert.assertEquals(_PARTITIONS, factory._results.size());
    
    int count = 0;
    for(Set<String> val : factory._results.values())
    {
      count += val.size();
    }
    Assert.assertEquals(count, _PARTITIONS *3);
  }
}

