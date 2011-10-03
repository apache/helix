package com.linkedin.clustermanager;

import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.messaging.handling.MessageHandler;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;

public class TestMessagingService extends ZkStandAloneCMHandler
{
  public static class TestMessagingHandlerFactory implements MessageHandlerFactory
  {
    public static HashSet<String> _processedMsgIds = new HashSet<String>();
    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      return new TestMessagingHandler();
    }

    @Override
    public String getMessageType()
    {
      return "TestExtensibility";
    }

    @Override
    public void reset()
    {
      // TODO Auto-generated method stub
      
    }
    
    public static class TestMessagingHandler implements MessageHandler
    {
      @Override
      public void handleMessage(Message message, NotificationContext context,
          Map<String, String> resultMap) throws InterruptedException
      {
        // TODO Auto-generated method stub
        System.out.println("TestMessagingHandler " + message.getMsgId());
        _processedMsgIds.add(message.getRecord().getSimpleField("TestMessagingPara"));
      }
    }
  }
  @Test
  public void TestMessageSimple() throws Exception
  {
    String hostSrc = "localhost_"+START_PORT;
    String hostDest = "localhost_"+(START_PORT + 1);
    
    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _managerMap.get(hostDest).getMessagingService().registerMessageHandlerFactory(factory.getMessageType(), factory);
    
    String msgId = new UUID(123,456).toString(); 
    Message msg = new Message(factory.getMessageType());
    msg.setMsgId(msgId);
    msg.setSrcName(hostSrc);
    msg.setTgtSessionId("*");
    msg.setMsgState("new");
    String para = "Testing messaging para";
    msg.getRecord().setSimpleField("TestMessagingPara", para);
    
    Criteria cr = new Criteria();
    cr.setInstanceName(hostDest);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(false);
    
    _managerMap.get(hostSrc).getMessagingService().send(cr, msg);
    
    Thread.currentThread().sleep(2000);
    Thread.currentThread().join();
    Assert.assertTrue(TestMessagingHandlerFactory._processedMsgIds.contains(para));
    
  }
}
