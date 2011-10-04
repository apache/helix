package com.linkedin.clustermanager;

import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.messaging.handling.MessageHandler;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
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
        resultMap.put("ReplyMessage", "TestReplyMessage");
      }
    }
  }
  
  
  @Test
  public void TestMessageSimpleSend() throws Exception
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
    //Thread.currentThread().join();
    Assert.assertTrue(TestMessagingHandlerFactory._processedMsgIds.contains(para));
    
  }
  public static class TestAsyncCallback extends AsyncCallback
  {
    static HashSet<String> _replyedMessageContents = new HashSet<String>();
    @Override
    public void onTimeOut()
    {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onReplyMessage(Message message)
    {
      // TODO Auto-generated method stub
      System.out.println("OnreplyMessage: "+ message.getRecord().getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ReplyMessage"));
      _replyedMessageContents.add(message.getRecord().getMapField(Message.Attributes.MESSAGE_RESULT.toString()).get("ReplyMessage"));
    }
    
  }
  @Test
  public void TestMessageSimpleSendReceiveAsync() throws Exception
  {
    String hostSrc = "localhost_"+START_PORT;
    String hostDest = "localhost_"+(START_PORT + 1);
    
    TestMessagingHandlerFactory factory = new TestMessagingHandlerFactory();
    _managerMap.get(hostDest).getMessagingService().registerMessageHandlerFactory(factory.getMessageType(), factory);
    
    
    _managerMap.get(hostSrc).getMessagingService().registerMessageHandlerFactory(factory.getMessageType(), factory);
    
    
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
    
    TestAsyncCallback callback = new TestAsyncCallback();
    
    _managerMap.get(hostSrc).getMessagingService().send(cr, msg, callback);
    
    Thread.currentThread().sleep(2000);
    //Thread.currentThread().join();
    Assert.assertTrue(TestAsyncCallback._replyedMessageContents.contains("TestReplyMessage"));
    
  }
}
