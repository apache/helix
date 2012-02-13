package com.linkedin.helix.messaging;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.Mocks;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.messaging.AsyncCallback;
import com.linkedin.helix.messaging.handling.AsyncCallbackService;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.messaging.handling.TestHelixTaskExecutor.MockClusterManager;
import com.linkedin.helix.model.Message;

public class TestAsyncCallbackSvc
{
  class MockHelixManager extends Mocks.MockManager
  {
    public String getSessionId()
    {
      return "123";
    }
  }
  
  class TestAsyncCallback extends AsyncCallback
  {
    HashSet<String> _repliedMessageId = new HashSet<String>();
    @Override
    public void onTimeOut()
    {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onReplyMessage(Message message)
    {
      // TODO Auto-generated method stub
      _repliedMessageId.add(message.getMsgId());
    }
    
  }
  @Test(groups =
  { "unitTest" })
  public void testAsyncCallbackSvc() throws Exception
  {
    AsyncCallbackService svc = new AsyncCallbackService();
    HelixManager manager = new MockHelixManager();
    NotificationContext changeContext = new NotificationContext(manager);
    
    Message msg = new Message(svc.getMessageType(), UUID.randomUUID().toString());
    msg.setTgtSessionId(manager.getSessionId());
    try
    {
      MessageHandler aHandler = svc.createHandler(msg, changeContext);
    }
    catch(HelixException e)
    {
      AssertJUnit.assertTrue(e.getMessage().indexOf(msg.getMsgId())!= -1);
    }
    Message msg2 = new Message("RandomType", UUID.randomUUID().toString());
    msg2.setTgtSessionId(manager.getSessionId());
    try
    {
      MessageHandler aHandler = svc.createHandler(msg2, changeContext);
    }
    catch(HelixException e)
    {
      AssertJUnit.assertTrue(e.getMessage().indexOf(msg2.getMsgId())!= -1);
    }
    Message msg3 = new Message(svc.getMessageType(), UUID.randomUUID().toString());
    msg3.setTgtSessionId(manager.getSessionId());
    msg3.setCorrelationId("wfwegw");
    try
    {
      MessageHandler aHandler = svc.createHandler(msg3, changeContext);
    }
    catch(HelixException e)
    {
      AssertJUnit.assertTrue(e.getMessage().indexOf(msg3.getMsgId())!= -1);
    }
    
    TestAsyncCallback callback = new TestAsyncCallback();
    String corrId = UUID.randomUUID().toString();
    svc.registerAsyncCallback(corrId, new TestAsyncCallback());
    svc.registerAsyncCallback(corrId, callback);
    
    List<Message> msgSent = new ArrayList<Message>();
    msgSent.add(new Message("Test", UUID.randomUUID().toString()));
    callback.setMessagesSent(msgSent);
    
    msg = new Message(svc.getMessageType(), UUID.randomUUID().toString());
    msg.setTgtSessionId("*");
    msg.setCorrelationId(corrId);
    
    MessageHandler aHandler = svc.createHandler(msg, changeContext);
    Map<String, String> resultMap = new HashMap<String, String>();
    aHandler.handleMessage();
    
    AssertJUnit.assertTrue(callback.isDone());
    AssertJUnit.assertTrue(callback._repliedMessageId.contains(msg.getMsgId()));
  }
}
