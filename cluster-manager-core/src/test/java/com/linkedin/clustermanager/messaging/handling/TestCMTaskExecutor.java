package com.linkedin.clustermanager.messaging.handling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.Mocks;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.Message;

public class TestCMTaskExecutor
{
  public static class MockClusterManager extends Mocks.MockManager
  {
    @Override
    public String getSessionId()
    {
      return "123";
    }
  }
  
  
  
  class TestMessageHandlerFactory implements MessageHandlerFactory
  {
    int _handlersCreated = 0;
    ConcurrentHashMap<String, String> _processedMsgIds = new ConcurrentHashMap<String, String>();
    class TestMessageHandler implements MessageHandler
    {

      @Override
      public void handleMessage(Message message, NotificationContext context,
          Map<String, String> resultMap) throws InterruptedException
      {
        // TODO Auto-generated method stub
        _processedMsgIds.put(message.getMsgId(), message.getMsgId());
        Thread.currentThread().sleep(100);
      }
      
    }
    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      // TODO Auto-generated method stub
      _handlersCreated++;
      return new TestMessageHandler();
    }

    @Override
    public String getMessageType()
    {
      // TODO Auto-generated method stub
      return "TestingMessageHandler";
    }

    @Override
    public void reset()
    {
      // TODO Auto-generated method stub
      
    }
  }
  
  class TestMessageHandlerFactory2 extends TestMessageHandlerFactory
  {
    @Override
    public String getMessageType()
    {
      // TODO Auto-generated method stub
      return "TestingMessageHandler2";
    }
  }
  
  class CancellableHandlerFactory implements MessageHandlerFactory
  {

    int _handlersCreated = 0;
    ConcurrentHashMap<String, String> _processedMsgIds = new ConcurrentHashMap<String, String>();
    ConcurrentHashMap<String, String> _processingMsgIds = new ConcurrentHashMap<String, String>();
    class CancellableHandler implements MessageHandler
    {
      public boolean _interrupted = false;
      @Override
      public void handleMessage(Message message, NotificationContext context,
          Map<String, String> resultMap) throws InterruptedException
      {
        // TODO Auto-generated method stub
        _processingMsgIds.put(message.getMsgId(), message.getMsgId());
        try
        {
          for (int i = 0; i < 10; i++)
          {
            Thread.sleep(100);
          }
        } catch (InterruptedException e)
        {
          _interrupted = true;
          message.getRecord().setSimpleField("Canceled", "Canceled");
          throw e;
        }
        _processedMsgIds.put(message.getMsgId(), message.getMsgId());
      }
      
    }
    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      // TODO Auto-generated method stub
      _handlersCreated++;
      return new CancellableHandler();
    }

    @Override
    public String getMessageType()
    {
      // TODO Auto-generated method stub
      return "Cancellable";
    }

    @Override
    public void reset()
    {
      // TODO Auto-generated method stub
      
    }
  }
  

  @Test (groups = {"unitTest"})
  public void TestNormalMsgExecution() throws InterruptedException
  {
    System.out.println("START TestCMTaskExecutor.TestNormalMsgExecution()");
    CMTaskExecutor executor = new CMTaskExecutor();
    ClusterManager manager = new MockClusterManager();
    
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);
    
    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
    executor.registerMessageHandlerFactory(factory2.getMessageType(), factory2);
    
    NotificationContext changeContext = new NotificationContext(manager);
    List<ZNRecord> msgList = new ArrayList<ZNRecord>();
    
    int nMsgs1 = 5;
    for(int i = 0; i < nMsgs1; i++)
    {
      Message msg = new Message(factory.getMessageType(), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg.getRecord());
    }
    
    
    int nMsgs2 = 4;
    for(int i = 0; i < nMsgs2; i++)
    {
      Message msg = new Message(factory2.getMessageType(), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msg.setCorrelationId(UUID.randomUUID().toString());
      msgList.add(msg.getRecord());
    }
    executor.onMessage("someInstance", msgList, changeContext);
    
    Thread.sleep(1000);
    
    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory2._processedMsgIds.size() == nMsgs2);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1);
    AssertJUnit.assertTrue(factory2._handlersCreated == nMsgs2);
    
    for(ZNRecord record : msgList)
    {
      AssertJUnit.assertTrue(factory._processedMsgIds.containsKey(record.getId()) || factory2._processedMsgIds.containsKey(record.getId()));
      AssertJUnit.assertFalse(factory._processedMsgIds.containsKey(record.getId()) && factory2._processedMsgIds.containsKey(record.getId()));
      
    }
    System.out.println("END TestCMTaskExecutor.TestNormalMsgExecution()");
  }
  
  @Test (groups = {"unitTest"})
  public void TestUnknownTypeMsgExecution() throws InterruptedException
  {
    CMTaskExecutor executor = new CMTaskExecutor();
    ClusterManager manager = new MockClusterManager();
    
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);
    
    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
    
    NotificationContext changeContext = new NotificationContext(manager);
    List<ZNRecord> msgList = new ArrayList<ZNRecord>();
    
    int nMsgs1 = 5;
    for(int i = 0; i < nMsgs1; i++)
    {
      Message msg = new Message(factory.getMessageType(), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg.getRecord());
    }
    
    
    int nMsgs2 = 4;
    for(int i = 0; i < nMsgs2; i++)
    {
      Message msg = new Message(factory2.getMessageType(), UUID.randomUUID().toString());
      msg.setTgtSessionId(manager.getSessionId());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg.getRecord());
    }
    executor.onMessage("someInstance", msgList, changeContext);
    
    Thread.sleep(1000);
    
    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory2._processedMsgIds.size() == 0);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1);
    AssertJUnit.assertTrue(factory2._handlersCreated == 0);
    
    for(ZNRecord record : msgList)
    {
      Message message = new Message(record);
      if(message.getMsgType().equalsIgnoreCase(factory.getMessageType()))
      {
        AssertJUnit.assertTrue(factory._processedMsgIds.containsKey(record.getId()));
      }
    }
  }
  

  @Test (groups = {"unitTest"})
  public void TestMsgSessionId() throws InterruptedException
  {
    CMTaskExecutor executor = new CMTaskExecutor();
    ClusterManager manager = new MockClusterManager();
    
    TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);
    
    TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
    executor.registerMessageHandlerFactory(factory2.getMessageType(), factory2);
    
    NotificationContext changeContext = new NotificationContext(manager);
    List<ZNRecord> msgList = new ArrayList<ZNRecord>();
    
    int nMsgs1 = 5;
    for(int i = 0; i < nMsgs1; i++)
    {
      Message msg = new Message(factory.getMessageType(), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msg.setTgtName("");
      msgList.add(msg.getRecord());
    }
    
    
    int nMsgs2 = 4;
    for(int i = 0; i < nMsgs2; i++)
    {
      Message msg = new Message(factory2.getMessageType(), UUID.randomUUID().toString());
      msg.setTgtSessionId("some other session id");
      msg.setTgtName("");
      msgList.add(msg.getRecord());
    }
    executor.onMessage("someInstance", msgList, changeContext);
    
    Thread.sleep(1000);
    
    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory2._processedMsgIds.size() == 0);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1);
    AssertJUnit.assertTrue(factory2._handlersCreated == 0);
    
    for(ZNRecord record : msgList)
    {
      Message message = new Message(record);
      if(message.getMsgType().equalsIgnoreCase(factory.getMessageType()))
      {
        AssertJUnit.assertTrue(factory._processedMsgIds.containsKey(record.getId()));
      }
    }
  }
  

  @Test (groups = {"unitTest"})
  public void TestTaskCancellation() throws InterruptedException
  {
    CMTaskExecutor executor = new CMTaskExecutor();
    ClusterManager manager = new MockClusterManager();
    
    CancellableHandlerFactory factory = new CancellableHandlerFactory();
    executor.registerMessageHandlerFactory(factory.getMessageType(), factory);
    
    NotificationContext changeContext = new NotificationContext(manager);
    List<ZNRecord> msgList = new ArrayList<ZNRecord>();
    
    int nMsgs1 = 0;
    for(int i = 0; i < nMsgs1; i++)
    {
      Message msg = new Message(factory.getMessageType(), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgList.add(msg.getRecord());
    }
    
    List<Message> msgListToCancel = new ArrayList<Message>();
    int nMsgs2 = 4;
    for(int i = 0; i < nMsgs2; i++)
    {
      Message msg = new Message(factory.getMessageType(), UUID.randomUUID().toString());
      msg.setTgtSessionId("*");
      msgList.add(msg.getRecord());
      msg.setTgtName("Localhost_1123");
      msg.setSrcName("127.101.1.23_2234");
      msgListToCancel.add(msg);
    }
    executor.onMessage("someInstance", msgList, changeContext);
    Thread.sleep(500);
    for(int i = 0; i < nMsgs2; i++)
    {
      executor.cancelTask(msgListToCancel.get(i), changeContext);
    }
    Thread.sleep(1500);
    
    AssertJUnit.assertTrue(factory._processedMsgIds.size() == nMsgs1);
    AssertJUnit.assertTrue(factory._handlersCreated == nMsgs1 + nMsgs2);

    AssertJUnit.assertTrue(factory._processingMsgIds.size() == nMsgs1 + nMsgs2);
    
    for(ZNRecord record : msgList)
    {
      Message message = new Message(record);
      if(message.getMsgType().equalsIgnoreCase(factory.getMessageType()))
      {
        AssertJUnit.assertTrue(factory._processingMsgIds.containsKey(record.getId()));
      }
    }
  }
  

  @Test (groups = {"unitTest"})
  public void testShutdown() throws InterruptedException
  {
     System.out.println("START TestCMTaskExecutor.TestNormalMsgExecution()");
     CMTaskExecutor executor = new CMTaskExecutor();
      ClusterManager manager = new MockClusterManager();
      
      TestMessageHandlerFactory factory = new TestMessageHandlerFactory();
      executor.registerMessageHandlerFactory(factory.getMessageType(), factory);
      
      TestMessageHandlerFactory2 factory2 = new TestMessageHandlerFactory2();
      executor.registerMessageHandlerFactory(factory2.getMessageType(), factory2);
      
      CancellableHandlerFactory factory3 = new CancellableHandlerFactory();
      executor.registerMessageHandlerFactory(factory3.getMessageType(), factory3);
      int nMsg1 = 10, nMsg2 = 10, nMsg3 = 10;
      List<ZNRecord> msgList = new ArrayList<ZNRecord>();
      
      for(int i = 0; i < nMsg1; i++)
      {
        Message msg = new Message(factory.getMessageType(), UUID.randomUUID().toString());
        msg.setTgtSessionId("*");
        msg.setTgtName("Localhost_1123");
        msg.setSrcName("127.101.1.23_2234");
        msgList.add(msg.getRecord());
      }
      
      for(int i = 0; i < nMsg2; i++)
      {
        Message msg = new Message(factory2.getMessageType(), UUID.randomUUID().toString());
        msg.setTgtSessionId("*");
        msgList.add(msg.getRecord());
        msg.setTgtName("Localhost_1123");
        msg.setSrcName("127.101.1.23_2234");
        msgList.add(msg.getRecord());
      }
      
      for(int i = 0; i < nMsg3; i++)
      {
        Message msg = new Message(factory3.getMessageType(), UUID.randomUUID().toString());
        msg.setTgtSessionId("*");
        msgList.add(msg.getRecord());
        msg.setTgtName("Localhost_1123");
        msg.setSrcName("127.101.1.23_2234");
        msgList.add(msg.getRecord());
      }
      NotificationContext changeContext = new NotificationContext(manager);
      executor.onMessage("some", msgList, changeContext);
      Thread.currentThread().sleep(500);
      for(ExecutorService svc : executor._threadpoolMap.values())
      {
        Assert.assertFalse(svc.isShutdown());
        }
      Assert.assertTrue(factory._processedMsgIds.size() > 0);
      executor.shutDown();
      for(ExecutorService svc : executor._threadpoolMap.values())
      {
        Assert.assertTrue(svc.isShutdown());
      }
  }
}
