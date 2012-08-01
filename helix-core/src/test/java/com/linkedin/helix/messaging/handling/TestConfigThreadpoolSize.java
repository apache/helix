package com.linkedin.helix.messaging.handling;

import java.util.HashSet;
import java.util.concurrent.ThreadPoolExecutor;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ConfigAccessor;
import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.integration.ZkStandAloneCMTestBase;
import com.linkedin.helix.integration.TestMessagingService.TestMessagingHandlerFactory.TestMessagingHandler;
import com.linkedin.helix.messaging.DefaultMessagingService;
import com.linkedin.helix.messaging.handling.HelixTaskExecutor;
import com.linkedin.helix.messaging.handling.HelixTaskResult;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.messaging.handling.MessageHandlerFactory;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorCode;
import com.linkedin.helix.messaging.handling.MessageHandler.ErrorType;
import com.linkedin.helix.model.Message;

public class TestConfigThreadpoolSize extends ZkStandAloneCMTestBase
{
  public static class TestMessagingHandlerFactory implements MessageHandlerFactory
  {
    public static HashSet<String> _processedMsgIds = new HashSet<String>();
    
    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      return null;
    }
    
    @Override
    public String getMessageType()
    {
      return "TestMsg";
    }
    
    @Override
    public void reset()
    {
      // TODO Auto-generated method stub
    }
    
  }
  
  public static class TestMessagingHandlerFactory2 implements MessageHandlerFactory
  {
    public static HashSet<String> _processedMsgIds = new HashSet<String>();
    
    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      return null;
    }
    
    @Override
    public String getMessageType()
    {
      return "TestMsg2";
    }
    
    @Override
    public void reset()
    {
      // TODO Auto-generated method stub
    }
    
  }
  @Test
  public void TestThreadPoolSizeConfig()
  {
    String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + 0);
    HelixManager manager = _startCMResultMap.get(instanceName)._manager;
    
    ConfigAccessor accessor = manager.getConfigAccessor();
    ConfigScope scope =
        new ConfigScopeBuilder().forCluster(manager.getClusterName()).forParticipant(instanceName).build();
    accessor.set(scope, "TestMsg.threadpoolSize", ""+12);
    
    scope =
        new ConfigScopeBuilder().forCluster(manager.getClusterName()).build();
    accessor.set(scope, "TestMsg.threadpoolSize", ""+8);
    
    for (int i = 0; i < NODE_NR; i++)
    {
      instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      
      _startCMResultMap.get(instanceName)._manager.getMessagingService().registerMessageHandlerFactory("TestMsg", new TestMessagingHandlerFactory());
      _startCMResultMap.get(instanceName)._manager.getMessagingService().registerMessageHandlerFactory("TestMsg2", new TestMessagingHandlerFactory2());
      
    
    }
    
    for (int i = 0; i < NODE_NR; i++)
    {
      instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      
      DefaultMessagingService svc = (DefaultMessagingService)(_startCMResultMap.get(instanceName)._manager.getMessagingService());
      HelixTaskExecutor helixExecutor = svc.getExecutor();
      ThreadPoolExecutor executor = (ThreadPoolExecutor)(helixExecutor._threadpoolMap.get("TestMsg"));
      
      ThreadPoolExecutor executor2 = (ThreadPoolExecutor)(helixExecutor._threadpoolMap.get("TestMsg2"));
      if(i != 0)
      {
        
        Assert.assertEquals(8, executor.getMaximumPoolSize());
      }
      else
      {
        Assert.assertEquals(12, executor.getMaximumPoolSize());
      }
      Assert.assertEquals(HelixTaskExecutor.DEFAULT_PARALLEL_TASKS, executor2.getMaximumPoolSize());
    }
  }
}
