package com.linkedin.helix.messaging.handling;

import java.util.concurrent.ThreadPoolExecutor;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ConfigAccessor;
import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.integration.ZkStandAloneCMTestBase;
import com.linkedin.helix.messaging.DefaultMessagingService;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestResourceThreadpoolSize extends ZkStandAloneCMTestBase
{
  @Test
  public void TestThreadPoolSizeConfig()
  {
    String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + 0);
    HelixManager manager = _startCMResultMap.get(instanceName)._manager;
    ConfigAccessor accessor = manager.getConfigAccessor();
    ConfigScope scope =
        new ConfigScopeBuilder().forCluster(manager.getClusterName()).forResource("NextDB").build();
    accessor.set(scope, HelixTaskExecutor.MAX_THREADS, ""+12);
    
    _setupTool.addResourceToCluster(CLUSTER_NAME, "NextDB", 64, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "NextDB", 3);
    
    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
    
    long taskcount = 0; 
    for (int i = 0; i < NODE_NR; i++)
    {
      instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      
      DefaultMessagingService svc = (DefaultMessagingService)(_startCMResultMap.get(instanceName)._manager.getMessagingService());
      HelixTaskExecutor helixExecutor = svc.getExecutor();
      ThreadPoolExecutor executor = (ThreadPoolExecutor)(helixExecutor._threadpoolMap.get(MessageType.STATE_TRANSITION + "." + "NextDB"));
      Assert.assertEquals(12, executor.getMaximumPoolSize());
      taskcount += executor.getCompletedTaskCount();
      Assert.assertTrue(executor.getCompletedTaskCount() > 0);
    }
    Assert.assertEquals(taskcount, 64 * 4);
  }
}
