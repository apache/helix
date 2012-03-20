package com.linkedin.helix.manager.zk;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.integration.ZkStandAloneCMTestBase;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestLiveInstanceBounce extends ZkStandAloneCMTestBase
{
  @Test 
  public void testInstanceBounce() throws Exception
  {
    String controllerName = CONTROLLER_PREFIX + "_0";
    StartCMResult controllerResult = _startCMResultMap.get(controllerName);
    ZKHelixManager controller = (ZKHelixManager) controllerResult._manager;
    int handlerSize = controller.getHandlers().size();
    
    for (int i = 0; i < 2; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      // kill 2 participants
      _startCMResultMap.get(instanceName)._manager.disconnect();
      _startCMResultMap.get(instanceName)._thread.interrupt();
      try
      {
        Thread.sleep(1000);
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
      // restart the participant
      StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName);
      _startCMResultMap.put(instanceName, result);
    }
    Thread.currentThread().sleep(2000);
    
    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
    // When a new live instance is created, we still add current state listener to it thus number should increase by 2
    Assert.assertEquals( controller.getHandlers().size(), handlerSize + 2);
  }
}
