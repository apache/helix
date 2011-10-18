package com.linkedin.clustermanager.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.StartCMResult;
import com.linkedin.clustermanager.controller.ClusterManagerMain;

public class TestStandAloneCMMain extends ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(TestStandAloneCMMain.class);
  
  @Test (groups = {"integrationTest"})
  public void testStandAloneCMMain() throws Exception
  {
    logger.info("Run at " + new Date(System.currentTimeMillis()));
    
    // TestHelper.startClusterController("-zkSvr " + ZK_ADDR + " -cluster " + CLUSTER_NAME +
    //      " -mode " + ClusterManagerMain.STANDALONE + " -controllerName controller_1");
    // TestHelper.startClusterController("-zkSvr " + ZK_ADDR + " -cluster " + CLUSTER_NAME +
    //      " -mode " + ClusterManagerMain.STANDALONE + " -controllerName controller_2");
    
    for (int i = 1; i <= 2; i++)
    {
      String controllerName = "controller_" + i;
      StartCMResult startResult =
          TestHelper.startClusterController(CLUSTER_NAME,
                                            controllerName,
                                            ZK_ADDR,
                                            ClusterManagerMain.STANDALONE,
                                            null);
      _threadMap.put(controllerName, startResult._thread);
      _managerMap.put(controllerName, startResult._manager);
    }
    
    Thread.sleep(2000);
    
    stopCurrentLeader(CLUSTER_NAME, _threadMap, _managerMap);
    
    // Thread.sleep(5000);
    // boolean result = ClusterStateVerifier.verifyClusterStates(ZK_ADDR, CLUSTER_NAME);
    // AssertJUnit.assertTrue(result);
    verifyIdealAndCurrentStateTimeout(CLUSTER_NAME);
    
    logger.info("End at " + new Date(System.currentTimeMillis())); 
  }

}
