package com.linkedin.clustermanager;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;

public class TestStandAloneCMMain extends ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(TestStandAloneCMMain.class);
  
  @Test
  public void testStandAloneCMMain() throws Exception
  {
    logger.info("Run at " + new Date(System.currentTimeMillis()));
    
    TestHelper.startClusterController("-zkSvr " + ZK_ADDR + " -cluster " + CLUSTER_NAME +
          " -mode " + ClusterManagerMain.STANDALONE + " -controllerName controller_1");
    TestHelper.startClusterController("-zkSvr " + ZK_ADDR + " -cluster " + CLUSTER_NAME +
          " -mode " + ClusterManagerMain.STANDALONE + " -controllerName controller_2");
    
    stopCurrentLeader(CLUSTER_NAME);
    
    Thread.sleep(5000);
    boolean result = ClusterStateVerifier.verifyClusterStates(ZK_ADDR, CLUSTER_NAME);
    Assert.assertTrue(result);
    
    logger.info("End at " + new Date(System.currentTimeMillis())); 
  }

}
