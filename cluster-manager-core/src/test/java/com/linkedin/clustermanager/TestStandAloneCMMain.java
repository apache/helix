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
    logger.info("Run testStandAloneCMMain() at " + new Date(System.currentTimeMillis()));
    
    final String clusterName = CLUSTER_PREFIX + "_" + this.getClass().getName();
    TestHelper.startClusterController("-zkSvr " + ZK_ADDR + " -cluster " + clusterName +
          " -mode " + ClusterManagerMain.STANDALONE + " -controllerName controller_1");
    TestHelper.startClusterController("-zkSvr " + ZK_ADDR + " -cluster " + clusterName +
          " -mode " + ClusterManagerMain.STANDALONE + " -controllerName controller_2");
    
    stopCurrentLeader(clusterName);
    
    Thread.sleep(5000);
    boolean result = ClusterStateVerifier.VerifyClusterStates(ZK_ADDR, clusterName);
    Assert.assertTrue(result);
    
    logger.info("End testStandAloneCMMain() at " + new Date(System.currentTimeMillis())); 
  }

}
