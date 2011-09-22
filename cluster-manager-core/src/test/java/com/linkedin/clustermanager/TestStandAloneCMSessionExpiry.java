package com.linkedin.clustermanager;

import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.tools.ClusterStateVerifier;

public class TestStandAloneCMSessionExpiry extends ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(TestStandAloneCMSessionExpiry.class);
  
  @Test
  public void testStandAloneCMSessionExpiry() 
  throws InterruptedException, IOException
  {
    logger.info("RUN at " + new Date(System.currentTimeMillis()));
    
    simulateSessionExpiry(_participantZkClients[0]);
    
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, "MyDB", 10, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);
    Thread.sleep(8000);
    boolean result = ClusterStateVerifier.VerifyClusterStates(ZK_ADDR, CLUSTER_NAME);
    Assert.assertTrue(result);
    logger.info("cluster:" + CLUSTER_NAME + " after pariticipant session expiry result:" + result);

    
    simulateSessionExpiry(_controllerZkClient);
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, "MyDB2", 8, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB2", 3);
    Thread.sleep(8000);
    
    result = ClusterStateVerifier.VerifyClusterStates(ZK_ADDR, CLUSTER_NAME);
    Assert.assertTrue(result);
    logger.info("cluster:" + CLUSTER_NAME + " after controller session expiry result:" + result);
    
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }

}
