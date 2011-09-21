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
    
    // final String clusterName = CLUSTER_PREFIX + "_" + this.getClass().getName();
    simulateSessionExpiry(_participantZkClients[0]);
    
    _setupTool.addResourceGroupToCluster(clusterName, "MyDB", 10, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(clusterName, "MyDB", 3);
    Thread.sleep(8000);
    boolean result = ClusterStateVerifier.VerifyClusterStates(ZK_ADDR, clusterName);
    Assert.assertTrue(result);
    logger.info("cluster:" + clusterName + " after pariticipant session expiry result:" + result);

    
    simulateSessionExpiry(_controllerZkClient);
    _setupTool.addResourceGroupToCluster(clusterName, "MyDB2", 8, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(clusterName, "MyDB2", 3);
    Thread.sleep(8000);
    
    result = ClusterStateVerifier.VerifyClusterStates(ZK_ADDR, clusterName);
    Assert.assertTrue(result);
    logger.info("cluster:" + clusterName + " after controller session expiry result:" + result);
    
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }

}
