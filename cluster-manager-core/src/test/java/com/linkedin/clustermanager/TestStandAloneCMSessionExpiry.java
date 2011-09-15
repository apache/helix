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
  public void testZkClusterManagerHandler() 
  throws InterruptedException, IOException
  {
    logger.info("RUN at " + new Date(System.currentTimeMillis()));
    
    simulateSessionExpiry(_participantZkClients[0]);
    
    _setupTool.addResourceGroupToCluster(storageCluster, "MyDB", 10, stateModel);
    _setupTool.rebalanceStorageCluster(storageCluster, "MyDB", 3);
    Thread.sleep(8000);
    boolean result = ClusterStateVerifier.VerifyClusterStates(zkAddr, storageCluster);
    Assert.assertTrue(result);
    logger.info("cluster:" + storageCluster + " after pariticipant session expiry result:" + result);

    
    simulateSessionExpiry(_controllerZkClient);
    _setupTool.addResourceGroupToCluster(storageCluster, "MyDB2", 8, stateModel);
    _setupTool.rebalanceStorageCluster(storageCluster, "MyDB2", 3);
    Thread.sleep(8000);
    
    result = ClusterStateVerifier.VerifyClusterStates(zkAddr, storageCluster);
    Assert.assertTrue(result);
    logger.info("cluster:" + storageCluster + " after controller session expiry result:" + result);
    
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }

}
