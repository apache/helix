package com.linkedin.clustermanager;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;

public class TestDynamicFileClusterManager extends FileClusterManagerHandler
{
  private static Logger logger = Logger.getLogger(TestDynamicFileClusterManager.class);
  
  @Test
  public void testDynamicFileClusterManager() 
  throws InterruptedException 
  {
    logger.info("RUN at " + new Date(System.currentTimeMillis()));

    // add a new db
    mgmtTool.addResourceGroup(storageCluster, "MyDB", 6, stateModel);
    rebalanceStorageCluster(storageCluster, "MyDB", 0);
    Thread.sleep(5000);
    
    // verify current state
    ZNRecord idealStates = accessor.getClusterProperty(ClusterPropertyType.IDEALSTATES, "MyDB");
    ZNRecord curStates =  accessor.getInstanceProperty("localhost_12918", InstancePropertyType.CURRENTSTATES, 
                                              manager.getSessionId(), "MyDB");
    boolean result = verifyCurStateAndIdealState(curStates, idealStates, "localhost_12918", "MyDB");
    Assert.assertTrue(result);
    
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }
  
}
