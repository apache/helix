package com.linkedin.clustermanager;

import java.util.Date;


import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;

public class TestDropResourceFileCM extends FileClusterManagerHandler
{
  private static Logger logger = Logger.getLogger(TestDropResourceFileCM.class);
  
  @Test
  public void testDropResourceFileCM() 
  throws InterruptedException 
  {
    logger.info("RUN at " + new Date(System.currentTimeMillis()));
    
    // add a db to be dropped
    mgmtTool.addResourceGroup(storageCluster, "DropDB", 10, stateModel);
    rebalanceStorageCluster(storageCluster, "DropDB", 2);
    Thread.sleep(10000);
    
    // verify current state
    ZNRecord idealStates = accessor.getClusterProperty(ClusterPropertyType.IDEALSTATES, "DropDB");
    ZNRecord curStates =  accessor.getInstanceProperty("localhost_12918", InstancePropertyType.CURRENTSTATES, 
                                              manager.getSessionId(), "DropDB");
    boolean result = verifyCurStateAndIdealState(curStates, idealStates,"localhost_12918", "DropDB");
    Assert.assertTrue(result);
    
    mgmtTool.dropResourceGroup(storageCluster, "DropDB");
    Thread.sleep(10000);
    
    for (int i = 0; i < storageNodeNr; i++)
    {
      String instanceName = "localhost_" + (12918 + i);
      verifyEmptyCurrentState(instanceName, "DropDB");
    }
    
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }
}
