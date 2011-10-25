package com.linkedin.clustermanager.integration;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;

public class TestDynamicFileClusterManager extends FileCMTestBase
{
  private static Logger logger = Logger.getLogger(TestDynamicFileClusterManager.class);
  
  @Test (groups = {"integrationTest"})
  public void testDynamicFileClusterManager() 
  throws InterruptedException 
  {
    logger.info("RUN testDynamicFileClusterManager() at " + new Date(System.currentTimeMillis()));

    // add a new db
    _mgmtTool.addResourceGroup(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 0);
    Thread.sleep(10000);
    
    // verify current state
    ZNRecord idealStates = _accessor.getProperty(PropertyType.IDEALSTATES, "MyDB");
    ZNRecord curStates =  _accessor.getProperty( PropertyType.CURRENTSTATES, "localhost_12918",
                                              _manager.getSessionId(), "MyDB");
    boolean result = verifyCurStateAndIdealState(curStates, idealStates, "localhost_12918", "MyDB");
    AssertJUnit.assertTrue(result);
    
    /*
    // drop db
    _mgmtTool.dropResourceGroup(CLUSTER_NAME, "MyDB");
    Thread.sleep(10000);
    
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (12918 + i);
      verifyEmptyCurrentState(instanceName, "MyDB");
    }
    */
    
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }
  
}
