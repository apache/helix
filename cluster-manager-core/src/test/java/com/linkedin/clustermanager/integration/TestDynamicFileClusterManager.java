package com.linkedin.clustermanager.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;

public class TestDynamicFileClusterManager extends FileCMTestBase
{
  private static Logger logger = Logger.getLogger(TestDynamicFileClusterManager.class);
  
  @Test (groups = {"integrationTest"})
  public void testDynamicFileClusterManager() 
  throws Exception 
  {
    logger.info("RUN testDynamicFileClusterManager() at " + new Date(System.currentTimeMillis()));

    // add a new db
    _mgmtTool.addResourceGroup(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 0);
//    Thread.sleep(10000);
    
    // verify current state
    ZNRecord idealStates = _accessor.getProperty(PropertyType.IDEALSTATES, "MyDB");
//    ZNRecord curStates =  _accessor.getProperty( PropertyType.CURRENTSTATES, "localhost_12918",
//                                              _manager.getSessionId(), "MyDB");
//    boolean result = verifyCurStateAndIdealState(curStates, idealStates, "localhost_12918", "MyDB");
//    AssertJUnit.assertTrue(result);
    
    try
    {
      boolean result = false;
      int j = 0;
      for (; j < 24; j++)
      {
        Thread.sleep(2000);
        ZNRecord curStates =  _accessor.getProperty( PropertyType.CURRENTSTATES, "localhost_12918",
            _manager.getSessionId(), "MyDB");
        result = verifyCurStateAndIdealState(curStates, idealStates, "localhost_12918", "MyDB");
        if (result == true)
        {
          break;
        }
      }
      // debug
      System.err.println("verifyIdealAndCurrentState(): wait "
          + ((j + 1) * 2000) + "ms to verify (" + result + ") participant:"
          + "localhost_12918");

      if (result == false)
      {
        System.err.println("verifyIdealAndCurrentState() fails for participant:"
            + "localhost_12918");
      }
      AssertJUnit.assertTrue(result);
    } catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    
    // drop db
    _mgmtTool.dropResourceGroup(CLUSTER_NAME, "MyDB");
    // Thread.sleep(10000);
    
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (12918 + i);
//      verifyEmptyCurrentState(instanceName, "MyDB");
      
      try
      {
        boolean result = false;
        int j = 0;
        for (; j < 24; j++)
        {
          Thread.sleep(2000);
          result = verifyEmptyCurrentState(instanceName, "MyDB");
          if (result == true)
          {
            break;
          }
        }
        // debug
        System.err.println("verifyEmptyCurrentState(): wait "
            + ((j + 1) * 2000) + "ms to verify (" + result + ") participant:"
            + instanceName);

        if (result == false)
        {
          System.err.println("verifyEmptyCurrentState() fails for participant:"
              + instanceName);
        }
        AssertJUnit.assertTrue(result);
      } catch (InterruptedException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    
    logger.info("END at " + new Date(System.currentTimeMillis()));
    super.afterClass();
  }
  
}
