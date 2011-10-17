package com.linkedin.clustermanager.zk;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.StartCMResult;

public class TestParticipantNameCollision extends ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(TestParticipantNameCollision.class);
  // static final AtomicInteger _exceptionCounter = new AtomicInteger();
  
  @Test
  public void testParticiptantNameCollision() throws Exception
  {
    logger.info("RUN at " + new Date(System.currentTimeMillis()));
    
    StartCMResult result = null;
    // int i = 0;
    for (int i = 0; i < 1; i++)
    {
      String instanceName = "localhost_" + (START_PORT + i);
      try
      {
        // the call fails on getClusterManagerForParticipant()
        // no threads start
        result = TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName, null);
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
    
    Thread.sleep(20000);
  
    try
    {
      boolean isConnected = false;
      int i = 0;
      for (i = 0; i < 24; i++)
      {
        Thread.sleep(2000);
        isConnected = result._manager.isConnected();
        if (isConnected == false)
        {
          break;
        }
      }
      // debug
      System.out.println("testParticiptantNameCollision(): wait " + ((i + 1) * 2000) + "ms to detect name collision ("
          + !isConnected + ") cluster:" + CLUSTER_NAME);
      if (isConnected == true)
      {
        System.out.println("testParticiptantNameCollision() fails");
      }
      AssertJUnit.assertFalse(isConnected);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }
}
