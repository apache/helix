package com.linkedin.clustermanager.zk;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.DummyProcessResult;

public class TestParticipantNameCollision extends ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(TestParticipantNameCollision.class);
  // static final AtomicInteger _exceptionCounter = new AtomicInteger();
  
  @Test
  public void testParticiptantNameCollision() throws Exception
  {
    logger.info("RUN at " + new Date(System.currentTimeMillis()));
    
    DummyProcessResult result = null;
    int i = 0;
    for (; i < 1; i++)
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
  
    Assert.assertFalse(result._manager.isConnected());
    // Assert.assertEquals(i, _exceptionCounter.get());
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }
}
