package com.linkedin.clustermanager;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper.DummyProcessResult;

public class TestParticipantNameCollision extends ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(TestParticipantNameCollision.class);
  static final AtomicInteger _exceptionCounter = new AtomicInteger();
  
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
      catch (ClusterManagerException e)
      {
        _exceptionCounter.addAndGet(1);
        logger.info("exceptionCounter:" + _exceptionCounter.get());
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
    
    Thread.sleep(40000);
  
    Assert.assertFalse(result._manager.isConnected());
    // Assert.assertEquals(i, _exceptionCounter.get());
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }
}
