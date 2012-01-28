package com.linkedin.helix.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;

public class TestParticipantNameCollision extends ZkStandAloneCMTestBase
{
  private static Logger logger = Logger.getLogger(TestParticipantNameCollision.class);

  @Test()
  public void testParticiptantNameCollision() throws Exception
  {
    logger.info("RUN TestParticipantNameCollision() at " + new Date(System.currentTimeMillis()));

    StartCMResult result = null;
    for (int i = 0; i < 1; i++)
    {
      String instanceName = "localhost_" + (START_PORT + i);
      try
      {
        // the call fails on getClusterManagerForParticipant()
        // no threads start
        result = TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName);
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }


    Thread.sleep(30000);
    TestHelper.verifyWithTimeout("verifyNotConnected", result._manager);

    logger.info("STOP TestParticipantNameCollision() at " + new Date(System.currentTimeMillis()));
  }
}
