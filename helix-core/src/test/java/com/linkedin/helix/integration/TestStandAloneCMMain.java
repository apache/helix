package com.linkedin.helix.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestStandAloneCMMain extends ZkStandAloneCMTestBase
{
  private static Logger logger = Logger.getLogger(TestStandAloneCMMain.class);

  @Test()
  public void testStandAloneCMMain() throws Exception
  {
    logger.info("RUN testStandAloneCMMain() at " + new Date(System.currentTimeMillis()));

    for (int i = 1; i <= 2; i++)
    {
      String controllerName = "controller_" + i;
      StartCMResult startResult =
          TestHelper.startController(CLUSTER_NAME,
                                            controllerName,
                                            ZK_ADDR,
                                            HelixControllerMain.STANDALONE);
      _startCMResultMap.put(controllerName, startResult);
    }

    stopCurrentLeader(_zkClient, CLUSTER_NAME, _startCMResultMap);
    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    logger.info("STOP testStandAloneCMMain() at " + new Date(System.currentTimeMillis()));
  }

}
