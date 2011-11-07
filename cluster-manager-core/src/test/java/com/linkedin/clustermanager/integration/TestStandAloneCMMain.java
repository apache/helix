package com.linkedin.clustermanager.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.StartCMResult;
import com.linkedin.clustermanager.controller.ClusterManagerMain;

public class TestStandAloneCMMain extends ZkStandAloneCMTestBase
{
  private static Logger logger = Logger.getLogger(TestStandAloneCMMain.class);

  @Test()
  public void testStandAloneCMMain() throws Exception
  {
    logger.info("Run at " + new Date(System.currentTimeMillis()));

    for (int i = 1; i <= 2; i++)
    {
      String controllerName = "controller_" + i;
      StartCMResult startResult =
          TestHelper.startClusterController(CLUSTER_NAME,
                                            controllerName,
                                            ZK_ADDR,
                                            ClusterManagerMain.STANDALONE,
                                            null);
      _startCMResultMap.put(controllerName, startResult);
    }

    Thread.sleep(2000);

    stopCurrentLeader(_zkClient, CLUSTER_NAME, _startCMResultMap);

    verifyIdealAndCurrentStateTimeout(CLUSTER_NAME);

    logger.info("End at " + new Date(System.currentTimeMillis()));
  }

}
