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
    logger.info("RUN testStandAloneCMMain() at " + new Date(System.currentTimeMillis()));

    for (int i = 1; i <= 2; i++)
    {
      String controllerName = "controller_" + i;
      StartCMResult startResult =
          TestHelper.startController(CLUSTER_NAME,
                                            controllerName,
                                            ZK_ADDR,
                                            ClusterManagerMain.STANDALONE);
      _startCMResultMap.put(controllerName, startResult);
    }

    stopCurrentLeader(_zkClient, CLUSTER_NAME, _startCMResultMap);
    verifyCluster();

    logger.info("STOP testStandAloneCMMain() at " + new Date(System.currentTimeMillis()));
  }

}
