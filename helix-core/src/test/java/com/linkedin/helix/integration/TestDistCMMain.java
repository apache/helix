package com.linkedin.helix.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.controller.ClusterManagerMain;

public class TestDistCMMain extends ZkDistCMTestBase
{
  private static Logger LOG = Logger.getLogger(TestDistCMMain.class);

  @Test
  public void testDistCMMain() throws Exception
  {
    LOG.info("RUN testDistCMMain() at " + new Date(System.currentTimeMillis()));

    verifyClusters();

    // add more controllers to controller cluster
    for (int i = 0; i < NODE_NR; i++)
    {
      String controller = CONTROLLER_PREFIX + ":" + (NODE_NR + i);
      _setupTool.addInstanceToCluster(CONTROLLER_CLUSTER, controller);
    }
    _setupTool.rebalanceStorageCluster(CONTROLLER_CLUSTER,
                                       CLUSTER_PREFIX + "_" + CLASS_NAME, 10);

    // start extra cluster controllers in distributed mode
    for (int i = 0; i < 5; i++)
    {
      String controller = CONTROLLER_PREFIX + "_" + (NODE_NR + i);

      StartCMResult result = TestHelper.startController(CONTROLLER_CLUSTER,
                                                               controller, ZK_ADDR,
                                                               ClusterManagerMain.DISTRIBUTED);
      _startCMResultMap.put(controller, result);
    }

    verifyClusters();

    for (int i = 0; i < NODE_NR; i++)
    {
      stopCurrentLeader(_zkClient, CONTROLLER_CLUSTER, _startCMResultMap);
      verifyClusters();
    }

    LOG.info("STOP testDistCMMain() at " + new Date(System.currentTimeMillis()));

  }
}
