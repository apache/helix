package com.linkedin.clustermanager.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.StartCMResult;

public class TestDistClusterController extends ZkDistCMTestBase
{
  private static Logger LOG = Logger.getLogger(TestDistClusterController.class);

  @Test()
  public void testDistClusterController() throws Exception
  {
    LOG.info("RUN testDistClusterController() at " + new Date(System.currentTimeMillis()));
    stopCurrentLeader(_zkClient, CONTROLLER_CLUSTER, _startCMResultMap);

    // setup the second cluster
    final String secondCluster = CLUSTER_PREFIX + "_" + CLASS_NAME + "_1";
    setupStorageCluster(_setupTool, secondCluster, "MyDB", 10, PARTICIPANT_PREFIX, 13918, STATE_MODEL, 3);

    // start dummy participants on the second cluster
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (13918 + i);
      if (_startCMResultMap.get(instanceName) != null)
      {
        LOG.error("fail to start participant:" + instanceName
                  + "(participant with the same name already running)");
      }
      else
      {
        StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR, secondCluster, instanceName, null);
        _startCMResultMap.put(instanceName, result);
      }
    }

    verifyClusters();
    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "MyDB",
                                 10,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_PREFIX + "_" + CLASS_NAME + "_1"),
                                 _zkClient);

    LOG.info("STOP testDistClusterController() at " + new Date(System.currentTimeMillis()));
  }

}
