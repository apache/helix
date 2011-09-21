package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

public class TestDistClusterController extends ZkDistCMHandler
{
  private static Logger logger = Logger.getLogger(TestDistClusterController.class);

  @Test
  public void testDistClusterController() throws Exception
  {
    logger.info("Run at " + new Date(System.currentTimeMillis()));

    // stop the current cluster controller
    // final String controllerCluster = CONTROLLER_CLUSTER_PREFIX + "_" + CLASS_NAME;
    stopCurrentLeader(CONTROLLER_CLUSTER);

    Thread.sleep(3000);

    // make sure a new leader is selected
    assertLeader(CONTROLLER_CLUSTER);

    // setup storage cluster: ESPRESSO_STORAGE_1
    final String secondCluster = CLUSTER_PREFIX + "_" + CLASS_NAME + "_1";
    setupStorageCluster(_setupTool, secondCluster, "MyDB", 10, 13918, STATE_MODEL);

    // start dummy participants for the second ESPRESSO_STORAGE cluster
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (13918 + i);
      if (_threadMap.get(instanceName) != null)
      {
        logger.error("fail to start participant:" + instanceName
            + " because there is already a thread with same instanceName running");
      }
      else
      {
        Thread thread =
            TestHelper.startDummyProcess(ZK_ADDR, secondCluster, instanceName, null);
        _threadMap.put(instanceName, thread);
      }
    }

    Thread.sleep(10000);

    List<String> clusterNames = new ArrayList<String>();
    final String firstCluster = CLUSTER_PREFIX + "_" + CLASS_NAME + "_0";
    clusterNames.add(firstCluster);
    clusterNames.add(secondCluster);
    verifyIdealAndCurrentState(clusterNames);

    logger.info("End at " + new Date(System.currentTimeMillis()));
  }

}
