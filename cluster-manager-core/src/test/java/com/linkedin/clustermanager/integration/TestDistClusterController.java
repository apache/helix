package com.linkedin.clustermanager.integration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.StartCMResult;

public class TestDistClusterController extends ZkDistCMTestBase
{
  private static Logger logger = Logger.getLogger(TestDistClusterController.class);

  @Test()
  public void testDistClusterController() throws Exception
  {
    logger.info("Run at " + new Date(System.currentTimeMillis()));
    Thread.sleep(5000);
    stopCurrentLeader(_zkClient, CONTROLLER_CLUSTER, _startCMResultMap);

    // setup the second cluster
    final String secondCluster = CLUSTER_PREFIX + "_" + CLASS_NAME + "_1";
    setupStorageCluster(_setupTool, secondCluster, "MyDB", 10, PARTICIPANT_PREFIX, 13918, STATE_MODEL);

    // start dummy participants on the second cluster
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (13918 + i);
      if (_startCMResultMap.get(instanceName) != null)
      {
        logger.error("fail to start participant:" + instanceName
                     + "(participant with the same name already running)");
      }
      else
      {
        StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR, secondCluster, instanceName, null);
        _startCMResultMap.put(instanceName, result);
      }
    }

    List<String> clusterNames = new ArrayList<String>();
    final String firstCluster = CLUSTER_PREFIX + "_" + CLASS_NAME + "_0";
    clusterNames.add(firstCluster);
    clusterNames.add(secondCluster);
    verifyIdealAndCurrentStateTimeout(clusterNames);

    logger.info("End at " + new Date(System.currentTimeMillis()));
  }

}
