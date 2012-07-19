package com.linkedin.helix.controller;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.integration.ZkStandAloneCMTestBase;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;

public class TestControllerRebalancingTimer extends ZkStandAloneCMTestBase
{
  private static Logger                LOG               =
      Logger.getLogger(TestControllerRebalancingTimer.class);

  @BeforeClass
  public void beforeClass() throws Exception
  {
//    Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("START " + CLASS_NAME + " at "
        + new Date(System.currentTimeMillis()));

    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    String namespace = "/" + CLUSTER_NAME;
    if (_zkClient.exists(namespace))
    {
      _zkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++)
    {
      String storageNodeName = PARTICIPANT_PREFIX + ":" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);
    String scopesStr = "CLUSTER=" + CLUSTER_NAME + ",RESOURCE="+TEST_DB;
    _setupTool.setConfig(scopesStr, "RebalanceTimerPeriod=500");
    // start dummy participants
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      if (_startCMResultMap.get(instanceName) != null)
      {
        LOG.error("fail to start particpant:" + instanceName
            + "(participant with same name already exists)");
      }
      else
      {
        StartCMResult result =
            TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName);
        _startCMResultMap.put(instanceName, result);
      }
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    StartCMResult startResult =
        TestHelper.startController(CLUSTER_NAME,
                                   controllerName,
                                   ZK_ADDR,
                                   HelixControllerMain.STANDALONE);
    _startCMResultMap.put(controllerName, startResult);
    
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              CLUSTER_NAME));

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 CLUSTER_NAME));
    Assert.assertTrue(result);
  }
  
  @Test
  public void TestRebalancingTimer() throws Exception
  {
    String controllerName = CONTROLLER_PREFIX + "_0";
    GenericHelixController controller = new GenericHelixController();
    _startCMResultMap.get(controllerName)._manager.addIdealStateChangeListener(controller);
    
    Assert.assertTrue(controller._rebalanceTimer != null);
    Assert.assertEquals(controller._timerPeriod, 500);

    String scopesStr = "CLUSTER=" + CLUSTER_NAME + ",RESOURCE="+TEST_DB;
    _setupTool.setConfig(scopesStr, "RebalanceTimerPeriod=200");
    

    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);
    
    Thread.sleep(1000);
    Assert.assertTrue(controller._rebalanceTimer != null);
    Assert.assertEquals(controller._timerPeriod, 200);
  }
}
