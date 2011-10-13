package com.linkedin.clustermanager.zk;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.DummyProcessResult;
import com.linkedin.clustermanager.controller.ClusterManagerMain;

public class TestDistCMMain extends ZkDistCMHandler
{
  
  private static Logger logger = Logger.getLogger(TestDistCMMain.class);
  
  @Test
  public void testDistCMMain() throws Exception
  {
    logger.info("RUN at " + new Date(System.currentTimeMillis()));
    
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
      
      DummyProcessResult result = TestHelper
          .startClusterController(CONTROLLER_CLUSTER, controller, ZK_ADDR, 
                                  ClusterManagerMain.DISTRIBUTED, null);
      _threadMap.put(controller, result._thread);
      _managerMap.put(controller, result._manager);
    }
    
    // List<String> clusterNames = new ArrayList<String>();
    // clusterNames.add(CONTROLLER_CLUSTER);
    verifyIdealAndCurrentStateTimeout(CONTROLLER_CLUSTER);
    // Thread.sleep(5000);
    
    // stop controllers
    for (int i = 0; i < NODE_NR; i++)
    {
      Thread.sleep(5000);
      stopCurrentLeader(CONTROLLER_CLUSTER, _threadMap, _managerMap);
    }
    
    Thread.sleep(5000);
    logger.info("END at " + new Date(System.currentTimeMillis()));
    // super.afterClass();
  }
}
