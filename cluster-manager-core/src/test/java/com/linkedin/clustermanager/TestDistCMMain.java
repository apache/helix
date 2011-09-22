package com.linkedin.clustermanager;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

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
      String controller = "controller:" + i;
      _setupTool.addInstanceToCluster(CONTROLLER_CLUSTER, controller);
    }
    _setupTool.rebalanceStorageCluster(CONTROLLER_CLUSTER, 
                 CLUSTER_PREFIX + "_" + CLASS_NAME, 10);

    // start extra cluster controllers in distributed mode
    for (int i = 0; i < 5; i++)
    {
      String controller = "controller_" + i;
      Thread thread = TestHelper.startClusterController("-zkSvr " + ZK_ADDR + " -cluster " + CONTROLLER_CLUSTER + 
           " -mode " + ClusterManagerMain.DISTRIBUTED + " -controllerName " + controller);
      _threadMap.put(controller, thread);
    }
    
    Thread.sleep(10000);
    
    // stop controllers
    for (int i = 0; i < NODE_NR; i++)
    {
      stopCurrentLeader(CONTROLLER_CLUSTER);
      Thread.sleep(5000);
    }
    
    assertLeader(CONTROLLER_CLUSTER);
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }
}
