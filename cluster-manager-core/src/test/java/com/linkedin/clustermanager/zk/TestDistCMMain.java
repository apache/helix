package com.linkedin.clustermanager.zk;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
      // Thread thread = TestHelper.startClusterController("-zkSvr " + ZK_ADDR + " -cluster " + CONTROLLER_CLUSTER + 
      //     " -mode " + ClusterManagerMain.DISTRIBUTED + " -controllerName " + controller);
      
      DummyProcessResult result = TestHelper
          .startClusterController(CONTROLLER_CLUSTER, controller, ZK_ADDR, 
                                  ClusterManagerMain.DISTRIBUTED, null);
      _threadMap.put(controller, result._thread);
      _managerMap.put(controller, result._manager);
    }
    
    /*
    try
    {
      boolean result = false;
      int i = 0;
      for ( ; i < 24; i++)
      {
        Thread.sleep(2000);
        result = ClusterStateVerifier.verifyClusterStates(ZK_ADDR, CONTROLLER_CLUSTER);
        if (result == true)
        {
          break;
        }
      }
      // debug
      System.out.println("testDistCMMain(): wait " + ((i+1) * 2000) 
                         + "ms to verify (" + result + ") cluster:" + CONTROLLER_CLUSTER);
      if (result == false)
      {
        System.out.println("testDistCMMain() verification fails");
      }
      Assert.assertTrue(result);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    */
    List<String> clusterNames = new ArrayList<String>();
    clusterNames.add(CONTROLLER_CLUSTER);
    verifyIdealAndCurrentStateTimeout(clusterNames);
    
    // stop controllers
    for (int i = 0; i < NODE_NR; i++)
    {
      stopCurrentLeader(CONTROLLER_CLUSTER, _threadMap, _managerMap);
      // Thread.sleep(5000);
    }
    
    // assertLeader(CONTROLLER_CLUSTER);
    logger.info("END at " + new Date(System.currentTimeMillis()));
  }
}
