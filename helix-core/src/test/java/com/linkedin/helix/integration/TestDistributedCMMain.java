package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.mock.controller.ClusterController;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;

public class TestDistributedCMMain extends ZkIntegrationTestBase
{

  @Test
  public void testDistributedCMMain() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterNamePrefix = className + "_" + methodName;
    final int n = 5;
    final int clusterNb = 10;

    System.out.println("START " + clusterNamePrefix + " at "
        + new Date(System.currentTimeMillis()));

    // setup 10 clusters
    for (int i = 0; i < clusterNb; i++)
    {
      String clusterName = clusterNamePrefix + "0_" + i;
      String participantName = "localhost" + i;
      String resourceName = "TestDB" + i;
      TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                              participantName, // participant name prefix
                              resourceName, // resource name prefix
                              1, // resources
                              8, // partitions per resource
                              n, // number of nodes
                              3, // replicas
                              "MasterSlave",
                              true); // do rebalance
    }

    // setup controller cluster
    final String controllerClusterName = "CONTROLLER_" + clusterNamePrefix;
    TestHelper.setupCluster("CONTROLLER_" + clusterNamePrefix, ZK_ADDR, 0, // controller
                                                                           // port
                            "controller", // participant name prefix
                            clusterNamePrefix, // resource name prefix
                            1, // resources
                            clusterNb, // partitions per resource
                            n, // number of nodes
                            3, // replicas
                            "LeaderStandby",
                            true); // do rebalance

    // start distributed cluster controllers
    ClusterController[] controllers = new ClusterController[n + n];
    for (int i = 0; i < n; i++)
    {
      controllers[i] =
          new ClusterController(controllerClusterName,
                                "controller_" + i,
                                ZK_ADDR,
                                HelixControllerMain.DISTRIBUTED.toString());
      controllers[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                      controllerClusterName),
                                                30000);
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // start first cluster
    MockParticipant[] participants = new MockParticipant[n];
    final String firstClusterName = clusterNamePrefix + "0_0";
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost0_" + (12918 + i);
      participants[i] =
          new MockParticipant(firstClusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");
    
    
    // add more controllers to controller cluster
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    for (int i = 0; i < n; i++)
    {
      String controller = "controller:" + (n + i);
      setupTool.addInstanceToCluster(controllerClusterName, controller);
    }
    setupTool.rebalanceStorageCluster(controllerClusterName, clusterNamePrefix + "0", 6);
    for (int i = n; i < 2 * n; i++)
    {
      controllers[i] =
          new ClusterController(controllerClusterName,
                                "controller_" + i,
                                ZK_ADDR,
                                HelixControllerMain.DISTRIBUTED.toString());
      controllers[i].syncStart();
    }

    
    // verify controller cluster
    result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                      controllerClusterName));
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");
    
    // verify first cluster
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");

    
    // stop controller_0-5
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(controllerClusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    for (int i = 0; i < n; i++)
    {
      LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
      String leaderName = leader.getId();
      int j = Integer.parseInt(leaderName.substring(leaderName.lastIndexOf('_') + 1));
      controllers[j].syncStop();
      
      result =
          ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                        controllerClusterName));
      Assert.assertTrue(result, "Controller cluster NOT in ideal state");
      
      result =
          ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                   firstClusterName));
      Assert.assertTrue(result, "first cluster NOT in ideal state");
    }

    
    // clean up
    // wait for all zk callbacks done
    System.out.println("Cleaning up...");
    Thread.sleep(2000);
    for (int i = 0; i < 5; i++)
    {
      result =
          ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                        controllerClusterName));
      controllers[i].syncStop();
    }
    
    // Thread.sleep(2000);
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterNamePrefix + " at "
        + new Date(System.currentTimeMillis()));

  }
}
