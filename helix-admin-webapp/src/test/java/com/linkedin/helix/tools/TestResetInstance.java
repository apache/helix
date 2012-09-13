package com.linkedin.helix.tools;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.mock.controller.ClusterController;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockParticipant.ErrTransition;
import com.linkedin.helix.webapp.resources.JsonParameters;

public class TestResetInstance extends AdminTestBase
{
  @Test
  public void testResetInstance() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            n, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance
    
//    // start admin thread
//    AdminThread adminThread = new AdminThread(ZK_ADDR, _port);
//    adminThread.start();

    // start controller
    ClusterController controller =
        new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>()
    {
      {
        put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
        put("OFFLINE-SLAVE", TestHelper.setOf("TestDB0_8"));
      }
    };

    // start mock participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0)
      {
        participants[i] =
            new MockParticipant(clusterName,
                                instanceName,
                                ZK_ADDR,
                                new ErrTransition(errPartitions));
      }
      else
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR);
      }
      participants[i].syncStart();
    }

    // verify cluster
    Map<String, Map<String, String>> errStateMap =
        new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");
    errStateMap.get("TestDB0").put("TestDB0_8", "localhost_12918");
    boolean result =
        ClusterStateVerifier.verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                       clusterName,
                                                                                                       errStateMap)));
    Assert.assertTrue(result, "Cluster verification fails");

    // reset node "localhost_12918"
    participants[0].setTransition(null);
    String hostName = "localhost_12918";
    String instanceUrl = "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/instances/" + hostName;

    Map<String, String> paramMap = new HashMap<String, String>();
    paramMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.resetInstance);
    TestHelixAdminScenariosRest.assertSuccessPostOperation(instanceUrl, paramMap, false);
    
    result =
        ClusterStateVerifier.verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                                       clusterName)));
    Assert.assertTrue(result, "Cluster verification fails");
    
    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
//    adminThread.stop();
    controller.syncStop();
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
