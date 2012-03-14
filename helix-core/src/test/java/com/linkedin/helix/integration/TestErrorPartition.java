package com.linkedin.helix.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockParticipant.ErrTransition;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestErrorPartition extends ZkIntegrationTestBase
{
  @Test()
  public void testErrorPartition() throws Exception
  {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START testErrorPartition() at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, 10, 5, 3,
        "MasterSlave", true);

    TestHelper
        .startController(clusterName, "controller_0", ZK_ADDR, HelixControllerMain.STANDALONE);
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0)
      {
        Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>() {
          {
            put("SLAVE-MASTER", TestHelper.setOf("TestDB0_0"));
          }
        };
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR,
            new ErrTransition(errPartitions));
      } else
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR);
      }
      new Thread(participants[i]).start();
    }

    Map<String, Map<String, String>> errStates = new HashMap<String, Map<String, String>>();
    errStates.put("TestDB0", new HashMap<String, String>());
    errStates.get("TestDB0").put("TestDB0_0", "localhost_12918");
    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName, errStates));
    Assert.assertTrue(result);

    
    Map<String, Set<String>> errorStateMap = new HashMap<String, Set<String>>() {
      {
        put("TestDB0_0", TestHelper.setOf("localhost_12918"));
      }
    };

    // verify "TestDB0_0", "localhost_12918" is in ERROR state
    TestHelper.verifyState(clusterName, ZK_ADDR, errorStateMap, "ERROR");

    // disable a partition on a node with error state
    ZKHelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.enablePartition(clusterName, "localhost_12918", "TestDB0", "TestDB0_0", false);

    result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName, errStates));
    Assert.assertTrue(result);

    TestHelper.verifyState(clusterName, ZK_ADDR, errorStateMap, "ERROR");

    // disable a node with error state
    tool.enableInstance(clusterName, "localhost_12918", false);

    result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName, errStates));
    Assert.assertTrue(result);

    System.out.println("END testErrorPartition() at " + new Date(System.currentTimeMillis()));
  }
}
