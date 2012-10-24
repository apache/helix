package org.apache.helix.integration;

import java.util.Date;

import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.mock.storage.MockParticipant;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCarryOverBadCurState extends ZkIntegrationTestBase
{
  @Test
  public void testCarryOverBadCurState() throws Exception
  {
    System.out.println("START testCarryOverBadCurState at "
        + new Date(System.currentTimeMillis()));

    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // add a bad current state
    ZNRecord badCurState = new ZNRecord("TestDB0");
    String path = PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName, "localhost_12918", "session_0", "TestDB0");
    _gZkClient.createPersistent(path, true);
    _gZkClient.writeData(path, badCurState);
    
    
    TestHelper.startController(clusterName,
                               "controller_0",
                               ZK_ADDR,
                               HelixControllerMain.STANDALONE);
    // start participants
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              clusterName));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);
    
    System.out.println("END testCarryOverBadCurState at "
        + new Date(System.currentTimeMillis()));

  }
}
