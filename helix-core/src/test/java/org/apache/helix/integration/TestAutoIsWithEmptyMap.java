package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.mock.storage.MockParticipant;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.IdealStateCalculatorForStorageNode;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAutoIsWithEmptyMap extends ZkIntegrationTestBase
{
  @Test
  public void testAutoIsWithEmptyMap() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "LeaderStandby", false); // do not rebalance

    // calculate and set custom ideal state
    String idealPath = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName, "TestDB0");
    ZNRecord curIdealState = _gZkClient.readData(idealPath);

    List<String> instanceNames = new ArrayList<String>(5);
    for (int i = 0; i < 5; i++)
    {
      int port = 12918 + i;
      instanceNames.add("localhost_" + port);
    }
    ZNRecord idealState = IdealStateCalculatorForStorageNode.calculateIdealState(instanceNames, 10,
        2, "TestDB0", "LEADER", "STANDBY");
    // System.out.println(idealState);
    // curIdealState.setSimpleField(IdealState.IdealStateProperty.IDEAL_STATE_MODE.toString(),
    // "CUSTOMIZED");
    curIdealState.setSimpleField(IdealState.IdealStateProperty.REPLICAS.toString(), "3");

    curIdealState.setListFields(idealState.getListFields());
    _gZkClient.writeData(idealPath, curIdealState);

    // start controller
    TestHelper
        .startController(clusterName, "controller_0", ZK_ADDR, HelixControllerMain.STANDALONE);

    // start participants
    MockParticipant[] participants = new MockParticipant[5];
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result = ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(
        ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // clean up
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
