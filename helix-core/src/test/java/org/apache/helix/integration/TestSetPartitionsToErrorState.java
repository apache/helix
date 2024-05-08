package org.apache.helix.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSetPartitionsToErrorState extends ZkTestBase {

  @Test()
  public void testSetPartitionsToErrorState() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start mock participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    // verify cluster
    HashMap<String, Map<String, String>> errStateMap = new HashMap<>();
    errStateMap.put("TestDB0", new HashMap<>());
    boolean result = ClusterStateVerifier.verifyByZkCallback(
        (new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName, errStateMap)));
    Assert.assertTrue(result, "Cluster verification fails");

    // set a non exist partition to ERROR, should throw exception
    try {
      String command = "--zkSvr " + ZK_ADDR + " --setPartitionsToError " + clusterName
          + " localhost_12918 TestDB0 TestDB0_nonExist";
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("Should throw exception on setting a non-exist partition to error");
    } catch (Exception e) {
      // OK
    }

    // set one partition not in ERROR state to ERROR
    String command = "--zkSvr " + ZK_ADDR + " --setPartitionsToError " + clusterName
        + " localhost_12918 TestDB0 TestDB0_4";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");
    result = ClusterStateVerifier.verifyByZkCallback(
        (new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName, errStateMap)));
    Assert.assertTrue(result, "Cluster verification fails");

    // set another partition not in ERROR state to ERROR
    command = "--zkSvr " + ZK_ADDR + " --setPartitionsToError " + clusterName
        + " localhost_12918 TestDB0 TestDB0_7";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    errStateMap.get("TestDB0").put("TestDB0_7", "localhost_12918");
    result = ClusterStateVerifier.verifyByZkCallback(
        (new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName, errStateMap)));
    Assert.assertTrue(result, "Cluster verification fails");

    // setting a partition already in ERROR state to ERROR - message does not get processed
    command = "--zkSvr " + ZK_ADDR + " --setPartitionsToError " + clusterName
        + " localhost_12918 TestDB0 TestDB0_7";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result = ClusterStateVerifier.verifyByZkCallback(
        (new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName, errStateMap)));
    Assert.assertTrue(result, "Cluster verification fails");

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }
    deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
