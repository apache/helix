package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestEndlessBestPossibleNodes extends ZkTestBase  {

  public static String CLUSTER_NAME = TestHelper.getTestClassName() + "_cluster";
  public static int PARTICIPANT_COUNT = 12;
  public static List<MockParticipantManager> _participants = new ArrayList<>();
  public static ClusterControllerManager _controller;
  public static ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() {
    System.out.println("Start test " + TestHelper.getTestClassName());
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < PARTICIPANT_COUNT; i++) {
      addParticipant("localhost_" + i);
    }

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig =  _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList("partcount"));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap("partcount", 10000));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap("partcount", 1));
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(57600000);
    clusterConfig.setPersistBestPossibleAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  // This test was constructed to capture the bug described in issue 2971
  // https://github.com/apache/helix/issues/2971
  @Test
  public void testEndlessBestPossibleNodes() throws Exception {
    int numPartition = 10;
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();

    // Create 1 WAGED Resource
    String firstDB = "InPlaceMigrationTestDB3";
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, firstDB, numPartition, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO.name(), null);
    IdealState idealStateOne =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, firstDB);
    idealStateOne.setMinActiveReplicas(2);
    idealStateOne.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, firstDB, idealStateOne);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, firstDB, 3);
    Assert.assertTrue(verifier.verifyByPolling());

    // Disable instances so delay rebalance overwrite is required due to min active
    for (int i = 0; i < PARTICIPANT_COUNT/2; i++) {
      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, _participants.get(i).getInstanceName(), false);
    }

    // Add new instance to cause partial rebalance to calculate a new best possible
    addParticipant("newInstance_0");

    // Sleep to let pipeline run and ZK writes occur
    Thread.sleep(5000);

    // There should only be 2 best possibles created (children will be 0, 1, LAST_WRITE, and LAST_SUCCESSFUL_WRITE)
    int childCount = _gZkClient.getChildren("/" + CLUSTER_NAME + "/ASSIGNMENT_METADATA/BEST_POSSIBLE").size();
    Assert.assertTrue(childCount > 0);
    Assert.assertTrue(childCount < 5, "Child count was " + childCount);
  }

  public MockParticipantManager addParticipant(String instanceName) {
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    participant.syncStart();
    _participants.add(participant);
    return participant;
  }
}
