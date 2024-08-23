package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestWagedNPE extends ZkTestBase  {

  public static String CLUSTER_NAME = TestHelper.getTestClassName() + "_cluster";
  public static int PARTICIPANT_COUNT = 10;
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
    String testCapacityKey = "TestCapacityKey";
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(testCapacityKey));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(testCapacityKey, 100));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(testCapacityKey, 1));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  @Test
  public void testNPE() throws Exception {
    String firstDB = "firstDB";
    int numPartition = 10;
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, firstDB, numPartition, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO.name(), null);

    IdealState idealStateOne =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, firstDB);
    idealStateOne.setMinActiveReplicas(2);
    idealStateOne.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, firstDB, idealStateOne);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, firstDB, 3);

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(Collections.singleton(firstDB))
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();

    Assert.assertTrue(verifier.verifyByPolling());

    // drop resource
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, firstDB);

    // add instance
    MockParticipantManager participantToAdd = addParticipant("instance_to_add");
     verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(new HashSet<>(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)))
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // add resource again
    String secondDb = "secondDB";
    _configAccessor.setResourceConfig(CLUSTER_NAME, secondDb, new ResourceConfig(secondDb));
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, secondDb, numPartition, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO.name(), null);
    IdealState idealStateTwo =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, secondDb);
    idealStateTwo.setMinActiveReplicas(2);
    idealStateTwo.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, secondDb, idealStateTwo);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, secondDb, 3);


    verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(Collections.singleton(secondDb))
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
    Assert.assertTrue(verifier.verifyByPolling());
  }

  public MockParticipantManager addParticipant(String instanceName) {
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    participant.syncStart();
    _participants.add(participant);
    return participant;
  }

  public void dropParticipant(String instanceName) {
    MockParticipantManager participantToDrop = _participants.stream()
        .filter(p -> p.getInstanceName().equals(instanceName)).findFirst().get();
    dropParticipant(participantToDrop);

  }

  public void dropParticipant(MockParticipantManager participantToDrop) {
    participantToDrop.syncStop();
    _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME,
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, participantToDrop.getInstanceName()));
    _participants.remove(participantToDrop);
  }
}
