package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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


public class TestWagedNPE extends ZkTestBase  {

  private final String CLUSTER_NAME = TestHelper.getTestClassName() + "_cluster";
  private final int PARTICIPANT_COUNT = 3;
  private final int PARTITION_COUNT = 3;
  private final int REPLICA_COUNT = 3;
  private final int DEFAULT_VERIFIER_TIMEOUT = 15000;
  private List<MockParticipantManager> _participants = new ArrayList<>();
  private ClusterControllerManager _controller;
  private ConfigAccessor _configAccessor;
  private BestPossibleExternalViewVerifier _verifier;

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
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(testCapacityKey, 3));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(testCapacityKey, 1));
    clusterConfig.setPersistBestPossibleAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    _verifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
  }

  // This test was constructed to capture the bug described in issue 2891
  // https://github.com/apache/helix/issues/2891
  @Test
  public void testNPEonNewResource() {
    System.out.println("Start test " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName());
    // Create 1 WAGED Resource
    String firstDB = "firstDB";
    addWagedResource(firstDB, PARTITION_COUNT, REPLICA_COUNT);

    // Wait for cluster to converge
    Assert.assertTrue(_verifier.verifyByPolling());

    // Drop resource
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, firstDB);

    // Wait for cluster to converge
    Assert.assertTrue(_verifier.verifyByPolling());

    // add instance
    MockParticipantManager instanceToAdd =  addParticipant("instance_to_add");

    // Wait for cluster to converge
    Assert.assertTrue(_verifier.verifyByPolling());

    // Add a new resource
    String secondDB = "secondDB";
    addWagedResource(secondDB, PARTITION_COUNT, REPLICA_COUNT);

    // Confirm cluster can converge. Cluster will not converge if NPE occurs during pipeline run
    Assert.assertTrue(_verifier.verifyByPolling());

    // Reset cluster
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, secondDB);
    instanceToAdd.syncStop();
    _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToAdd.getInstanceName()));
    Assert.assertTrue(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME).isEmpty());
    Assert.assertTrue(_verifier.verifyByPolling());
    System.out.println("End test " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testNPEonNewResource")
  public void testNPEonRebalanceFailure() throws Exception {
    System.out.println("Start test " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName());
    // Add 1 WAGED resource that will be succesfully placed
    String firstDB = "firstDB";
    addWagedResource(firstDB, PARTITION_COUNT, REPLICA_COUNT);

    // Wait for cluster to converge
    Assert.assertTrue(_verifier.verifyByPolling());

    // Add a 2nd WAGED resource that will fail to place
    String secondDB = "secondDB";
    addWagedResource(secondDB, PARTITION_COUNT, REPLICA_COUNT);

    // Kill 1 instance
    MockParticipantManager instanceToKill = _participants.get(0);
    instanceToKill.syncStop();
    Assert.assertTrue(TestHelper.verify(() ->
        !_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKill.getInstanceName()),
        DEFAULT_VERIFIER_TIMEOUT));

    // Assert that each partition for firstDB has a LEADER replica
    _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, firstDB)
        .getRecord().getMapFields().forEach((partition, partitionStateMap) -> {
      Assert.assertFalse(partitionStateMap.containsKey(instanceToKill.getInstanceName()));
      Assert.assertTrue(partitionStateMap.containsValue("LEADER"));
    });

    // Drop the dead instance
    _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToKill.getInstanceName()));

    // Kill another instance
    MockParticipantManager instanceToKill2 = _participants.get(1);
    instanceToKill2.syncStop();
    Assert.assertTrue(TestHelper.verify(() ->
            !_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKill2.getInstanceName()),
        DEFAULT_VERIFIER_TIMEOUT));

    // Assert that each partition for firstDB has a LEADER replica still
    Assert.assertTrue(TestHelper.verify( () -> {
      AtomicBoolean verified = new AtomicBoolean(true);
      _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, firstDB)
          .getRecord().getMapFields().forEach((partition, partitionStateMap) -> {
            boolean result = !partitionStateMap.containsKey(instanceToKill.getInstanceName()) &&
                !partitionStateMap.containsKey(instanceToKill2.getInstanceName()) &&
                partitionStateMap.containsValue("LEADER");
            if (!result) {
              verified.set(result);
            }
          });
      return verified.get();}, DEFAULT_VERIFIER_TIMEOUT));
    System.out.println("End test " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName());
  }

  public MockParticipantManager addParticipant(String instanceName) {
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    participant.syncStart();
    _participants.add(participant);
    return participant;
  }

  private void addWagedResource(String resourceName, int partitions, int replicas) {
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, resourceName, partitions, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO.name(), null);
    IdealState idealStateOne =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, resourceName);
    idealStateOne.setMinActiveReplicas(2);
    idealStateOne.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, resourceName, idealStateOne);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, resourceName, replicas);
  }
}
