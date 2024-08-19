package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestForceKillInstance extends ZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestForceKillInstance.class);
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private static final int NUM_REPLICAS = 3;
  private static final int NUM_PARTITIONS = 10;
  private static final int NUM_MIN_ACTIVE_REPLICAS = 2;
  private static final int NUM_NODES = NUM_REPLICAS;
  private BestPossibleExternalViewVerifier _bestPossibleClusterVerifier;
  private HelixAdmin _admin;
  HelixDataAccessor _dataAccessor;
  List<MockParticipantManager> _participants = new ArrayList<MockParticipantManager>();
  List<String> _resources;
  protected ClusterControllerManager _controller;



  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    IdealState crushedResource = createResourceWithWagedRebalance(CLUSTER_NAME, "Test_CRUSHED_Resource",
        "MasterSlave", NUM_PARTITIONS, NUM_REPLICAS, NUM_MIN_ACTIVE_REPLICAS);
    IdealState wagedResource = createResourceWithDelayedRebalance(CLUSTER_NAME, "Test_WAGED_Resource",
        "MasterSlave", NUM_PARTITIONS, NUM_REPLICAS, NUM_MIN_ACTIVE_REPLICAS, 200000, CrushEdRebalanceStrategy.class.getName());
    _resources = Arrays.asList(crushedResource.getId(), wagedResource.getId());
    _bestPossibleClusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setResources(new HashSet<>(_resources)).setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();

    for (int i = 0; i < NUM_NODES; i++) {
      addParticipant(CLUSTER_NAME, "localhost_" + i);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _admin = new ZKHelixAdmin(_gZkClient);
  }

  @BeforeMethod
  public void beforeMethod() {
    _bestPossibleClusterVerifier.verifyByPolling();
  }

  @Test
  public void testForceKillDropsAssignment() {
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // ensure no live instance znode and no assignments
    Assert.assertFalse(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should not exist after force kill");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Actually stop the instance, re assert no live instance znode and no assignments
    instanceToKill.syncStop();
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should not exist after force kill");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
  }

  @Test(dependsOnMethods = "testForceKillDropsAssignment")
  public void testSessionExpiration() throws Exception {
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Expire the session of the instance, assert live instance znode was recreated but instance does not have assignments
    ZkTestHelper.expireSession(instanceToKill.getZkClient());
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist after session recreation");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
  }

  @Test(dependsOnMethods = "testSessionExpiration")
  public void testDisconnectReconnect() {
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should not exist after force kill");

    // Restart the instance, assert live instance znode recreated but still no assignments
    // This should behave the same as if session was expired for same zkClient
    ZkTestHelper.simulateZkStateReconnected(instanceToKill.getZkClient());
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should not recreated if client reconnects within same session");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
  }

  @Test(dependsOnMethods = "testDisconnectReconnect")
  public void testRemoveUnknownOperationAfterForceKill() {
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    _admin.setInstanceOperation(CLUSTER_NAME, instanceToKillName, InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // ensure no live instance znode and no assignments
    Assert.assertFalse(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should not exist after force kill");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
  }

  @Test(dependsOnMethods = "testRemoveUnknownOperationAfterForceKill")
  public void testSessionExpirationWithoutUnknownOperation() throws Exception {
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    _admin.setInstanceOperation(CLUSTER_NAME, instanceToKillName, InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Expire the session of the instance, assert live instance znode was recreated and instance has assignments
    ZkTestHelper.expireSession(instanceToKill.getZkClient());
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist after session recreation");
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
  }

  @Test(dependsOnMethods = "testSessionExpirationWithoutUnknownOperation")
  public void testDisconnectReconnectWithoutUnknownOperation() {
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    _admin.setInstanceOperation(CLUSTER_NAME, instanceToKillName, InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should not exist after force kill");

    // Restart the instance, assert live instance znode recreated but still no assignments
    // This should behave the same as if session was expired for same zkClient
    ZkTestHelper.simulateZkStateReconnected(instanceToKill.getZkClient());
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists("/" + CLUSTER_NAME + "/LIVEINSTANCES/" + instanceToKillName),
        "Instance znode should not recreated if client reconnects within same session");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
  }

  private MockParticipantManager addParticipant(String cluster, String instanceName) {
    _gSetupTool.addInstanceToCluster(cluster, instanceName);
    MockParticipantManager toAddParticipant =
        new MockParticipantManager(ZK_ADDR, cluster, instanceName);
    _participants.add(toAddParticipant);
    toAddParticipant.syncStart();
    return toAddParticipant;
  }

    protected void dropParticipant(String cluster, String instanceName) {
      // find mock participant manager with instanceName and remove it from _mockParticipantManagers.
      MockParticipantManager toRemoveManager = _participants.stream()
          .filter(manager -> manager.getInstanceName().equals(instanceName))
          .findFirst()
          .orElse(null);
      if (toRemoveManager != null) {
        toRemoveManager.syncStop();
        _participants.remove(toRemoveManager);
      }

      InstanceConfig instanceConfig = _gSetupTool.getClusterManagementTool().getInstanceConfig(cluster, instanceName);
      _gSetupTool.getClusterManagementTool().dropInstance(cluster, instanceConfig);
    }

  private Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    for (String resource : _resources) {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, resource);
      externalViews.put(resource, ev);
    }
    return externalViews;
  }

  private Map<String, String> getInstanceCurrentStates(String instanceName) {
    Map<String, String> assignment = new HashMap<>();
    for (ExternalView ev : getEVs().values()) {
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap.containsKey(instanceName)) {
          assignment.put(partition, stateMap.get(instanceName));
        }
      }
    }
    return assignment;
  }
}
