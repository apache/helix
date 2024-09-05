package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
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
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
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
  private static final int NUM_NODES = 3;
  private BestPossibleExternalViewVerifier _bestPossibleClusterVerifier;
  private ZkHelixClusterVerifier _clusterVerifier;
  private HelixAdmin _admin;
  private HelixDataAccessor _dataAccessor;
  private List<MockParticipantManager> _participants = new ArrayList<>();
  private List<String> _resources;
  private ClusterControllerManager _controller;
  private Set<String> _downwardSTBlockedInstances = new HashSet<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    enablePersistIntermediateAssignment(_gZkClient, CLUSTER_NAME, true);

    IdealState wagedResource = createResourceWithWagedRebalance(CLUSTER_NAME, "Test_WAGED_Resource",
        "MasterSlave", NUM_PARTITIONS, NUM_REPLICAS, NUM_MIN_ACTIVE_REPLICAS);
    IdealState crushedResource = createResourceWithDelayedRebalance(CLUSTER_NAME, "Test_CRUSHED_Resource",
        "MasterSlave", NUM_PARTITIONS, NUM_REPLICAS, NUM_MIN_ACTIVE_REPLICAS, 0, CrushEdRebalanceStrategy.class.getName());
    _resources = Arrays.asList(crushedResource.getId(), wagedResource.getId());
    _bestPossibleClusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setResources(new HashSet<>(_resources)).setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setDeactivatedNodeAwareness(true).setResources(new HashSet<>(_resources))
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();

    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = "localhost_" + i;
      addParticipant(CLUSTER_NAME, instanceName);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _admin = new ZKHelixAdmin(_gZkClient);
    _dataAccessor = _controller.getHelixDataAccessor();
  }

  @BeforeMethod
  public void beforeMethod() {
    _bestPossibleClusterVerifier.verifyByPolling();
    _downwardSTBlockedInstances.clear();
  }

  @Test
  public void testForceKillDropsAssignment() {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());


    // ensure no live instance znode and no assignments
    Assert.assertFalse(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should not exist after force kill");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Actually stop the instance, re assert no live instance znode and no assignments
    instanceToKill.syncStop();
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should not exist after force kill");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testForceKillDropsAssignment")
  public void testSessionExpiration() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Expire the session of the instance, assert live instance znode was recreated but instance does not have assignments
    ZkTestHelper.expireSession(instanceToKill.getZkClient());
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist after session recreation");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testSessionExpiration")
  public void testDisconnectReconnect() {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should not exist after force kill");

    // Restart the instance, assert live instance znode recreated but still no assignments
    // This should behave the same as if session was expired for same zkClient
    ZkTestHelper.simulateZkStateReconnected(instanceToKill.getZkClient());
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should not recreated if client reconnects within same session");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testDisconnectReconnect")
  public void testRemoveUnknownOperationAfterForceKill() {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    _admin.setInstanceOperation(CLUSTER_NAME, instanceToKillName, InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // ensure no live instance znode and no assignments
    Assert.assertFalse(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should not exist after force kill");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testRemoveUnknownOperationAfterForceKill")
  public void testSessionExpirationWithoutUnknownOperation() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    _admin.setInstanceOperation(CLUSTER_NAME, instanceToKillName, InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Expire the session of the instance, assert live instance znode was recreated and instance has assignments
    ZkTestHelper.expireSession(instanceToKill.getZkClient());
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist after session recreation");
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testSessionExpirationWithoutUnknownOperation")
  public void testDisconnectReconnectWithoutUnknownOperation() {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Get assignments on node, assert it has at least one assignemnt
    Assert.assertFalse(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should have at least one assignment");
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    _admin.setInstanceOperation(CLUSTER_NAME, instanceToKillName, InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should not exist after force kill");

    // Restart the instance, assert live instance znode recreated but still no assignments
    // This should behave the same as if session was expired for same zkClient
    ZkTestHelper.simulateZkStateReconnected(instanceToKill.getZkClient());
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should not recreated if client reconnects within same session");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testDisconnectReconnectWithoutUnknownOperation")
  public void testLiveInstanceZNodeImmediatelyRecreated() {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    LiveInstance liveInstance = _dataAccessor.getProperty(_dataAccessor.keyBuilder().liveInstance(instanceToKillName));
    LiveInstanceZnodeListener listener = new LiveInstanceZnodeListener(_dataAccessor, liveInstance);
    _gZkClient.subscribeDataChanges(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath(), listener, true);

    // Force kill the instance
    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist because of listener");
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Manually remove the live instance znode that was created by the listener
    _gZkClient.unsubscribeDataChanges(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath(), listener);
    _dataAccessor.removeProperty(_dataAccessor.keyBuilder().liveInstance(instanceToKillName));

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  // Test to confirm that the custom state model factory is correctly blocking downward state transitions
  @Test(dependsOnMethods = "testLiveInstanceZNodeImmediatelyRecreated")
  public void testDownwardStateTransitionsBlocked() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();

    // Block downward ST and get assignments on node, assert it has at least one assignment
    _downwardSTBlockedInstances.add(instanceToKillName);
    Map<String, String> assignments = getInstanceCurrentStates(instanceToKillName);
    Assert.assertFalse(assignments.isEmpty(), "Instance should have at least one assignment");

    // Manually set instance operation to unknown, this should block topstate handoff
    _admin.setInstanceOperation(CLUSTER_NAME, instanceToKillName, InstanceConstants.InstanceOperation.UNKNOWN);
    Thread.sleep(1000); // Wait for downward ST to be sent
    Assert.assertEquals(getInstanceCurrentStates(instanceToKillName).size(), assignments.size(),
        "Instance should not have lost any assignments");
    Assert.assertFalse(_dataAccessor.getChildNames(_dataAccessor.keyBuilder().messages(instanceToKillName)).isEmpty(),
        "Instance should have pending ST messages");

    // Remove block on downward ST so instance can drop assignments
    _downwardSTBlockedInstances.remove(instanceToKillName);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  // Recreate scenario where node is not capable of processing downward state transitions, so setting unknown operation
  // will not have any effect on external view. Only removing of live instance (force kill) will drop assignments
  @Test(dependsOnMethods = "testDownwardStateTransitionsBlocked")
  public void testForceKillWithBlockedDownwardStateTransition() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    MockParticipantManager instanceToKill = _participants.get(0);
    String instanceToKillName = instanceToKill.getInstanceName();
    Assert.assertTrue(_gZkClient.exists(_dataAccessor.keyBuilder().liveInstance(instanceToKillName).getPath()),
        "Instance znode should exist before force kill");

    // Block downward ST and get assignments on node, assert it has at least one assignemnt
    _downwardSTBlockedInstances.add(instanceToKillName);
    Map<String, String> assignments = getInstanceCurrentStates(instanceToKillName);
    Assert.assertFalse(assignments.isEmpty(), "Instance should have at least one assignment");

    // Manually set instance operation to unknown, this should block topstate handoff
    _admin.setInstanceOperation(CLUSTER_NAME, instanceToKillName, InstanceConstants.InstanceOperation.UNKNOWN);
    Thread.sleep(1000); // Wait for downward ST to be sent
    Assert.assertEquals(getInstanceCurrentStates(instanceToKillName).size(), assignments.size(),
        "Instance should not have lost any assignments");
    Assert.assertFalse(_dataAccessor.getChildNames(_dataAccessor.keyBuilder().messages(instanceToKillName)).isEmpty(),
        "Instance should have pending ST messages");

    _admin.forceKillInstance(CLUSTER_NAME, instanceToKillName);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(getInstanceCurrentStates(instanceToKillName).isEmpty(),
        "Instance should not have any assignments");

    // Reset state of cluster
    dropParticipant(CLUSTER_NAME, instanceToKillName);
    addParticipant(CLUSTER_NAME, instanceToKillName);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

    private MockParticipantManager addParticipant(String cluster, String instanceName) {
      _gSetupTool.addInstanceToCluster(cluster, instanceName);
      MockParticipantManager toAddParticipant =
          new MockParticipantManager(ZK_ADDR, cluster, instanceName);
      StateMachineEngine stateMachine = toAddParticipant.getStateMachineEngine();
      stateMachine.registerStateModelFactory("MasterSlave",
          new TestBlockDownwardStateTransitionModelFactory(instanceName));
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

  private class LiveInstanceZnodeListener implements IZkDataListener {
    private final HelixDataAccessor _helixDataAccessor;
    private final LiveInstance _liveInstance;

    public LiveInstanceZnodeListener(HelixDataAccessor helixDataAccessor, LiveInstance liveInstance) {
      this._helixDataAccessor = helixDataAccessor;
      this._liveInstance = liveInstance;
    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      //no-op
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      System.out.println("LIVEINSTANCE znode deleted. Recreating...");
      _helixDataAccessor.setProperty(_helixDataAccessor.keyBuilder().liveInstance(_liveInstance.getInstanceName()), _liveInstance);
    }
  }

  // Statemodel factory that loops endlessly on downward state transitions if instance is in blocked set. This is used
  // to mimic scenario where node is being sent downward ST's but is not processing them, so topstate handoff cannot occur
  public class TestBlockDownwardStateTransitionModelFactory extends StateModelFactory<TestBlockDownwardStateTransitionStateModel> {

    private final String _instanceName;
    public TestBlockDownwardStateTransitionModelFactory(String instanceName) {
      _instanceName = instanceName;
    }

    @Override
    public TestBlockDownwardStateTransitionStateModel createNewStateModel(String resourceName, String partitionKey) {
      return new TestBlockDownwardStateTransitionStateModel(_instanceName);
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {"MASTER", "SLAVE", "ERROR"})
  public class TestBlockDownwardStateTransitionStateModel extends StateModel {
    private final String _instanceName;

    public TestBlockDownwardStateTransitionStateModel(String instanceName) {
      _instanceName = instanceName;
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      LOG.info("Become MASTER from SLAVE");
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      LOG.info("Become SLAVE from OFFLINE");
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      loopWhileBlocked();
      LOG.info("Become SLAVE from MASTER");
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      loopWhileBlocked();
      LOG.info("Become OFFLINE from SLAVE");
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOG.info("Become DROPPED from OFFLINE");
    }

    private void loopWhileBlocked() {
      while (_downwardSTBlockedInstances.contains(_instanceName)) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // no op, continue loop
        }
      }
    }
  }
}
