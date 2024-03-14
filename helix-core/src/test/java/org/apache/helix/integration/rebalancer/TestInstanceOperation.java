package org.apache.helix.integration.rebalancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixRollbackException;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestInstanceOperation extends ZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);
  public static final int TIMEOUT = 10000;
  private final int ZONE_COUNT = 4;
  protected final int START_NUM_NODE = 10;
  protected static final int START_PORT = 12918;
  private static int _nextStartPort = START_PORT;
  protected static final int PARTITIONS = 20;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private final String TEST_CAPACITY_KEY = "TestCapacityKey";
  private final int TEST_CAPACITY_VALUE = 100;
  protected static final String ZONE = "zone";
  protected static final String HOST = "host";
  protected static final String LOGICAL_ID = "logicalId";
  protected static final String TOPOLOGY = String.format("%s/%s/%s", ZONE, HOST, LOGICAL_ID);
  protected static final ImmutableSet<String> TOP_STATE_SET =
      ImmutableSet.of("MASTER");
  protected static final ImmutableSet<String> SECONDARY_STATE_SET =
      ImmutableSet.of("SLAVE", "STANDBY");
  protected static final ImmutableSet<String> ACCEPTABLE_STATE_SET =
      ImmutableSet.of("MASTER", "LEADER", "SLAVE", "STANDBY");
  private int REPLICA = 3;
  protected ClusterControllerManager _controller;
  private HelixManager _spectator;
  private RoutingTableProvider _routingTableProviderDefault;
  private RoutingTableProvider _routingTableProviderEV;
  private RoutingTableProvider _routingTableProviderCS;
  List<MockParticipantManager> _participants = new ArrayList<>();
  List<String> _participantNames = new ArrayList<>();
  private Set<String> _allDBs = new HashSet<>();
  private ZkHelixClusterVerifier _clusterVerifier;
  private ZkHelixClusterVerifier _bestPossibleClusterVerifier;
  private ConfigAccessor _configAccessor;
  private long _stateModelDelay = 3L;

  private final long DEFAULT_RESOURCE_DELAY_TIME = 1800000L;
  private HelixAdmin _admin;
  protected AssignmentMetadataStore _assignmentMetadataStore;
  HelixDataAccessor _dataAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < START_NUM_NODE; i++) {
      String participantName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
      addParticipant(participantName);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setDeactivatedNodeAwareness(true)
        .setResources(_allDBs)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
    _bestPossibleClusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setResources(_allDBs)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    _configAccessor = new ConfigAccessor(_gZkClient);
    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);

    // start spectator
    _spectator =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "spectator", InstanceType.SPECTATOR,
            ZK_ADDR);
    _spectator.connect();
    _routingTableProviderDefault = new RoutingTableProvider(_spectator);
    _routingTableProviderEV = new RoutingTableProvider(_spectator, PropertyType.EXTERNALVIEW);
    _routingTableProviderCS = new RoutingTableProvider(_spectator, PropertyType.CURRENTSTATES);

    setupClusterConfig();

    createTestDBs(DEFAULT_RESOURCE_DELAY_TIME);

    setUpWagedBaseline();

    _admin = new ZKHelixAdmin(_gZkClient);
  }

  @AfterClass
  public void afterClass() {
    // Drop all DBs
    for (String db : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    _controller.syncStop();
    _routingTableProviderDefault.shutdown();
    _routingTableProviderEV.shutdown();
    _routingTableProviderCS.shutdown();
    _spectator.disconnect();
  }

  private void setupClusterConfig() {
    _stateModelDelay = 3L;
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(1800000L);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private void enabledTopologyAwareRebalance() {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology(TOPOLOGY);
    clusterConfig.setFaultZoneType(ZONE);
    clusterConfig.setTopologyAwareEnabled(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private void disableTopologyAwareRebalance() {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(false);
    clusterConfig.setTopology(null);
    clusterConfig.setFaultZoneType(null);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private void removeOfflineOrDisabledOrSwapInInstances() {
    // Remove all instances that are not live, disabled, or in SWAP_IN state.
    for (int i = 0; i < _participants.size(); i++) {
      String participantName = _participantNames.get(i);
      InstanceConfig instanceConfig =
          _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, participantName);
      if (!_participants.get(i).isConnected() || !instanceConfig.getInstanceEnabled()
          || instanceConfig.getInstanceOperation()
          .equals(InstanceConstants.InstanceOperation.SWAP_IN.name())) {
        if (_participants.get(i).isConnected()) {
          _participants.get(i).syncStop();
        }
        _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, instanceConfig);
        _participantNames.remove(i);
        _participants.remove(i);
        i--;
      }
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @Test
  public void testEvacuate() throws Exception {
    System.out.println("START TestInstanceOperation.testEvacuate() at " + new Date(System.currentTimeMillis()));

    // Add semi-auto DBs
    String semiAutoDB = "SemiAutoTestDB_1";
    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, semiAutoDB,
        _participants.stream().map(ZKHelixManager::getInstanceName).collect(Collectors.toList()),
        BuiltInStateModelDefinitions.OnlineOffline.name(), 1, _participants.size());
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // EV should contain all participants, check resources one by one
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
    }

    // evacuated instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // New ev should contain all instances but the evacuated one
    assignment = getEVs();
    List<String> currentActiveInstances =
        _participantNames.stream().filter(n -> !n.equals(instanceToEvacuate)).collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
      Set<String> newPAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newPAssignedParticipants.contains(instanceToEvacuate));
      Assert.assertTrue(newPAssignedParticipants.containsAll(currentActiveInstances));
    }

    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));
    Assert.assertTrue(_admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, instanceToEvacuate));

    // Drop semi-auto DBs
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, semiAutoDB);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Disable, stop, and drop the instance from the cluster.
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceToEvacuate, false);
    _participants.get(0).syncStop();
    removeOfflineOrDisabledOrSwapInInstances();

    // Compare the current ev with the previous one, it should be exactly the same since the baseline should not change
    // after the instance is dropped.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertEquals(getEVs(), assignment);
}

  @Test(dependsOnMethods = "testEvacuate")
  public void testRevertEvacuation() throws Exception {
    System.out.println("START TestInstanceOperation.testRevertEvacuation() at " + new Date(System.currentTimeMillis()));
    // revert an evacuate instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // EV should contain all participants, check resources one by one
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test(dependsOnMethods = "testRevertEvacuation")
  public void testAddingNodeWithEvacuationTag() throws Exception {
    System.out.println("START TestInstanceOperation.testAddingNodeWithEvacuationTag() at " + new Date(System.currentTimeMillis()));
    // first disable and instance, and wait for all replicas to be moved out
    String mockNewInstance = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, mockNewInstance, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    //ev should contain all instances but the disabled one
    Map<String, ExternalView> assignment = getEVs();
    List<String> currentActiveInstances =
        _participantNames.stream().filter(n -> !n.equals(mockNewInstance)).collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource), REPLICA-1);
      Set<String> newPAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newPAssignedParticipants.contains(mockNewInstance));
      Assert.assertTrue(newPAssignedParticipants.containsAll(currentActiveInstances));
    }

    // add evacuate tag and enable instance
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, mockNewInstance, InstanceConstants.InstanceOperation.EVACUATE);
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, mockNewInstance, true);
    //ev should be the same
    assignment = getEVs();
    currentActiveInstances =
        _participantNames.stream().filter(n -> !n.equals(mockNewInstance)).collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource), REPLICA-1);
      Set<String> newPAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newPAssignedParticipants.contains(mockNewInstance));
      Assert.assertTrue(newPAssignedParticipants.containsAll(currentActiveInstances));
    }

    // now remove operation tag
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // EV should contain all participants, check resources one by one
    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testAddingNodeWithEvacuationTag")
  public void testNodeSwapNoTopologySetup() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapNoTopologySetup() at " + new Date(
        System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Add instance with InstanceOperation set to SWAP_IN
    // There should be an error that the logicalId does not have SWAP_OUT instance because,
    // helix can't determine what topology key to use to get the logicalId if TOPOLOGY is not set.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testNodeSwapNoTopologySetup")
  public void testAddingNodeWithSwapOutInstanceOperation() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testAddingNodeWithSwapOutInstanceOperation() at " + new Date(
            System.currentTimeMillis()));

    enabledTopologyAwareRebalance();
    removeOfflineOrDisabledOrSwapInInstances();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_OUT, true, -1);
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testAddingNodeWithSwapOutInstanceOperation")
  public void testAddingNodeWithSwapOutNodeInstanceOperationUnset() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testAddingNodeWithSwapOutNodeInstanceOperationUnset() at "
            + new Date(System.currentTimeMillis()));

    removeOfflineOrDisabledOrSwapInInstances();

    // Set instance's InstanceOperation to null
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName, null);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testAddingNodeWithSwapOutNodeInstanceOperationUnset")
  public void testNodeSwapWithNoSwapOutNode() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapWithNoSwapOutNode() at " + new Date(
        System.currentTimeMillis()));

    removeOfflineOrDisabledOrSwapInInstances();

    // Add new instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, "1000", "zone_1000",
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);
  }

  @Test(dependsOnMethods = "testNodeSwapWithNoSwapOutNode")
  public void testNodeSwapSwapInNodeNoInstanceOperationEnabled() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapSwapInNodeNoInstanceOperationEnabled() at "
            + new Date(System.currentTimeMillis()));

    removeOfflineOrDisabledOrSwapInInstances();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Add instance with same logicalId with InstanceOperation unset
    // This should work because adding instance with InstanceOperation unset will automatically
    // set the InstanceOperation to SWAP_IN.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, true, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testNodeSwapSwapInNodeNoInstanceOperationEnabled")
  public void testNodeSwapSwapInNodeWithAlreadySwappingPair() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapSwapInNodeWithAlreadySwappingPair() at "
            + new Date(System.currentTimeMillis()));

    removeOfflineOrDisabledOrSwapInInstances();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);

    // Add another instance with InstanceOperation set to SWAP_IN with same logicalId as previously
    // added SWAP_IN instance.
    String secondInstanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(secondInstanceToSwapInName,
        instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testNodeSwapSwapInNodeWithAlreadySwappingPair")
  public void testNodeSwapWrongFaultZone() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapWrongFaultZone() at " + new Date(
        System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Add instance with InstanceOperation set to SWAP_IN
    // There should be an error because SWAP_IN instance must be in the same FAULT_ZONE as the SWAP_OUT instance.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE) + "1",
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testNodeSwapWrongFaultZone")
  public void testNodeSwapWrongCapacity() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapWrongCapacity() at " + new Date(
        System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Add instance with InstanceOperation set to SWAP_IN
    // There should be an error because SWAP_IN instance must have same capacity as the SWAP_OUT node.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, TEST_CAPACITY_VALUE - 10);
  }

  @Test(dependsOnMethods = "testNodeSwapWrongCapacity")
  public void testNodeSwap() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwap() at " + new Date(System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // Store original EV
    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Validate that the assignment has not changed since setting the InstanceOperation to SWAP_OUT
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);

    // Validate that partitions on SWAP_OUT instance does not change after setting the InstanceOperation to SWAP_OUT
    // and adding the SWAP_IN instance to the cluster.
    // Check that the SWAP_IN instance has the same partitions as the SWAP_OUT instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Set.of(instanceToSwapInName), Collections.emptySet());

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Validate that the SWAP_OUT instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance is now in the routing tables.
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);


    // Assert that SWAP_OUT instance is disabled and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // Validate that the SWAP_IN instance has the same partitions the SWAP_OUT instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Set.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwap")
  public void testNodeSwapDisableAndReenable() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwap() at " + new Date(System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // Store original EV
    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Validate that the assignment has not changed since setting the InstanceOperation to SWAP_OUT
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);

    // Validate that partitions on SWAP_OUT instance does not change after setting the InstanceOperation to SWAP_OUT
    // and adding the SWAP_IN instance to the cluster.
    // Check that the SWAP_IN instance has the same partitions as the SWAP_OUT instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Set.of(instanceToSwapInName), Collections.emptySet());

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Disable the SWAP_IN instance
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, instanceToSwapInName, false);

    // Check that the SWAP_IN instance's replicas match the SWAP_OUT instance's replicas
    // but all of them are OFFLINE
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Map<String, Map<String, String>> resourcePartitionStateOnSwapOutInstance =
        getResourcePartitionStateOnInstance(getEVs(), instanceToSwapOutName);
    Map<String, Map<String, String>> resourcePartitionStateOnSwapInInstance =
        getResourcePartitionStateOnInstance(getEVs(), instanceToSwapInName);
    Assert.assertEquals(
        resourcePartitionStateOnSwapInInstance.values().stream().flatMap(p -> p.keySet().stream())
            .collect(Collectors.toSet()),
        resourcePartitionStateOnSwapOutInstance.values().stream().flatMap(p -> p.keySet().stream())
            .collect(Collectors.toSet()));
    Set<String> swapInInstancePartitionStates =
        resourcePartitionStateOnSwapInInstance.values().stream().flatMap(e -> e.values().stream())
            .collect(Collectors.toSet());
    Assert.assertEquals(swapInInstancePartitionStates.size(), 1);
    Assert.assertTrue(swapInInstancePartitionStates.contains("OFFLINE"));

    // Re-enable the SWAP_IN instance
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceToSwapInName, true);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Validate that the SWAP_OUT instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance is now in the routing tables.
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    // Assert that SWAP_OUT instance is disabled and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // Validate that the SWAP_IN instance has the same partitions the SWAP_OUT instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Set.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapDisableAndReenable")
  public void testNodeSwapSwapInNodeNoInstanceOperationDisabled() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapSwapInNodeNoInstanceOperationDisabled() at "
            + new Date(System.currentTimeMillis()));

    removeOfflineOrDisabledOrSwapInInstances();

    // Store original EVs
    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Validate that the assignment has not changed since setting the InstanceOperation to SWAP_OUT
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation unset, should automatically be set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, false, -1);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Enable the SWAP_IN instance, so it can start being assigned replicas
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceToSwapInName, true);

    // Validate that partitions on SWAP_OUT instance does not change after setting the InstanceOperation to SWAP_OUT
    // and adding the SWAP_IN instance to the cluster.
    // Check that the SWAP_IN instance has the same partitions as the SWAP_OUT instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Set.of(instanceToSwapInName), Collections.emptySet());

    // Validate that the SWAP_OUT instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));
    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Assert that SWAP_OUT instance is disabled and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // Validate that the SWAP_IN instance has the same partitions the SWAP_OUT instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Set.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapSwapInNodeNoInstanceOperationDisabled")
  public void testNodeSwapCancelSwapWhenReadyToComplete() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapCancelSwapWhenReadyToComplete() at " + new Date(
            System.currentTimeMillis()));

    removeOfflineOrDisabledOrSwapInInstances();

    // Store original EVs
    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Validate that the assignment has not changed since setting the InstanceOperation to SWAP_OUT
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);

    // Validate that partitions on SWAP_OUT instance does not change after setting the InstanceOperation to SWAP_OUT
    // and adding the SWAP_IN instance to the cluster.
    // Check that the SWAP_IN instance has the same partitions as the SWAP_OUT instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Set.of(instanceToSwapInName), Collections.emptySet());

    // Validate that the SWAP_OUT instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Cancel SWAP by disabling the SWAP_IN instance and remove SWAP_OUT InstanceOperation from SWAP_OUT instance.
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, instanceToSwapInName, false);
    // Stop the participant
    _participants.get(_participants.size() - 1).syncStop();

    // Wait for cluster to converge.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_OUT instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Validate there are no partitions on the SWAP_IN instance.
    Assert.assertEquals(getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapInName).size(), 0);

    // Validate that the SWAP_OUT instance has the same partitions as it had before.
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_OUT instance has the same partitions as it had before.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet())), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapCancelSwapWhenReadyToComplete")
  public void testNodeSwapAfterEMM() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapAfterEMM() at " + new Date(
        System.currentTimeMillis()));

    removeOfflineOrDisabledOrSwapInInstances();

    // Store original EVs
    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    // Put the cluster in maintenance mode.
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    // Validate that the assignment has not changed since setting the InstanceOperation to SWAP_OUT
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);

    // Validate that the assignment has not changed since adding the SWAP_IN node.
    // During MM, the cluster should not compute new assignment.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Remove the cluster from maintenance mode.
    // Now swapping will begin
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Validate that partitions on SWAP_OUT instance does not change after exiting MM
    // Check that the SWAP_IN instance has the same partitions as the SWAP_OUT instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Set.of(instanceToSwapInName), Collections.emptySet());

    // Validate that the SWAP_OUT instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));
    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance is now in the routing tables.
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    // Assert that SWAP_OUT instance is disabled and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // Validate that the SWAP_IN instance has the same partitions the SWAP_OUT instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Set.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapAfterEMM")
  public void testNodeSwapWithSwapOutInstanceDisabled() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapWithSwapOutInstanceDisabled() at " + new Date(
            System.currentTimeMillis()));

    removeOfflineOrDisabledOrSwapInInstances();

    // Store original EVs
    Map<String, ExternalView> originalEVs = getEVs();

    // Set instance's InstanceOperation to SWAP_OUT
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    Set<String> swapOutInstanceOriginalPartitions =
        getPartitionsAndStatesOnInstance(originalEVs, instanceToSwapOutName).keySet();

    // Disable the SWAP_OUT instance.
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, instanceToSwapOutName, false);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_OUT instance has all partitions in OFFLINE state
    Set<String> swapOutInstanceOfflineStates =
        new HashSet<>(getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapOutName).values());
    Assert.assertEquals(swapOutInstanceOfflineStates.size(), 1);
    Assert.assertTrue(swapOutInstanceOfflineStates.contains("OFFLINE"));

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, true, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance has the same partitions as the SWAP_OUT instance in second top state.
    Map<String, String> swapInInstancePartitionsAndStates =
        getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapInName);
    Assert.assertEquals(swapInInstancePartitionsAndStates.keySet().size(), 0);

    // Assert canSwapBeCompleted is false because SWAP_OUT instance is disabled.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Enable the SWAP_OUT instance.
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, instanceToSwapOutName, true);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance has the same partitions the SWAP_OUT instance originally
    // had. Validate they are in second top state.
    swapInInstancePartitionsAndStates =
        getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapInName);
    Assert.assertTrue(
        swapInInstancePartitionsAndStates.keySet().containsAll(swapOutInstanceOriginalPartitions));
    Set<String> swapInInstanceStates = new HashSet<>(swapInInstancePartitionsAndStates.values());
    swapInInstanceStates.removeAll(SECONDARY_STATE_SET);
    Assert.assertEquals(swapInInstanceStates.size(), 0);

    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Assert that SWAP_OUT instance is disabled and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    verifier(
        () -> (getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapOutName).isEmpty()),
        TIMEOUT);
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testNodeSwapWithSwapOutInstanceDisabled")
  public void testNodeSwapAddSwapInFirstEnabledBeforeSwapOutSet() {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapAddSwapInFirstEnabledBeforeSwapOutSet() at "
            + new Date(System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // Get the SWAP_OUT instance.
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to SWAP_IN enabled before setting SWAP_OUT instance.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, true, -1);
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testNodeSwapAddSwapInFirstEnabledBeforeSwapOutSet")
  public void testNodeSwapAddSwapInFirstEnableBeforeSwapOutSet() {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapAddSwapInFirstEnableBeforeSwapOutSet() at "
            + new Date(System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // Get the SWAP_OUT instance.
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, false, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Enable the SWAP_IN instance before we have set the SWAP_OUT instance.
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceToSwapInName, true);
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testNodeSwapAddSwapInFirstEnableBeforeSwapOutSet")
  public void testUnsetInstanceOperationOnSwapInWhenAlreadyUnsetOnSwapOut() {
    System.out.println(
        "START TestInstanceOperation.testUnsetInstanceOperationOnSwapInWhenAlreadyUnsetOnSwapOut() at "
            + new Date(System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // Get the SWAP_OUT instance.
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, false, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Try to remove the InstanceOperation from the SWAP_IN instance before the SWAP_OUT instance is set.
    // This should throw exception because we cannot ever have two instances with the same logicalId and both have InstanceOperation
    // unset.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToSwapInName, null);
  }

  @Test(dependsOnMethods = "testUnsetInstanceOperationOnSwapInWhenAlreadyUnsetOnSwapOut")
  public void testNodeSwapAddSwapInFirst() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapAddSwapInFirst() at " + new Date(
        System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // Store original EV
    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    // Get the SWAP_OUT instance.
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, false, -1);

    // Validate that the assignment has not changed since setting the InstanceOperation to SWAP_OUT
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // After the SWAP_IN instance is added, we set the InstanceOperation to SWAP_OUT
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.SWAP_OUT);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Enable the SWAP_IN instance to begin the swap operation.
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceToSwapInName, true);

    // Validate that partitions on SWAP_OUT instance does not change after setting the InstanceOperation to SWAP_OUT
    // and adding the SWAP_IN instance to the cluster.
    // Check that the SWAP_IN instance has the same partitions as the SWAP_OUT instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Set.of(instanceToSwapInName), Collections.emptySet());

    // Validate that the SWAP_OUT instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));
    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance is now in the routing tables.
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    // Assert that SWAP_OUT instance is disabled and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // Validate that the SWAP_IN instance has the same partitions the SWAP_OUT instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Set.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapAddSwapInFirst")
  public void testEvacuateAndCancelBeforeBootstrapFinish() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testEvacuateAndCancelBeforeBootstrapFinish() at " + new Date(
            System.currentTimeMillis()));
    removeOfflineOrDisabledOrSwapInInstances();

    // add a resource where downward state transition is slow
    createResourceWithDelayedRebalance(CLUSTER_NAME, "TEST_DB3_DELAYED_CRUSHED", "MasterSlave",
        PARTITIONS, REPLICA, REPLICA - 1, 200000, CrushEdRebalanceStrategy.class.getName());
    _allDBs.add("TEST_DB3_DELAYED_CRUSHED");
    // add a resource where downward state transition is slow
    createResourceWithWagedRebalance(CLUSTER_NAME, "TEST_DB4_DELAYED_WAGED", "MasterSlave",
        PARTITIONS, REPLICA, REPLICA - 1);
    _allDBs.add("TEST_DB4_DELAYED_WAGED");
    // wait for assignment to finish
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // set bootstrap ST delay to a large number
    _stateModelDelay = -10000L;
    // evacuate an instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.EVACUATE);
    // Messages should be pending at all instances besides the evacuate one
    for (String participant : _participantNames) {
      if (participant.equals(instanceToEvacuate)) {
        continue;
      }
      verifier(() -> ((_dataAccessor.getChildNames(
          _dataAccessor.keyBuilder().messages(participant))).isEmpty()), 30000);
    }
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));
    Assert.assertFalse(_admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, instanceToEvacuate));

    // sleep a bit so ST messages can start executing
    Thread.sleep(Math.abs(_stateModelDelay / 100));
    // before we cancel, check current EV
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      // check every replica has >= 3 partitions and a top state partition
      validateAssignmentInEv(assignment.get(resource));
    }

    // cancel the evacuation
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, null);

    assignment = getEVs();
    for (String resource : _allDBs) {
      // check every replica has >= 3 active replicas, even before cluster converge
      validateAssignmentInEv(assignment.get(resource));
    }

    // check cluster converge. We have longer delay for ST then verifier timeout. It will only converge if we cancel ST.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // EV should contain all participants, check resources one by one
    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      // check every replica has >= 3 active replicas again
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test(dependsOnMethods = "testEvacuateAndCancelBeforeBootstrapFinish")
  public void testEvacuateAndCancelBeforeDropFinish() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testEvacuateAndCancelBeforeDropFinish() at " + new Date(
            System.currentTimeMillis()));

    // set DROP ST delay to a large number
    _stateModelDelay = 10000L;

    // evacuate an instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.EVACUATE);

    // message should be pending at the to evacuate participant
    verifier(() -> ((_dataAccessor.getChildNames(
        _dataAccessor.keyBuilder().messages(instanceToEvacuate))).isEmpty()), 30000);
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));

    // cancel evacuation
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, null);
    // check every replica has >= 3 active replicas, even before cluster converge
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // EV should contain all participants, check resources one by one
    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      // check every replica has >= 3 active replicas
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test(dependsOnMethods = "testEvacuateAndCancelBeforeDropFinish")
  public void testMarkEvacuationAfterEMM() throws Exception {
    System.out.println("START TestInstanceOperation.testMarkEvacuationAfterEMM() at " + new Date(
        System.currentTimeMillis()));
    _stateModelDelay = 1000L;
    Assert.assertFalse(_gSetupTool.getClusterManagementTool().isInMaintenanceMode(CLUSTER_NAME));
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);
    String newParticipantName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(newParticipantName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertFalse(
          getParticipantsInEv(assignment.get(resource)).contains(newParticipantName));
    }

    // set evacuate operation
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.EVACUATE);

    // there should be no evacuation happening
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).contains(instanceToEvacuate));
    }

    // exit MM
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    assignment = getEVs();
    List<String> currentActiveInstances =
        _participantNames.stream().filter(n -> !n.equals(instanceToEvacuate))
            .collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
      Set<String> newPAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newPAssignedParticipants.contains(instanceToEvacuate));
      Assert.assertTrue(newPAssignedParticipants.containsAll(currentActiveInstances));
    }
    Assert.assertTrue(_admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, instanceToEvacuate));

    _stateModelDelay = 3L;
  }

  @Test(dependsOnMethods = "testMarkEvacuationAfterEMM")
  public void testEvacuationWithOfflineInstancesInCluster() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testEvacuationWithOfflineInstancesInCluster() at " + new Date(
            System.currentTimeMillis()));
    _participants.get(1).syncStop();
    _participants.get(2).syncStop();

    String evacuateInstanceName = _participants.get(_participants.size() - 2).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, evacuateInstanceName,
        InstanceConstants.InstanceOperation.EVACUATE);

    Map<String, ExternalView> assignment;
    // EV should contain all participants, check resources one by one
    assignment = getEVs();
    for (String resource : _allDBs) {
      verifier(() -> {
        ExternalView ev = assignment.get(resource);
        for (String partition : ev.getPartitionSet()) {
          AtomicInteger activeReplicaCount = new AtomicInteger();
          ev.getStateMap(partition).values().stream().filter(
                  v -> v.equals("MASTER") || v.equals("LEADER") || v.equals("SLAVE") || v.equals(
                      "FOLLOWER") || v.equals("STANDBY"))
              .forEach(v -> activeReplicaCount.getAndIncrement());
          if (activeReplicaCount.get() < REPLICA - 1 || (
              ev.getStateMap(partition).containsKey(evacuateInstanceName) && ev.getStateMap(
                  partition).get(evacuateInstanceName).equals("MASTER") && ev.getStateMap(partition)
                  .get(evacuateInstanceName).equals("LEADER"))) {
            return false;
          }
        }
        return true;
      }, 30000);
    }

    removeOfflineOrDisabledOrSwapInInstances();
    addParticipant(PARTICIPANT_PREFIX + "_" + _nextStartPort);
    addParticipant(PARTICIPANT_PREFIX + "_" + _nextStartPort);
    dropTestDBs(ImmutableSet.of("TEST_DB3_DELAYED_CRUSHED", "TEST_DB4_DELAYED_WAGED"));
  }

  /**
   * Verifies that the given verifier returns true within the given timeout. Handles AssertionError
   * by returning false, which TestHelper.verify will not do. Asserts that return value from
   * TestHelper.verify is true.
   *
   * @param verifier the verifier to run
   * @param timeout  the timeout to wait for the verifier to return true
   * @throws Exception if TestHelper.verify throws an exception
   */
  private static void verifier(TestHelper.Verifier verifier, long timeout) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      try {
        boolean result = verifier.verify();
        if (!result) {
          LOG.error("Verifier returned false, retrying...");
        }
        return result;
      } catch (AssertionError e) {
        LOG.error("Caught AssertionError on verifier attempt: ", e);
        return false;
      }
    }, timeout));
  }

  private MockParticipantManager createParticipant(String participantName) {
    // start dummy participants
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, participantName);
    StateMachineEngine stateMachine = participant.getStateMachineEngine();
    // Using a delayed state model
    StDelayMSStateModelFactory delayFactory = new StDelayMSStateModelFactory();
    stateMachine.registerStateModelFactory("MasterSlave", delayFactory);
    return participant;
  }

  private void addParticipant(String participantName, String logicalId, String zone,
      InstanceConstants.InstanceOperation instanceOperation, boolean enabled, int capacity) {
    InstanceConfig config = new InstanceConfig.Builder().setDomain(
            String.format("%s=%s, %s=%s, %s=%s", ZONE, zone, HOST, participantName, LOGICAL_ID,
                logicalId)).setInstanceEnabled(enabled).setInstanceOperation(instanceOperation)
        .build(participantName);

    if (capacity >= 0) {
      config.setInstanceCapacityMap(Map.of(TEST_CAPACITY_KEY, capacity));
    }
    _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, config);

    MockParticipantManager participant = createParticipant(participantName);

    participant.syncStart();
    _participants.add(participant);
    _participantNames.add(participantName);
    _nextStartPort++;
  }

  private void addParticipant(String participantName) {
    addParticipant(participantName, UUID.randomUUID().toString(),
        "zone_" + _participants.size() % ZONE_COUNT, null, true, -1);
  }

   private void createTestDBs(long delayTime) throws InterruptedException {
     createResourceWithDelayedRebalance(CLUSTER_NAME, "TEST_DB0_CRUSHED",
         BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA - 1, -1,
         CrushEdRebalanceStrategy.class.getName());
     _allDBs.add("TEST_DB0_CRUSHED");
    createResourceWithDelayedRebalance(CLUSTER_NAME, "TEST_DB1_CRUSHED",
        BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA - 1, 2000000,
        CrushEdRebalanceStrategy.class.getName());
    _allDBs.add("TEST_DB1_CRUSHED");
    createResourceWithWagedRebalance(CLUSTER_NAME, "TEST_DB2_WAGED", BuiltInStateModelDefinitions.LeaderStandby.name(),
        PARTITIONS, REPLICA, REPLICA - 1);
    _allDBs.add("TEST_DB2_WAGED");

     Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private void dropTestDBs(Set<String> dbs) throws Exception {
    for (String db : dbs) {
      _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, db);
      _allDBs.remove(db);
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    for (String db : _allDBs) {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  private boolean verifyIS(String evacuateInstanceName) {
    for (String db : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      for (String partition : is.getPartitionSet()) {
        List<String> newPAssignedParticipants = is.getPreferenceList(partition);
        if (newPAssignedParticipants.contains(evacuateInstanceName)) {
          System.out.println("partition " + partition + " assignment " + newPAssignedParticipants + " ev " + evacuateInstanceName);
          return false;
        }
      }
    }
    return true;
  }

  private Set<String> getParticipantsInEv(ExternalView ev) {
    Set<String> assignedParticipants = new HashSet<>();
    for (String partition : ev.getPartitionSet()) {
      ev.getStateMap(partition)
          .keySet()
          .stream()
          .filter(k -> !ev.getStateMap(partition).get(k).equals("OFFLINE"))
          .forEach(assignedParticipants::add);
    }
    return assignedParticipants;
  }

  private Map<String, String> getPartitionsAndStatesOnInstance(Map<String, ExternalView> evs,
      String instanceName) {
    Map<String, String> instancePartitions = new HashMap<>();
    for (String resourceEV : evs.keySet()) {
      for (String partition : evs.get(resourceEV).getPartitionSet()) {
        if (evs.get(resourceEV).getStateMap(partition).containsKey(instanceName)) {
          instancePartitions.put(partition,
              evs.get(resourceEV).getStateMap(partition).get(instanceName));
        }
      }
    }

    return instancePartitions;
  }

  private Map<String, Map<String, String>> getResourcePartitionStateOnInstance(
      Map<String, ExternalView> evs, String instanceName) {
    Map<String, Map<String, String>> stateByPartitionByResource = new HashMap<>();
    for (String resourceEV : evs.keySet()) {
      for (String partition : evs.get(resourceEV).getPartitionSet()) {
        if (evs.get(resourceEV).getStateMap(partition).containsKey(instanceName)) {
          if (!stateByPartitionByResource.containsKey(resourceEV)) {
            stateByPartitionByResource.put(resourceEV, new HashMap<>());
          }
          stateByPartitionByResource.get(resourceEV)
              .put(partition, evs.get(resourceEV).getStateMap(partition).get(instanceName));
        }
      }
    }

    return stateByPartitionByResource;
  }

  private Set<String> getInstanceNames(Collection<InstanceConfig> instanceConfigs) {
    return instanceConfigs.stream().map(InstanceConfig::getInstanceName)
        .collect(Collectors.toSet());
  }

  private void validateRoutingTablesInstance(Map<String, ExternalView> evs, String instanceName,
      boolean shouldContain) {
    RoutingTableProvider[] routingTableProviders =
        new RoutingTableProvider[]{_routingTableProviderDefault, _routingTableProviderEV, _routingTableProviderCS};
    getResourcePartitionStateOnInstance(evs, instanceName).forEach((resource, partitions) -> {
      partitions.forEach((partition, state) -> {
        Arrays.stream(routingTableProviders).forEach(rtp -> Assert.assertEquals(
            getInstanceNames(rtp.getInstancesForResource(resource, partition, state)).contains(
                instanceName), shouldContain));
      });
    });

    Arrays.stream(routingTableProviders).forEach(rtp -> {
      Assert.assertEquals(getInstanceNames(rtp.getInstanceConfigs()).contains(instanceName),
          shouldContain);
    });
  }

  private void validateEVCorrect(ExternalView actual, ExternalView original,
      Map<String, String> swapOutInstancesToSwapInInstances, Set<String> inFlightSwapInInstances,
      Set<String> completedSwapInInstanceNames) {
    Assert.assertEquals(actual.getPartitionSet(), original.getPartitionSet());
    IdealState is = _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, original.getResourceName());
    StateModelDefinition stateModelDef = _gSetupTool.getClusterManagementTool()
        .getStateModelDef(CLUSTER_NAME, is.getStateModelDefRef());
    for (String partition : actual.getPartitionSet()) {
      Map<String, String> expectedStateMap = new HashMap<>(original.getStateMap(partition));
      for (String swapOutInstance : swapOutInstancesToSwapInInstances.keySet()) {
        if (expectedStateMap.containsKey(swapOutInstance) && inFlightSwapInInstances.contains(
            swapOutInstancesToSwapInInstances.get(swapOutInstance))) {
          // If the corresponding swapInInstance is in-flight, add it to the expectedStateMap
          // with the same state as the swapOutInstance or secondState if the swapOutInstance
          // has a topState.
          expectedStateMap.put(swapOutInstancesToSwapInInstances.get(swapOutInstance),
              expectedStateMap.get(swapOutInstance).equals(stateModelDef.getTopState())
                  ? (String) stateModelDef.getSecondTopStates().toArray()[0]
                  : expectedStateMap.get(swapOutInstance));
        } else if (expectedStateMap.containsKey(swapOutInstance)
            && completedSwapInInstanceNames.contains(
            swapOutInstancesToSwapInInstances.get(swapOutInstance))) {
          // If the corresponding swapInInstance is completed, add it to the expectedStateMap
          // with the same state as the swapOutInstance.
          expectedStateMap.put(swapOutInstancesToSwapInInstances.get(swapOutInstance),
              expectedStateMap.get(swapOutInstance));
          expectedStateMap.remove(swapOutInstance);
        }
      }
      Assert.assertEquals(actual.getStateMap(partition), expectedStateMap, "Error for partition " + partition
          + " in resource " + actual.getResourceName());
    }
  }

  private boolean validateEVsCorrect(Map<String, ExternalView> actuals,
      Map<String, ExternalView> originals, Map<String, String> swapOutInstancesToSwapInInstances,
      Set<String> inFlightSwapInInstances, Set<String> completedSwapInInstanceNames) {
    Assert.assertEquals(actuals.keySet(), originals.keySet());
    for (String resource : actuals.keySet()) {
      validateEVCorrect(actuals.get(resource), originals.get(resource),
          swapOutInstancesToSwapInInstances, inFlightSwapInInstances, completedSwapInInstanceNames);
    }
    return true;
  }

  private void validateAssignmentInEv(ExternalView ev) {
    validateAssignmentInEv(ev, REPLICA);
  }

  private void validateAssignmentInEv(ExternalView ev, int expectedNumber) {
    Set<String> partitionSet = ev.getPartitionSet();
    for (String partition : partitionSet) {
      AtomicInteger activeReplicaCount = new AtomicInteger();
      ev.getStateMap(partition).values().stream().filter(ACCEPTABLE_STATE_SET::contains)
          .forEach(v -> activeReplicaCount.getAndIncrement());
      Assert.assertTrue(activeReplicaCount.get() >=expectedNumber);
    }
  }

  private void setUpWagedBaseline() {
    _assignmentMetadataStore = new AssignmentMetadataStore(new ZkBucketDataAccessor(ZK_ADDR), CLUSTER_NAME) {
      public Map<String, ResourceAssignment> getBaseline() {
        // Ensure this metadata store always read from the ZK without using cache.
        super.reset();
        return super.getBaseline();
      }

      public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
        // Ensure this metadata store always read from the ZK without using cache.
        super.reset();
        return super.getBestPossibleAssignment();
      }
    };

    // Set test instance capacity and partition weights
    ClusterConfig clusterConfig = _dataAccessor.getProperty(_dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(TEST_CAPACITY_KEY));
    clusterConfig.setDefaultInstanceCapacityMap(
        Collections.singletonMap(TEST_CAPACITY_KEY, TEST_CAPACITY_VALUE));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(TEST_CAPACITY_KEY, 1));
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().clusterConfig(), clusterConfig);
  }

  // A state transition model where either downward ST are slow (_stateModelDelay >0) or upward ST are slow (_stateModelDelay <0)
  public class StDelayMSStateModelFactory extends StateModelFactory<StDelayMSStateModel> {

    @Override
    public StDelayMSStateModel createNewStateModel(String resourceName, String partitionKey) {
      StDelayMSStateModel model = new StDelayMSStateModel();
      return model;
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {"MASTER", "SLAVE", "ERROR"})
  public class StDelayMSStateModel extends StateModel {

    public StDelayMSStateModel() {
      _cancelled = false;
    }

    private void sleepWhileNotCanceled(long sleepTime) throws InterruptedException{
      while(sleepTime >0 && !isCancelled()) {
        Thread.sleep(TIMEOUT);
        sleepTime = sleepTime - TIMEOUT;
      }
      if (isCancelled()) {
        _cancelled = false;
        throw new HelixRollbackException("EX");
      }
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) throws InterruptedException {
      if (_stateModelDelay < 0) {
        sleepWhileNotCanceled(Math.abs(_stateModelDelay));
      }
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) throws InterruptedException {
      if (_stateModelDelay < 0) {
        sleepWhileNotCanceled(Math.abs(_stateModelDelay));
      }
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) throws InterruptedException {
      if (_stateModelDelay > 0) {
        sleepWhileNotCanceled(_stateModelDelay);
      }
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) throws InterruptedException {
      if (_stateModelDelay > 0) {
        sleepWhileNotCanceled(_stateModelDelay);
      }
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) throws InterruptedException {
      if (_stateModelDelay > 0) {
        sleepWhileNotCanceled(_stateModelDelay);
      }
    }
  }
}
