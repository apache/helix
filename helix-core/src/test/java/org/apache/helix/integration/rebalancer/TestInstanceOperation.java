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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixRollbackException;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
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
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestInstanceOperation extends ZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestInstanceOperation.class);
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
  private BestPossibleExternalViewVerifier _bestPossibleClusterVerifier;
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

  @BeforeMethod
  public void beforeMethod() throws Exception {
    removeOfflineOrInactiveInstances();
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
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

  private void removeOfflineOrInactiveInstances() {
    // Remove all instances that are not live, disabled, or in SWAP_IN state.
    for (int i = 0; i < _participants.size(); i++) {
      String participantName = _participantNames.get(i);
      InstanceConfig instanceConfig =
          _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, participantName);
      if (!_participants.get(i).isConnected() || !instanceConfig.getInstanceEnabled()
          || instanceConfig.getInstanceOperation().getOperation()
          .equals(InstanceConstants.InstanceOperation.SWAP_IN)) {
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

    // Compare the current ev with the previous one, it should be exactly the same since the baseline should not change
    // after the instance is dropped.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertEquals(getEVs(), assignment);
}

  @Test
  public void testEvacuateWithCustomizedResource() throws Exception {
    System.out.println("START TestInstanceOperation.testEvacuateWithCustomizedResource() at " + new Date(System.currentTimeMillis()));
    for( String resource : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, resource);
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    String customizedDB = "CustomizedTestDB";
    Map<Integer, String> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(Integer.valueOf(0), _participants.get(0).getInstanceName());
    createResourceInCustomizedMode(_gSetupTool, CLUSTER_NAME, customizedDB, partitionInstanceMap);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);
    // evacuated instance
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));
    // Drop customized DBs in clusterx
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, customizedDB);
    createTestDBs(DEFAULT_RESOURCE_DELAY_TIME);
  }

  @Test
  public void testEvacuateWithCustomizedResourceOfflineInstance() throws Exception {
    /*
      Test the following scenario
       * Customized resource partitions P0, P1 assigned to an instance A.
       * Instance A becomes unhealthy.
       * Cluster enters maintenance mode.
       * Evacuate operation is set of instance A
       * Cluster exists maintenance mode
       * isEvacuateFinished operation on instanceA returns False
       * new ideal state is computed, which doesn't have instance A
       * isEvacuateFinished operation on instanceA returns true
     */
    System.out.println("START TestInstanceOperation.testEvacuateWithCustomizedResourceOfflineInstance() at " + new Date(System.currentTimeMillis()));
    for( String resource : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, resource);
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    String customizedDB = "CustomizedTestDB";
    Map<Integer, String> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put(Integer.valueOf(0), _participants.get(0).getInstanceName());
    partitionInstanceMap.put(Integer.valueOf(1), _participants.get(0).getInstanceName());
    createResourceInCustomizedMode(_gSetupTool, CLUSTER_NAME, customizedDB, partitionInstanceMap);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    _participants.get(0).syncStop();
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);
    // evacuated instance
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));
    partitionInstanceMap.put(Integer.valueOf(0), _participants.get(1).getInstanceName());
    partitionInstanceMap.put(Integer.valueOf(1), _participants.get(1).getInstanceName());
    IdealState newIdealState = createCustomizedResourceIdealState(customizedDB, partitionInstanceMap);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, customizedDB, newIdealState);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));
    // Drop customized DBs in clusterx
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, customizedDB);
    createTestDBs(DEFAULT_RESOURCE_DELAY_TIME);
  }

  @Test(dependsOnMethods = "testEvacuate")
  public void testRevertEvacuation() throws Exception {
    System.out.println("START TestInstanceOperation.testRevertEvacuation() at " + new Date(System.currentTimeMillis()));
    // revert an evacuate instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToEvacuate,
        InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToEvacuate)
            .getInstanceEnabled());
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
    // This is using a deprecated method to ensure that the disabling still takes precedence over the InstanceOperation when being set
    // to false.
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, mockNewInstance, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    // ev should contain all instances but the disabled one
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
    // Because HELIX_ENABLED is set to false, getInstanceOperation still returns DISABLE
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, mockNewInstance, InstanceConstants.InstanceOperation.EVACUATE);

    // enable instance so InstanceOperation is no longer overriden with DISABLE
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, mockNewInstance, true);

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
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // EV should contain all participants, check resources one by one
    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test(dependsOnMethods = "testAddingNodeWithEvacuationTag")
  public void testNodeSwapNoTopologySetup() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapNoTopologySetup() at " + new Date(
        System.currentTimeMillis()));
    removeOfflineOrInactiveInstances();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();

    // Add instance with InstanceOperation set to SWAP_IN as default
    // The instance will be added with UNKNOWN because the logicalId will not match the
    // swap out instance since the topology configs are not set.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test(dependsOnMethods = "testNodeSwapNoTopologySetup")
  public void testAddingNodeWithEnableInstanceOperation() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testAddingNodeWithEnableInstanceOperation() at " + new Date(
            System.currentTimeMillis()));

    enabledTopologyAwareRebalance();
    removeOfflineOrInactiveInstances();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to ENABLE
    // The instance should be added with UNKNOWN since there is already an instance with
    // the same logicalId in the cluster and this instance is not being set to SWAP_IN when
    // added.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.ENABLE, -1);

    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test(dependsOnMethods = "testAddingNodeWithEnableInstanceOperation")
  public void testNodeSwapWithNoSwapOutNode() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapWithNoSwapOutNode() at " + new Date(
        System.currentTimeMillis()));

    removeOfflineOrInactiveInstances();

    // Add new instance with InstanceOperation set to SWAP_IN
    // The instance should be added with UNKNOWN since there is not an instance with a matching
    // logicalId in the cluster to swap with.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, "1000", "zone_1000",
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test(dependsOnMethods = "testNodeSwapWithNoSwapOutNode")
  public void testNodeSwapSwapInNodeNoInstanceOperationEnabled() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapSwapInNodeNoInstanceOperationEnabled() at "
            + new Date(System.currentTimeMillis()));

    removeOfflineOrInactiveInstances();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with same logicalId with InstanceOperation unset, this is the same as default
    // which is ENABLE.
    // The instance should be set to UNKNOWN since there is already a matching logicalId in the cluster.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, -1);

    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    // Setting the InstanceOperation to SWAP_IN should work because there is a matching logicalId in
    // the cluster and the InstanceCapacityWeights and FaultZone match.
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapInName,
        InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertEquals(_gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(), InstanceConstants.InstanceOperation.SWAP_IN);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testNodeSwapSwapInNodeNoInstanceOperationEnabled")
  public void testNodeSwapSwapInNodeWithAlreadySwappingPair() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapSwapInNodeWithAlreadySwappingPair() at "
            + new Date(System.currentTimeMillis()));

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Add another instance with InstanceOperation set to SWAP_IN with same logicalId as previously
    // added SWAP_IN instance.
    String secondInstanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(secondInstanceToSwapInName,
        instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Instance should be UNKNOWN since there was already a swapping pair.
    Assert.assertEquals(_gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, secondInstanceToSwapInName).getInstanceOperation()
            .getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    // Try to set the InstanceOperation to SWAP_IN, it should throw an exception since there is already
    // a swapping pair.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, secondInstanceToSwapInName,
            InstanceConstants.InstanceOperation.SWAP_IN);
  }

  @Test(dependsOnMethods = "testNodeSwapSwapInNodeWithAlreadySwappingPair")
  public void testNodeSwap() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwap() at " + new Date(System.currentTimeMillis()));

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String resourceToDisablePartition = _allDBs.iterator().next();
    // Disable 1 partition that is assigned to the instance that will be swapped out.
    getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapOutName).entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(resourceToDisablePartition)).findFirst()
        .ifPresent(entry -> {
          String partition = entry.getKey();
          instanceToSwapOutInstanceConfig.setInstanceEnabledForPartition(resourceToDisablePartition,
              partition, false);
        });
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, instanceToSwapOutName, instanceToSwapOutInstanceConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Store original EV
    Map<String, ExternalView> originalEVs = getEVs();

    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Create a custom change listener to check if the throttles are enabled after the swap is completed.
    CustomIndividualInstanceConfigChangeListener instanceToSwapInInstanceConfigListener =
        new CustomIndividualInstanceConfigChangeListener();

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1, instanceToSwapInInstanceConfigListener, null);

    // Validate that the throttles are off since the InstanceOperation is set to SWAP_IN
    Assert.assertFalse(instanceToSwapInInstanceConfigListener.isThrottlesEnabled());

    // Check that the SWAP_IN instance has the same partitions as the swap out instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Validate that the swap out instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    // Get both instanceConfigs and make sure correct fields are copied over.
    InstanceConfig instanceToSwapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName);

    Assert.assertEquals(instanceToSwapInInstanceConfig.getRecord()
            .getMapField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name()),
        instanceToSwapOutInstanceConfig.getRecord()
            .getMapField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name()));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance is now in the routing tables.
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    // Assert that swap out instance is not active and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());
    Assert.assertEquals(_gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceOperation()
            .getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    // Check to make sure the throttle was enabled again after the swap was completed.
    Assert.assertTrue(instanceToSwapInInstanceConfigListener.isThrottlesEnabled());

    // Validate that the SWAP_IN instance has the same partitions the swap out instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(instanceToSwapInName))), TIMEOUT);

    InstanceConfig instanceToSwapInInstanceConfigAfterSwap = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName);
    instanceToSwapInInstanceConfigAfterSwap.setInstanceEnabledForPartition(resourceToDisablePartition, true);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, instanceToSwapInName,
        instanceToSwapInInstanceConfigAfterSwap);
  }

  @Test(dependsOnMethods = "testNodeSwap")
  public void testNodeSwapDisableAndReenable() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwap() at " + new Date(System.currentTimeMillis()));

    // Store original EV
    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Validate that the assignment has not changed since setting the InstanceOperation to swap out
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Check that the SWAP_IN instance has the same partitions as the swap out instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Try to disable the swap out instance, it should not do anything.
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, instanceToSwapOutName, false);

    // Check that the SWAP_IN instance's replicas match the SWAP_OUT instance's replicas
    // and all of them are OFFLINE.
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
    Set<String> swapOutInstancePartitionStates =
        resourcePartitionStateOnSwapOutInstance.values().stream().flatMap(e -> e.values().stream())
            .collect(Collectors.toSet());
    Assert.assertEquals(swapOutInstancePartitionStates.size(), 1);
    Assert.assertTrue(swapOutInstancePartitionStates.contains("OFFLINE"));
    Set<String> swapInInstancePartitionStates =
        resourcePartitionStateOnSwapInInstance.values().stream().flatMap(e -> e.values().stream())
            .collect(Collectors.toSet());
    Assert.assertEquals(swapInInstancePartitionStates.size(), 1);
    Assert.assertTrue(swapInInstancePartitionStates.contains("OFFLINE"));

    // Validate that the SWAP_OUT instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Re-enable the swap out instance
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceToSwapOutName, true);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Validate that the SWAP_OUT instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance is now in the routing tables.
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    // Assert that swap out instance is not active and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());
    Assert.assertEquals(_gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceOperation()
            .getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    // Validate that the SWAP_IN instance has the same partitions the swap out instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapDisableAndReenable")
  public void testNodeSwapSwapInNodeNoInstanceOperation() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapSwapInNodeNoInstanceOperation() at "
            + new Date(System.currentTimeMillis()));

    // Store original EVs
    Map<String, ExternalView> originalEVs = getEVs();

    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation unset, should set to UNKNOWN.
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE), null, -1);

    // Validate that the SWAP_IN instance does not have any partitions on it.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Set InstanceOperation to SWAP_IN
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapInName,
        InstanceConstants.InstanceOperation.SWAP_IN);

    // Check that the SWAP_IN instance has the same partitions as the swap out instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    // Validate that the swap out instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));
    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Assert that swap out instance is inactive and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // Validate that the SWAP_IN instance has the same partitions the swap out instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapSwapInNodeNoInstanceOperation")
  public void testNodeSwapCancelSwapWhenReadyToComplete() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapCancelSwapWhenReadyToComplete() at " + new Date(
            System.currentTimeMillis()));

    // Store original EVs
    Map<String, ExternalView> originalEVs = getEVs();
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Validate that the assignment has not changed since setting the InstanceOperation to swap out
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Check that the SWAP_IN instance has the same partitions as the swap out instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    // Validate that the swap out instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Cancel the swap by setting the InstanceOperation to UNKNOWN on the SWAP_IN instance.
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapInName,
        InstanceConstants.InstanceOperation.UNKNOWN);

    // Validate there are no partitions on the SWAP_IN instance.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Wait for cluster to converge.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the swap out instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Validate there are no partitions on the SWAP_IN instance.
    Assert.assertEquals(getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapInName).size(), 0);

    // Validate that the swap out instance has the same partitions as it had before.
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName, InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the swap out instance has the same partitions as it had before.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet())), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapCancelSwapWhenReadyToComplete")
  public void testNodeSwapAfterEMM() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapAfterEMM() at " + new Date(
        System.currentTimeMillis()));

    // Store original EVs
    Map<String, ExternalView> originalEVs = getEVs();
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    // Put the cluster in maintenance mode.
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Validate that the assignment has not changed.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Validate that the assignment has not changed since adding the SWAP_IN node.
    // During MM, the cluster should not compute new assignment on SWAP_IN node.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Remove the cluster from maintenance mode.
    // Now swapping will begin
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Validate that partitions on swap out instance does not change after exiting MM
    // Check that the SWAP_IN instance has the same partitions as the swap out instance
    // but none of them are in a top state.
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(instanceToSwapInName), Collections.emptySet());

    // Validate that the swap out instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapOutName, true);
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));
    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance is now in the routing tables.
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    // Assert that swap out instance is disabled and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // Validate that the SWAP_IN instance has the same partitions the swap out instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapAfterEMM")
  public void testNodeSwapWithSwapOutInstanceDisabled() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapWithSwapOutInstanceDisabled() at " + new Date(
            System.currentTimeMillis()));

    // Store original EVs
    Map<String, ExternalView> originalEVs = getEVs();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Disable the swap out instance.
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, instanceToSwapOutName, false);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the swap out instance has all partitions in OFFLINE state
    Set<String> swapOutInstanceOfflineStates =
        new HashSet<>(getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapOutName).values());
    Assert.assertEquals(swapOutInstanceOfflineStates.size(), 1);
    Assert.assertTrue(swapOutInstanceOfflineStates.contains("OFFLINE"));

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance has no partitions because the swap started when the swap out node was offline
    Map<String, String> swapInInstancePartitionsAndStates =
        getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapInName);
    Assert.assertEquals(swapInInstancePartitionsAndStates.size(), 0);

    // Assert canSwapBeCompleted is false because swap out instance is disabled.
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Assert that swap out instance is disabled and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    verifier(
        () -> (getPartitionsAndStatesOnInstance(getEVs(), instanceToSwapOutName).isEmpty()),
        TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapWithSwapOutInstanceDisabled")
  public void testNodeSwapWithSwapOutInstanceOffline() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testNodeSwapWithSwapOutInstanceOffline() at " + new Date(
            System.currentTimeMillis()));

    // Store original EV
    Map<String, ExternalView> originalEVs = getEVs();
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);
    // Assert SWAP_IN taking affect
    Assert.assertEquals(_gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(), InstanceConstants.InstanceOperation.SWAP_IN);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Kill the participant
    _participants.get(0).syncStop();

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Assert canSwapBeCompleted is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Validate that the swap out instance is in routing tables and SWAP_IN is not.
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, false);

    // Assert completeSwapIfPossible is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance is now in the routing tables.
    validateRoutingTablesInstance(getEVs(), instanceToSwapInName, true);

    // Assert that swap out instance is inactive and has no partitions assigned to it.
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName).getInstanceEnabled());

    // Validate that the SWAP_IN instance has the same partitions the swap out instance had before
    // swap was completed.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(dependsOnMethods = "testNodeSwapWithSwapOutInstanceOffline")
  public void testSwapEvacuateAdd() throws Exception {
    System.out.println("START TestInstanceOperation.testSwapEvacuateAdd() at " + new Date(
        System.currentTimeMillis()));
    removeOfflineOrInactiveInstances();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Store original EV
    Map<String, ExternalView> originalEVs = getEVs();
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();

    // Enter maintenance mode
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Set instance's InstanceOperation to EVACUATE
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.EVACUATE);

    // Validate that the assignment has not changed since setting the InstanceOperation to EVACUATE
    validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), Collections.emptySet());

    // Add instance with InstanceOperation set to ENABLE
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.ENABLE, -1);

    // Exit maintenance mode
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that the SWAP_IN instance has the same partitions the swap out instance had.
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(instanceToSwapInName))), TIMEOUT);

    // Assert isEvacuateFinished is true
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .isEvacuateFinished(CLUSTER_NAME, instanceToSwapOutName));

    // Set the EVACUATE instance to UNKNOWN
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.UNKNOWN);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Validate that dropping the instance has not changed the assignment
    verifier(() -> (validateEVsCorrect(getEVs(), originalEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(instanceToSwapInName))), TIMEOUT);
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testSwapEvacuateAdd")
  public void testUnsetInstanceOperationOnSwapInWhenSwapping() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testUnsetInstanceOperationOnSwapInWhenSwapping() at "
            + new Date(System.currentTimeMillis()));

    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Try to remove the InstanceOperation from the SWAP_IN instance before swap in instance is set to unknown.
    // This should throw exception because we cannot ever have two instances with the same logicalId and both have InstanceOperation
    // unset.
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToSwapInName, InstanceConstants.InstanceOperation.ENABLE);
  }

  @Test(dependsOnMethods = "testUnsetInstanceOperationOnSwapInWhenSwapping")
  public void testNodeSwapAddSwapInFirst() throws Exception {
    System.out.println("START TestInstanceOperation.testNodeSwapAddSwapInFirst() at " + new Date(
        System.currentTimeMillis()));

    // Store original EV
    Map<String, ExternalView> originalEVs = getEVs();
    // Get the swap out instance.
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);

    // Add instance with InstanceOperation set to SWAP_IN
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);
  }

  @Test(dependsOnMethods = "testNodeSwapAddSwapInFirst")
  public void testDisabledPartitionsBeforeSwapInitiated() throws Exception {
    enabledTopologyAwareRebalance();
    System.out.println(
        "START TestInstanceOperation.testEvacuateWithDisabledPartition() at " + new Date(System.currentTimeMillis()));
    String toAddParticipant = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    removeOfflineOrInactiveInstances();
    addParticipant(toAddParticipant);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    // Add instance with InstanceOperation set to UNKNOWN
    String instanceToSwapOutName = _participants.get(_participants.size()-1).getInstanceName();
    InstanceConfig instanceToSwapOutconfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();
    swapOutInstancesToSwapInInstances.put(instanceToSwapOutName, instanceToSwapInName);

    addParticipant(instanceToSwapInName, instanceToSwapOutconfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutconfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.UNKNOWN, -1);

    _clusterVerifier.verifyByPolling();
    Map<String, ExternalView> beforeEVs = getEVs();

    // Set all partitions to disabled and set instance operation to SWAP_IN
    InstanceConfig swapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName);
    swapInInstanceConfig.setInstanceEnabledForPartition(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY,
        "", false);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, instanceToSwapInName, swapInInstanceConfig);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapInName,
        InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Assert assignment is in IdealState, but all states are offline
    for (String resource : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, resource);
      ExternalView ev = beforeEVs.get(resource);
      for (String partition : is.getPartitionSet()) {
        if (ev.getStateMap(partition).containsKey(instanceToSwapOutName)) {
          Assert.assertEquals(is.getInstanceStateMap(partition).get(instanceToSwapInName), "OFFLINE");
        }
      }
    }

    // Assert not possible to complete swap (swap in should not have current states)
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName));

    // Re-enable all partitions, IS states no longer forced to OFFLINE and swap allowed to complete
    swapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapInName);
    swapInInstanceConfig.setInstanceEnabledForPartition(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY,
        "", true);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, instanceToSwapInName, swapInInstanceConfig);

    // Assert successfully complete swap
    verifier(() -> _gSetupTool.getClusterManagementTool().canCompleteSwap(CLUSTER_NAME, instanceToSwapOutName), 30000);
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, instanceToSwapOutName, false));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), beforeEVs, swapOutInstancesToSwapInInstances, Collections.emptySet(),
        ImmutableSet.of(instanceToSwapInName));
    _participants.get(_participants.size()-1).syncStop();
  }

  @Test(dependsOnMethods = "testDisabledPartitionsBeforeSwapInitiated")
  public void testDisabledPartitionsAfterSwapInitiated() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testDisabledPartitionsAfterSwapInitiated() at " + new Date(System.currentTimeMillis()));

    String toAddParticipant = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    removeOfflineOrInactiveInstances();
    addParticipant(toAddParticipant);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Map<String, ExternalView> beforeSwapAndDisableEVs = getEVs();

    String swapOutInstanceName = _participants.get(_participants.size()-1).getInstanceName();
    InstanceConfig swapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutInstanceName);

    // Add SWAP_IN instance
    String swapInInstanceName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(swapInInstanceName, swapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        swapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);
    Map<String, String> swapOutInstancesToSwapInInstances = new HashMap<>();
    swapOutInstancesToSwapInInstances.put(swapOutInstanceName, swapInInstanceName);

    // Assert SWAP_IN taking affect
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, swapInInstanceName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool().canCompleteSwap(CLUSTER_NAME, swapOutInstanceName));

    // Get current EVs and validate swap proceeding normally
    Map<String, ExternalView> beforeDisableEVs = getEVs();
    validateEVsCorrect(beforeDisableEVs, beforeSwapAndDisableEVs, swapOutInstancesToSwapInInstances,
        ImmutableSet.of(swapInInstanceName), Collections.emptySet());

    // Set all partitions to disabled on SWAP_IN instance
    InstanceConfig swapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapInInstanceName);
    swapInInstanceConfig.setInstanceEnabledForPartition(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY,
        "", false);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, swapInInstanceName, swapInInstanceConfig);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    // Assert not possible to complete swap (swap in should not have intended current states)
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutInstanceName));

    // Assert EV is correct - all swap affected partitions will have 1 extra replica in OFFLINE state hosted on SWAP_IN node
    Map<String, ExternalView> currentEVs = getEVs();
    for (String resource : _allDBs) {
      validateEVCorrect(currentEVs.get(resource), beforeDisableEVs.get(resource), swapOutInstancesToSwapInInstances,
          ImmutableSet.of(swapInInstanceName), Collections.emptySet(), ImmutableSet.of(swapInInstanceName));
    }

    // Assert not possible to complete swap
    Assert.assertFalse(_gSetupTool.getClusterManagementTool().canCompleteSwap(CLUSTER_NAME, swapOutInstanceName));

    // Re-enable all partitions, IS states no longer forced to OFFLINE and swap allowed to complete
    swapInInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapInInstanceName);
    swapInInstanceConfig.setInstanceEnabledForPartition(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY,
        "", true);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, swapInInstanceName, swapInInstanceConfig);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool().canCompleteSwap(CLUSTER_NAME, swapOutInstanceName));
    Assert.assertTrue(_gSetupTool.getClusterManagementTool().completeSwapIfPossible(CLUSTER_NAME, swapOutInstanceName,
        false));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Assert swap completed successfully
    validateEVsCorrect(getEVs(), beforeSwapAndDisableEVs, swapOutInstancesToSwapInInstances,
        Collections.emptySet(), ImmutableSet.of(swapInInstanceName));
    _participants.get(_participants.size()-1).syncStop();
  }

  @Test(dependsOnMethods = "testDisabledPartitionsAfterSwapInitiated")
  public void testEvacuateAndCancelBeforeBootstrapFinish() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testEvacuateAndCancelBeforeBootstrapFinish() at " + new Date(
            System.currentTimeMillis()));

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
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);

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
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);
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

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testMarkEvacuationAfterEMM")
  public void testSwapEvacuateAddRemoveEvacuate() throws Exception {
    System.out.println("START TestInstanceOperation.testSwapEvacuateAddRemoveEvacuate() at " + new Date(
        System.currentTimeMillis()));

    // Set instance's InstanceOperation to EVACUATE
    String instanceToSwapOutName = _participants.get(0).getInstanceName();
    InstanceConfig instanceToSwapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, instanceToSwapOutName);
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName,
        InstanceConstants.InstanceOperation.EVACUATE);

    // Add instance with InstanceOperation set to ENABLE
    String instanceToSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToSwapInName, instanceToSwapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        instanceToSwapOutInstanceConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.ENABLE, -1);

    // Remove EVACUATE instance's InstanceOperation
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToSwapOutName, InstanceConstants.InstanceOperation.ENABLE);
  }

  @Test(dependsOnMethods = "testSwapEvacuateAddRemoveEvacuate")
  public void testUnknownDoesNotTriggerRebalance() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testUnknownDoesNotTriggerRebalance() at " + new Date(
            System.currentTimeMillis()));

    Map<String, IdealState> idealStatesBefore = getISs();

    // Add instance with InstanceOperation set to UNKNOWN (should not be considered in placement calculations)
    String instanceToAdd = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToAdd, "foo", "bar", InstanceConstants.InstanceOperation.UNKNOWN, -1);
    List<MockParticipantManager> testParticipants = new ArrayList<>();
    testParticipants.add(_participants.get(_participants.size() - 1));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Assert no rebalance
    Assert.assertEquals(idealStatesBefore, getISs());

    // Assert same no rebalance behavior for node with same logical ID as existing node
    String instanceToAdd2 = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    InstanceConfig swapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, _participants.get(0).getInstanceName());
    addParticipant(instanceToAdd2,  swapOutInstanceConfig.getLogicalId(LOGICAL_ID),
        swapOutInstanceConfig.getDomainAsMap().get(ZONE), InstanceConstants.InstanceOperation.UNKNOWN, -1);
    testParticipants.add(_participants.get(_participants.size() - 1));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertEquals(idealStatesBefore, getISs());


    // Clean up
    for (MockParticipantManager participant : testParticipants) {
      participant.syncStop();
    }
  }

  @Test(dependsOnMethods = "testUnknownDoesNotTriggerRebalance")
  public void testEvacuationWithOfflineInstancesInCluster() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testEvacuationWithOfflineInstancesInCluster() at " + new Date(
            System.currentTimeMillis()));
    _participants.get(1).syncStop();
    _participants.get(2).syncStop();

    String evacuateInstanceName = _participants.get(_participants.size() - 2).getInstanceName();
    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME, evacuateInstanceName,
        InstanceConstants.InstanceOperation.EVACUATE);

    // EV should contain all participants, check resources one by one
    verifier(() -> {
      Map<String, ExternalView> assignment = getEVs();
      for (String resource : _allDBs) {
        ExternalView ev = assignment.get(resource);
        for (String partition : ev.getPartitionSet()) {
          AtomicInteger activeReplicaCount = new AtomicInteger();
          ev.getStateMap(partition).values().stream().filter(
                  v -> v.equals("MASTER") || v.equals("LEADER") || v.equals("SLAVE") || v.equals(
                      "FOLLOWER") || v.equals("STANDBY"))
              .forEach(v -> activeReplicaCount.getAndIncrement());
          // If min active replicas violated OR if instance is evacuating and is top state for partition
          if (activeReplicaCount.get() < REPLICA - 1 || (ev.getStateMap(partition).containsKey(evacuateInstanceName) &&
              (ev.getStateMap(partition).get(evacuateInstanceName).equals("MASTER") ||
                  ev.getStateMap(partition).get(evacuateInstanceName).equals("LEADER")))) {
            return false;
          }
        }
      }
      return true;
    }, 30000);

    removeOfflineOrInactiveInstances();
    addParticipant(PARTICIPANT_PREFIX + "_" + _nextStartPort);
    addParticipant(PARTICIPANT_PREFIX + "_" + _nextStartPort);
    dropTestDBs(ImmutableSet.of("TEST_DB3_DELAYED_CRUSHED", "TEST_DB4_DELAYED_WAGED"));
  }

  @Test(dependsOnMethods = "testEvacuationWithOfflineInstancesInCluster")
  public void testEvacuateWithDisabledPartition() throws Exception {
    System.out.println(
        "START TestInstanceOperation.testEvacuateWithDisabledPartition() at " + new Date(
            System.currentTimeMillis()));
    StateTransitionCountStateModelFactory stateTransitionCountStateModelFactory = new StateTransitionCountStateModelFactory();
    String testCrushedDBName = "testEvacuateWithDisabledPartition_CRUSHED_DB0";
    String testWagedDBName = "testEvacuateWithDisabledPartition_WAGED_DB1";
    String toDisableThenEvacuateInstanceName = "disable_then_evacuate_host";
    addParticipant(toDisableThenEvacuateInstanceName, stateTransitionCountStateModelFactory);
    MockParticipantManager toDisableThenEvacuateParticipant = _participants.get(_participants.size() - 1);

    List<String> testResources = Arrays.asList(testCrushedDBName, testWagedDBName);
    createResourceWithDelayedRebalance(CLUSTER_NAME, testCrushedDBName, "MasterSlave",
        PARTITIONS, REPLICA, REPLICA-1, 200000, CrushEdRebalanceStrategy.class.getName());
    createResourceWithWagedRebalance(CLUSTER_NAME, testWagedDBName, "MasterSlave", PARTITIONS,
        REPLICA, REPLICA-1);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    int upwardSTCountBeforeDisableThenEvacuate = stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter();
    int downwardSTCountBeforeDisableThenEvacuate = stateTransitionCountStateModelFactory.getDownwardStateTransitionCounter();


    InstanceConfig instanceConfig = _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME,
        toDisableThenEvacuateInstanceName);
    instanceConfig.setInstanceEnabledForPartition(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", false);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, toDisableThenEvacuateInstanceName, instanceConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    // EV should not have disabled instance above the lowest state (OFFLINE)
    verifier(() -> {
      for (String resource : testResources) {
        ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, resource);
        for (String partition : ev.getPartitionSet()) {
          if (ev.getStateMap(partition).containsKey(toDisableThenEvacuateInstanceName) && !ev.getStateMap(partition).
              get(toDisableThenEvacuateInstanceName).equals("OFFLINE")) {
            return false;
          }
        }
      }
      return true;
    }, 5000);

    // Assert node received downward state transitions and no upward transitions
    Assert.assertEquals(stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter(),
        upwardSTCountBeforeDisableThenEvacuate, "Upward state transitions should not have been received");
    Assert.assertTrue(stateTransitionCountStateModelFactory.getDownwardStateTransitionCounter() >
        downwardSTCountBeforeDisableThenEvacuate, "Should have received downward state transitions");

    _gSetupTool.getClusterManagementTool().setInstanceOperation(CLUSTER_NAME,
        toDisableThenEvacuateInstanceName, InstanceConstants.InstanceOperation.EVACUATE);

    verifier(() -> _admin.isEvacuateFinished(CLUSTER_NAME, toDisableThenEvacuateInstanceName), 30000);
    int downwardSTCountAfterEvacuateComplete = stateTransitionCountStateModelFactory.getDownwardStateTransitionCounter();

    // Assert node received no upward state transitions after evacuation was called on already disabled node
    Assert.assertEquals(stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter(),
        upwardSTCountBeforeDisableThenEvacuate, "Upward state transitions should not have been received");

    // Re-enable all partitions for the instance
    instanceConfig = _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME,
        toDisableThenEvacuateInstanceName);
    instanceConfig.setInstanceEnabledForPartition(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", true);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, toDisableThenEvacuateInstanceName, instanceConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Assert node received no upward state transitions after re-enabled partitions
    Assert.assertEquals(stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter(),
        upwardSTCountBeforeDisableThenEvacuate, "Upward state transitions should not have been received");

    // Disable all partitions for the instance again
    instanceConfig = _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME,
        toDisableThenEvacuateInstanceName);
    instanceConfig.setInstanceEnabledForPartition(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", false);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, toDisableThenEvacuateInstanceName, instanceConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Assert node received no upward state transitions after disabling already evacuated node
    Assert.assertEquals(stateTransitionCountStateModelFactory.getUpwardStateTransitionCounter(),
        upwardSTCountBeforeDisableThenEvacuate, "Upward state transitions should not have been received");
    Assert.assertEquals(stateTransitionCountStateModelFactory.getDownwardStateTransitionCounter(),
        downwardSTCountAfterEvacuateComplete, "Downward state transitions should not have been received");


    // Clean up test resources
    for (String resource : testResources) {
      _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resource);
    }
    // Clean up test participant
    toDisableThenEvacuateParticipant.syncStop();
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

  private static class CustomIndividualInstanceConfigChangeListener implements InstanceConfigChangeListener {
    private boolean throttlesEnabled;

    public CustomIndividualInstanceConfigChangeListener() {
      throttlesEnabled = true;
    }

    public boolean isThrottlesEnabled() {
      return throttlesEnabled;
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfig,
        NotificationContext context) {
      if (instanceConfig.get(0).getInstanceOperation().getOperation()
          .equals(InstanceConstants.InstanceOperation.SWAP_IN)) {
        throttlesEnabled = false;
      } else {
        throttlesEnabled = true;
      }
    }
  }

  private MockParticipantManager createParticipant(String participantName, StateModelFactory stateModelFactory) throws Exception {
    // start dummy participants
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, participantName, 10, null);
    StateMachineEngine stateMachine = participant.getStateMachineEngine();
    // Default to delayed statemodel if stateModel not provided
    stateMachine.registerStateModelFactory("MasterSlave", stateModelFactory != null ?
        stateModelFactory : new StDelayMSStateModelFactory());
    return participant;
  }

  private void addParticipant(String participantName) throws Exception {
    addParticipant(participantName, UUID.randomUUID().toString(),
        "zone_" + _participants.size() % ZONE_COUNT, null, -1);
  }

  private void addParticipant(String participantName, StateModelFactory stateModelFactory) throws Exception {
    addParticipant(participantName, UUID.randomUUID().toString(),
        "zone_" + _participants.size() % ZONE_COUNT, null, -1, null, stateModelFactory);
  }

  private void addParticipant(String participantName, String logicalId, String zone,
      InstanceConstants.InstanceOperation instanceOperation, int capacity)
      throws Exception {
    addParticipant(participantName, logicalId, zone, instanceOperation, capacity, null, null);
  }

  private void addParticipant(String participantName, String logicalId, String zone,
      InstanceConstants.InstanceOperation instanceOperation, int capacity,
      InstanceConfigChangeListener listener, StateModelFactory stateModelFactory) throws Exception {
    InstanceConfig config = new InstanceConfig.Builder().setDomain(
            String.format("%s=%s, %s=%s, %s=%s", ZONE, zone, HOST, participantName, LOGICAL_ID,
                logicalId)).setInstanceOperation(instanceOperation)
        .build(participantName);

    if (capacity >= 0) {
      config.setInstanceCapacityMap(ImmutableMap.of(TEST_CAPACITY_KEY, capacity));
    }
    _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, config);

    MockParticipantManager participant = createParticipant(participantName, stateModelFactory);

    participant.syncStart();
    if (listener != null) {
      participant.addListener(listener,
          new PropertyKey.Builder(CLUSTER_NAME).instanceConfig(participantName),
          HelixConstants.ChangeType.INSTANCE_CONFIG,
          new Watcher.Event.EventType[]{Watcher.Event.EventType.NodeDataChanged});
    }
    _participants.add(participant);
    _participantNames.add(participantName);
    _nextStartPort++;
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

  private Map<String, IdealState> getISs() {
    Map<String, IdealState> idealStates = new HashMap<String, IdealState>();
    for (String db : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      idealStates.put(db, is);
    }
    return idealStates;
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
      Set<String> completedSwapInInstanceNames, Set<String> allPartitionsDisabledInstances) {
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
          String expectedState = expectedStateMap.get(swapOutInstance).equals(stateModelDef.getTopState())
              ? (String) stateModelDef.getSecondTopStates().toArray()[0]
              : expectedStateMap.get(swapOutInstance);
          if (allPartitionsDisabledInstances.contains(swapOutInstancesToSwapInInstances.get(swapOutInstance))) {
            expectedState = stateModelDef.getInitialState();
          }
          expectedStateMap.put(swapOutInstancesToSwapInInstances.get(swapOutInstance), expectedState);
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
          swapOutInstancesToSwapInInstances, inFlightSwapInInstances, completedSwapInInstanceNames, Collections.emptySet());
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

  // State Transition Factory that has counters to track number of state transitions. The counters are shared across
  // all state models. You can register this state model for a single participant if you need to isolate the counter.
  public class StateTransitionCountStateModelFactory extends StateModelFactory<StateTransitionCountStateModel> {

    private final AtomicInteger _upwardStateTransitionCounter = new AtomicInteger(0);
    private final AtomicInteger _downwardStateTransitionCounter = new AtomicInteger(0);

      @Override
      public StateTransitionCountStateModel createNewStateModel(String resourceName, String partitionKey) {
        return new StateTransitionCountStateModel(_upwardStateTransitionCounter, _downwardStateTransitionCounter);
      }

      public int getUpwardStateTransitionCounter() {
        return _upwardStateTransitionCounter.get();
      }

      public int getDownwardStateTransitionCounter() {
        return _downwardStateTransitionCounter.get();
      }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {"MASTER", "SLAVE", "ERROR"})
  public class StateTransitionCountStateModel extends StateModel {
    AtomicInteger _upwardStateTransitionCounter;
    AtomicInteger _downwardStateTransitionCounter;
    public StateTransitionCountStateModel(AtomicInteger upwardStateTransitionCounter, AtomicInteger downwardStateTransitionCounter) {
      _upwardStateTransitionCounter = upwardStateTransitionCounter;
      _downwardStateTransitionCounter = downwardStateTransitionCounter;
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      _upwardStateTransitionCounter.incrementAndGet();
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      _upwardStateTransitionCounter.incrementAndGet();
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      _downwardStateTransitionCounter.incrementAndGet();
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      _downwardStateTransitionCounter.incrementAndGet();
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      _downwardStateTransitionCounter.incrementAndGet();
    }
  }
}
