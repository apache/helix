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


/**
 * Integration tests for the InstanceOperation feature in Helix.
 *
 * InstanceOperation allows administrators to control instance behavior during cluster rebalancing.
 * Supported operations:
 * - ENABLE: Normal operation, instance can receive replicas
 * - DISABLE: All replicas on the instance are set to OFFLINE
 * - EVACUATE: All replicas are moved off the instance (requires replacement bootstrap)
 * - SWAP_IN: Node receives replicas from corresponding swap-out node (same logicalId)
 * - UNKNOWN: Instance is not available for new assignments
 *
 * This test class covers:
 * 1. Basic EVACUATE functionality and cancellation
 * 2. SWAP_IN/SWAP_OUT node swapping with matching logicalId
 * 3. Interaction between InstanceOperation and deprecated enableInstance
 * 4. Behavior during maintenance mode
 * 5. Disabled partitions during swap/evacuate operations
 * 6. Routing table updates during instance operations
 */
public class TestInstanceOperation extends ZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestInstanceOperation.class);

  // Timeout for cluster verification
  private static final long VERIFICATION_TIMEOUT = 45000L;

  // Test cluster configuration
  private static final int ZONE_COUNT = 4;
  private static final int START_NUM_NODE = 10;
  private static final int START_PORT = 12918;
  private static final int PARTITIONS = 20;
  private static final int REPLICA = 3;

  // Topology configuration
  private static final String ZONE = "zone";
  private static final String HOST = "host";
  private static final String LOGICAL_ID = "logicalId";
  private static final String TOPOLOGY = String.format("%s/%s/%s", ZONE, HOST, LOGICAL_ID);

  // Acceptable states for validation
  private static final Set<String> TOP_STATE_SET = ImmutableSet.of("MASTER");
  private static final Set<String> SECONDARY_STATE_SET = ImmutableSet.of("SLAVE", "STANDBY");
  private static final Set<String> ACCEPTABLE_STATE_SET = ImmutableSet.of("MASTER", "LEADER", "SLAVE", "STANDBY");

  // Test capacity configuration
  private static final String TEST_CAPACITY_KEY = "TestCapacityKey";
  private static final int TEST_CAPACITY_VALUE = 100;

  // Test resources added during test suite
  private static final Set<String> TEMP_TEST_RESOURCES = new HashSet<>();

  // Instance naming
  private static int _nextStartPort = START_PORT;
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  // Test state
  protected ClusterControllerManager _controller;
  private HelixManager _spectator;
  private RoutingTableProvider _routingTableProviderDefault;
  private RoutingTableProvider _routingTableProviderEV;
  private RoutingTableProvider _routingTableProviderCS;
  private List<MockParticipantManager> _participants = new ArrayList<>();
  private List<String> _participantNames = new ArrayList<>();
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

    // Add initial participants
    for (int i = 0; i < START_NUM_NODE; i++) {
      String participantName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
      addParticipant(participantName);
    }

    // Start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Set up cluster verifiers
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

    // Start spectator for routing table validation
    _spectator = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "spectator", InstanceType.SPECTATOR, ZK_ADDR);
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
    // Drop all test resources
    for (String db : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    }
    for (String db : TEMP_TEST_RESOURCES) {
      try {
        _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
      } catch (Exception e) {
        LOG.warn("Failed to drop resource {}: {}", db, e.getMessage());
      }
    }
    TEMP_TEST_RESOURCES.clear();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Cleanup order: participants -> controller -> routing providers -> cluster
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    _controller.syncStop();
    _routingTableProviderDefault.shutdown();
    _routingTableProviderEV.shutdown();
    _routingTableProviderCS.shutdown();
    _spectator.disconnect();

    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    LOG.info("========== beforeMethod START ==========");
    long startTime = System.currentTimeMillis();

    // Remove any offline or inactive instances
    removeOfflineOrInactiveInstances();

    // Verify cluster is stable
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling(),
        "BestPossible verification failed");
    Assert.assertTrue(_clusterVerifier.verifyByPolling(),
        "Cluster verification failed");

    LOG.info("========== beforeMethod END (took {} ms) ==========", System.currentTimeMillis() - startTime);
  }

  // ==================== EVACUATE Tests ====================

  /**
   * Tests basic EVACUATE functionality - evacuate an instance and verify replicas are moved off.
   */
  @Test
  public void testEvacuate() throws Exception {
    LOG.info("START testEvacuate");

    // Create semi-auto DB for testing
    String semiAutoDB = "SemiAutoTestDB_1";
    List<String> participantNames = _participants.stream()
        .map(ZKHelixManager::getInstanceName)
        .collect(Collectors.toList());
    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, semiAutoDB,
        participantNames,
        BuiltInStateModelDefinitions.OnlineOffline.name(), 1, _participants.size());

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify EV contains all participants
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames),
          "EV should contain all participants for resource: " + resource);
    }

    // Evacuate the first instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify evacuated instance is not in EV, but all others are
    assignment = getEVs();
    List<String> currentActiveInstances = _participantNames.stream()
        .filter(n -> !n.equals(instanceToEvacuate))
        .collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
      Set<String> newAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newAssignedParticipants.contains(instanceToEvacuate),
          "Evacuated instance should not be in EV");
      Assert.assertTrue(newAssignedParticipants.containsAll(currentActiveInstances),
          "EV should contain all active instances");
    }

    // Verify evacuate status
    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate),
        "Evacuation should be finished");
    Assert.assertTrue(_admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, instanceToEvacuate),
        "Instance should be ready for preparing to join");

    // Cleanup
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, semiAutoDB);
    _allDBs.remove(semiAutoDB);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    LOG.info("END testEvacuate");
  }

  /**
   * Tests that EVACUATE can be reverted by setting operation back to ENABLE.
   */
  @Test(dependsOnMethods = "testEvacuate")
  public void testRevertEvacuation() throws Exception {
    LOG.info("START testRevertEvacuation");

    String instanceToRevert = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToRevert, InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceToRevert)
            .getInstanceEnabled(),
        "Instance should be enabled after reverting evacuation");
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify all participants are back in EV
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames),
          "All participants should be in EV after reverting evacuation");
      validateAssignmentInEv(assignment.get(resource));
    }

    LOG.info("END testRevertEvacuation");
  }

  /**
   * Tests that disabling an instance takes precedence over EVACUATE operation.
   * When HELIX_ENABLED is false, InstanceOperation returns DISABLE.
   */
  @Test(dependsOnMethods = "testRevertEvacuation")
  public void testDisableTakesPrecedenceOverEvacuate() throws Exception {
    LOG.info("START testDisableTakesPrecedenceOverEvacuate");

    String instanceToTest = _participants.get(0).getInstanceName();

    // First disable the instance
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceToTest, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Now try to set EVACUATE - it should remain overridden by DISABLE
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToTest, InstanceConstants.InstanceOperation.EVACUATE);

    // Enable the instance so EVACUATE is no longer overridden
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceToTest, true);

    // Verify EVACUATE is now in effect
    Map<String, ExternalView> assignment = getEVs();
    List<String> currentActiveInstances = _participantNames.stream()
        .filter(n -> !n.equals(instanceToTest))
        .collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource), REPLICA - 1);
      Set<String> assignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(assignedParticipants.contains(instanceToTest),
          "Instance should not be in EV after EVACUATE");
      Assert.assertTrue(assignedParticipants.containsAll(currentActiveInstances),
          "All other instances should be in EV");
    }

    // Remove EVACUATE operation
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToTest, InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify instance is back
    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames),
          "All participants should be in EV after removing EVACUATE");
      validateAssignmentInEv(assignment.get(resource));
    }

    LOG.info("END testDisableTakesPrecedenceOverEvacuate");
  }

  // ==================== SWAP_IN/SWAP_OUT Tests ====================

  /**
   * Tests that SWAP_IN requires topology configuration to match logicalId.
   * Without topology, SWAP_IN defaults to UNKNOWN.
   */
  @Test(dependsOnMethods = "testDisableTakesPrecedenceOverEvacuate")
  public void testSwapInRequiresTopologyForLogicalIdMatch() throws Exception {
    LOG.info("START testSwapInRequiresTopologyForLogicalIdMatch");

    // Get current first participant as swap-out candidate
    String swapOutInstanceName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutInstanceName);

    // Try to add a swap-in without topology setup - should result in UNKNOWN
    String swapInInstanceName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(swapInInstanceName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Without topology, SWAP_IN cannot match logicalId, so it becomes UNKNOWN
    InstanceConstants.InstanceOperation actualOp =
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, swapInInstanceName)
            .getInstanceOperation().getOperation();
    Assert.assertEquals(actualOp, InstanceConstants.InstanceOperation.UNKNOWN,
        "SWAP_IN should become UNKNOWN without topology for logicalId matching");

    LOG.info("END testSwapInRequiresTopologyForLogicalIdMatch");
  }

  /**
   * Tests that adding an ENABLE instance with a duplicate logicalId becomes UNKNOWN.
   */
  @Test(dependsOnMethods = "testSwapInRequiresTopologyForLogicalIdMatch")
  public void testDuplicateLogicalIdWithEnableBecomesUnknown() throws Exception {
    LOG.info("START testDuplicateLogicalIdWithEnableBecomesUnknown");

    enableTopologyAwareRebalance();
    removeOfflineOrInactiveInstances();

    String swapOutInstanceName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutInstanceName);

    // Add instance with same logicalId and ENABLE operation
    String instanceToAddName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToAddName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.ENABLE, -1);

    // Duplicate logicalId with ENABLE should be UNKNOWN
    InstanceConstants.InstanceOperation actualOp =
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToAddName)
            .getInstanceOperation().getOperation();
    Assert.assertEquals(actualOp, InstanceConstants.InstanceOperation.UNKNOWN,
        "Instance with duplicate logicalId and ENABLE should become UNKNOWN");

    LOG.info("END testDuplicateLogicalIdWithEnableBecomesUnknown");
  }

  /**
   * Tests that SWAP_IN with no matching swap-out instance becomes UNKNOWN.
   */
  @Test(dependsOnMethods = "testDuplicateLogicalIdWithEnableBecomesUnknown")
  public void testSwapInWithoutMatchingSwapOutBecomesUnknown() throws Exception {
    LOG.info("START testSwapInWithoutMatchingSwapOutBecomesUnknown");

    removeOfflineOrInactiveInstances();

    // Add new instance with SWAP_IN but non-existent logicalId
    String instanceToAddName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToAddName, "non_existent_logical_id", "zone_non_existent",
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // No matching swap-out should result in UNKNOWN
    InstanceConstants.InstanceOperation actualOp =
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, instanceToAddName)
            .getInstanceOperation().getOperation();
    Assert.assertEquals(actualOp, InstanceConstants.InstanceOperation.UNKNOWN,
        "SWAP_IN without matching swap-out should become UNKNOWN");

    LOG.info("END testSwapInWithoutMatchingSwapOutBecomesUnknown");
  }

  /**
   * Tests that SWAP_IN can be set explicitly when InstanceOperation is initially unset.
   */
  @Test(dependsOnMethods = "testSwapInWithoutMatchingSwapOutBecomesUnknown")
  public void testSetSwapInOperationExplicitly() throws Exception {
    LOG.info("START testSetSwapInOperationExplicitly");

    removeOfflineOrInactiveInstances();

    String swapOutInstanceName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutInstanceName);

    // Add instance without setting InstanceOperation (defaults to null/ENABLE)
    String swapInInstanceName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(swapInInstanceName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE), null, -1);

    // Should be UNKNOWN due to duplicate logicalId
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, swapInInstanceName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    // Now explicitly set SWAP_IN - should work since logicalId matches
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, swapInInstanceName, InstanceConstants.InstanceOperation.SWAP_IN);
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, swapInInstanceName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutInstanceName, false));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    LOG.info("END testSetSwapInOperationExplicitly");
  }

  /**
   * Tests that adding a second SWAP_IN with same logicalId throws HelixException.
   */
  @Test(dependsOnMethods = "testSetSwapInOperationExplicitly", expectedExceptions = HelixException.class)
  public void testDuplicateSwapInPairThrowsException() throws Exception {
    LOG.info("START testDuplicateSwapInPairThrowsException");

    String swapOutInstanceName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutInstanceName);

    // Add first SWAP_IN instance
    String firstSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(firstSwapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Second SWAP_IN with same logicalId should throw
    String secondSwapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(secondSwapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Should be UNKNOWN because first swap-in already exists
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, secondSwapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    // Trying to set SWAP_IN should throw
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, secondSwapInName, InstanceConstants.InstanceOperation.SWAP_IN);
  }

  /**
   * Tests basic node swap functionality - swap out one node, swap in another with same logicalId.
   */
  @Test(dependsOnMethods = "testDuplicateSwapInPairThrowsException")
  public void testNodeSwapBasic() throws Exception {
    LOG.info("START testNodeSwapBasic");

    Map<String, String> swapOutToSwapIn = new HashMap<>();

    String swapOutName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    // Disable one partition on swap-out for testing
    String resourceToDisable = _allDBs.iterator().next();
    getPartitionsAndStatesOnInstance(getEVs(), swapOutName).entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(resourceToDisable))
        .findFirst()
        .ifPresent(entry -> {
          swapOutConfig.setInstanceEnabledForPartition(resourceToDisable, entry.getKey(), false);
        });
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, swapOutName, swapOutConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Store original EV for comparison
    Map<String, ExternalView> originalEVs = getEVs();

    // Create listener to track throttle changes
    CustomIndividualInstanceConfigChangeListener configListener =
        new CustomIndividualInstanceConfigChangeListener();

    // Add swap-in instance
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutToSwapIn.put(swapOutName, swapInName);
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1, configListener, null);

    // SWAP_IN should have throttles disabled
    Assert.assertFalse(configListener.isThrottlesEnabled(),
        "SWAP_IN instance should have throttles disabled");

    // Verify swap-in has same partitions but all OFFLINE
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        ImmutableSet.of(swapInName), Collections.emptySet());

    // Verify can complete swap
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutName),
        "Swap should be completable");

    // Verify routing tables - swap-out in, swap-in out
    validateRoutingTablesInstance(getEVs(), swapOutName, true);
    validateRoutingTablesInstance(getEVs(), swapInName, false);

    // Complete the swap
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutName, false),
        "Swap should complete successfully");

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify swap-in is now in routing tables
    validateRoutingTablesInstance(getEVs(), swapInName, true);

    // Verify swap-out is disabled with UNKNOWN operation
    InstanceConfig swapOutInstanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);
    Assert.assertFalse(swapOutInstanceConfig.getInstanceEnabled(),
        "Swap-out instance should be disabled");
    Assert.assertEquals(swapOutInstanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN,
        "Swap-out should have UNKNOWN operation");

    // Throttles should be re-enabled after swap
    Assert.assertTrue(configListener.isThrottlesEnabled(),
        "Throttles should be re-enabled after swap");

    // Verify final EV is correct
    verifier(() -> validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), ImmutableSet.of(swapInName)), VERIFICATION_TIMEOUT);

    // Re-enable the disabled partition on swap-in
    InstanceConfig swapInConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapInName);
    swapInConfig.setInstanceEnabledForPartition(resourceToDisable, true);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, swapInName, swapInConfig);

    LOG.info("END testNodeSwapBasic");
  }

  /**
   * Tests that disabling/re-enabling swap-out during swap does not affect the swap process.
   */
  @Test(dependsOnMethods = "testNodeSwapBasic")
  public void testSwapNotAffectedByDisableReenableOfSwapOut() throws Exception {
    LOG.info("START testSwapNotAffectedByDisableReenableOfSwapOut");

    Map<String, String> swapOutToSwapIn = new HashMap<>();

    String swapOutName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    Map<String, ExternalView> originalEVs = getEVs();

    // Add swap-in
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutToSwapIn.put(swapOutName, swapInName);
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        ImmutableSet.of(swapInName), Collections.emptySet());

    // Try to disable swap-out - should not affect swap
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, swapOutName, false);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Verify swap-out has all OFFLINE and swap-in has all OFFLINE replicas
    Map<String, Map<String, String>> swapOutState =
        getResourcePartitionStateOnInstance(getEVs(), swapOutName);
    Map<String, Map<String, String>> swapInState =
        getResourcePartitionStateOnInstance(getEVs(), swapInName);

    Assert.assertEquals(
        swapOutState.values().stream().flatMap(p -> p.keySet().stream()).collect(Collectors.toSet()),
        swapInState.values().stream().flatMap(p -> p.keySet().stream()).collect(Collectors.toSet()),
        "Swap-out and swap-in should have same partitions");

    Assert.assertEquals(
        swapOutState.values().stream().flatMap(e -> e.values().stream()).collect(Collectors.toSet()),
        ImmutableSet.of("OFFLINE"),
        "Swap-out should have all OFFLINE replicas");
    Assert.assertEquals(
        swapInState.values().stream().flatMap(e -> e.values().stream()).collect(Collectors.toSet()),
        ImmutableSet.of("OFFLINE"),
        "Swap-in should have all OFFLINE replicas");

    // Re-enable swap-out - should still not affect swap
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, swapOutName, true);
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Complete swap
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutName, false),
        "Swap should complete");

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify swap-out is disabled
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName).getInstanceEnabled(),
        "Swap-out should be disabled");

    // Verify final EV
    verifier(() -> validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), ImmutableSet.of(swapInName)), VERIFICATION_TIMEOUT);

    LOG.info("END testSwapNotAffectedByDisableReenableOfSwapOut");
  }

  /**
   * Tests that SWAP_IN can be set after instance is added without operation.
   */
  @Test(dependsOnMethods = "testSwapNotAffectedByDisableReenableOfSwapOut")
  public void testSwapInCanBeSetAfterInstanceAdded() throws Exception {
    LOG.info("START testSwapInCanBeSetAfterInstanceAdded");

    Map<String, String> swapOutToSwapIn = new HashMap<>();
    Map<String, ExternalView> originalEVs = getEVs();

    String swapOutName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    // Add instance without setting InstanceOperation
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutToSwapIn.put(swapOutName, swapInName);
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE), null, -1);

    // Should have no partitions initially
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), Collections.emptySet());

    // Now set SWAP_IN
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, swapInName, InstanceConstants.InstanceOperation.SWAP_IN);

    // Verify SWAP_IN takes effect
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        ImmutableSet.of(swapInName), Collections.emptySet());

    // Complete swap
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutName, false),
        "Swap should complete");

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify final state
    verifier(() -> validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), ImmutableSet.of(swapInName)), VERIFICATION_TIMEOUT);

    LOG.info("END testSwapInCanBeSetAfterInstanceAdded");
  }

  /**
   * Tests that swap can be cancelled by setting SWAP_IN to UNKNOWN.
   */
  @Test(dependsOnMethods = "testSwapInCanBeSetAfterInstanceAdded")
  public void testSwapCanBeCancelled() throws Exception {
    LOG.info("START testSwapCanBeCancelled");

    Map<String, String> swapOutToSwapIn = new HashMap<>();
    Map<String, ExternalView> originalEVs = getEVs();

    String swapOutName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    // Add swap-in
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutToSwapIn.put(swapOutName, swapInName);
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        ImmutableSet.of(swapInName), Collections.emptySet());

    // Verify can complete
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutName),
        "Should be able to complete swap");

    // Cancel swap by setting to UNKNOWN
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, swapInName, InstanceConstants.InstanceOperation.UNKNOWN);

    // Verify no partitions on swap-in
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), Collections.emptySet());
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify swap-in has no partitions
    Assert.assertEquals(getPartitionsAndStatesOnInstance(getEVs(), swapInName).size(), 0,
        "Swap-in should have no partitions after cancellation");

    // Verify swap-out is still normal
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), Collections.emptySet());

    // Revert swap-out to normal
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, swapOutName, InstanceConstants.InstanceOperation.ENABLE);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), Collections.emptySet());

    LOG.info("END testSwapCanBeCancelled");
  }

  /**
   * Tests that SWAP_IN during maintenance mode doesn't compute new assignments.
   */
  @Test(dependsOnMethods = "testSwapCanBeCancelled")
  public void testSwapInDuringMaintenanceMode() throws Exception {
    LOG.info("START testSwapInDuringMaintenanceMode");

    Map<String, String> swapOutToSwapIn = new HashMap<>();
    Map<String, ExternalView> originalEVs = getEVs();

    // Enter maintenance mode
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);

    String swapOutName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    // Add swap-in during MM
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutToSwapIn.put(swapOutName, swapInName);
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // During MM, no new assignments should be computed
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), Collections.emptySet());

    // Exit maintenance mode - swap should begin
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Now swap should be in progress
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        ImmutableSet.of(swapInName), Collections.emptySet());

    // Verify routing tables
    validateRoutingTablesInstance(getEVs(), swapOutName, true);
    validateRoutingTablesInstance(getEVs(), swapInName, false);

    // Complete swap
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutName),
        "Should be able to complete swap");
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutName, false),
        "Swap should complete");

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify swap-in in routing tables
    validateRoutingTablesInstance(getEVs(), swapInName, true);

    // Verify final EV
    verifier(() -> validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), ImmutableSet.of(swapInName)), VERIFICATION_TIMEOUT);

    LOG.info("END testSwapInDuringMaintenanceMode");
  }

  /**
   * Tests swap when swap-out instance is already disabled.
   */
  @Test(dependsOnMethods = "testSwapInDuringMaintenanceMode")
  public void testSwapWithSwapOutDisabled() throws Exception {
    LOG.info("START testSwapWithSwapOutDisabled");

    Map<String, ExternalView> originalEVs = getEVs();

    String swapOutName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    // Disable swap-out first
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, swapOutName, false);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify swap-out has all OFFLINE
    Set<String> swapOutStates = new HashSet<>(
        getPartitionsAndStatesOnInstance(getEVs(), swapOutName).values());
    Assert.assertEquals(swapOutStates.size(), 1);
    Assert.assertTrue(swapOutStates.contains("OFFLINE"));

    // Add swap-in
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Swap-in should have no partitions because swap-out was disabled
    Map<String, String> swapInPartitions =
        getPartitionsAndStatesOnInstance(getEVs(), swapInName);
    Assert.assertEquals(swapInPartitions.size(), 0,
        "Swap-in should have no partitions when swap-out is disabled");

    // Can still complete swap
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutName),
        "Should be able to complete swap");

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutName, false),
        "Swap should complete");

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Swap-out should remain disabled
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName).getInstanceEnabled(),
        "Swap-out should remain disabled");

    // Swap-out should have no partitions
    verifier(() -> getPartitionsAndStatesOnInstance(getEVs(), swapOutName).isEmpty(),
        VERIFICATION_TIMEOUT);

    LOG.info("END testSwapWithSwapOutDisabled");
  }

  /**
   * Tests swap when swap-out instance goes offline during swap.
   */
  @Test(dependsOnMethods = "testSwapWithSwapOutDisabled")
  public void testSwapWithSwapOutGoesOffline() throws Exception {
    LOG.info("START testSwapWithSwapOutGoesOffline");

    Map<String, String> swapOutToSwapIn = new HashMap<>();
    Map<String, ExternalView> originalEVs = getEVs();

    String swapOutName = _participants.get(0).getInstanceName();
    LOG.info("Swap-out instance: {}", swapOutName);

    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    // Add swap-in
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    LOG.info("Adding swap-in instance: {}", swapInName);
    swapOutToSwapIn.put(swapOutName, swapInName);
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    // Verify SWAP_IN is set
    Assert.assertEquals(
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, swapInName)
            .getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN,
        "SWAP_IN should be set");

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Stop swap-out participant
    LOG.info("Stopping swap-out participant: {}", swapOutName);
    _participants.get(0).syncStop();

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Can complete swap
    boolean canComplete = _gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutName);
    LOG.info("canCompleteSwap result: {}", canComplete);
    Assert.assertTrue(canComplete, "Should be able to complete swap");

    // Complete swap
    boolean completed = _gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutName, false);
    LOG.info("completeSwapIfPossible result: {}", completed);
    Assert.assertTrue(completed, "Swap should complete");

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify swap-in in routing tables
    validateRoutingTablesInstance(getEVs(), swapInName, true);

    // Verify swap-out is disabled
    boolean isEnabled = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName).getInstanceEnabled();
    LOG.info("Swap-out enabled: {}", isEnabled);
    Assert.assertFalse(isEnabled, "Swap-out should be disabled");

    // Verify final EV
    verifier(() -> validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), ImmutableSet.of(swapInName)), VERIFICATION_TIMEOUT);

    LOG.info("END testSwapWithSwapOutGoesOffline");
  }

  /**
   * Tests EVACUATE + ADD new instance + maintenance mode + swap pattern.
   */
  @Test(dependsOnMethods = "testSwapWithSwapOutGoesOffline")
  public void testEvacuateWithAddDuringMaintenanceMode() throws Exception {
    LOG.info("START testEvacuateWithAddDuringMaintenanceMode");

    removeOfflineOrInactiveInstances();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, String> swapOutToSwapIn = new HashMap<>();
    Map<String, ExternalView> originalEVs = getEVs();

    // Enter maintenance mode
    LOG.info("Entering maintenance mode");
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Set evacuate on an instance
    String swapOutName = _participants.get(0).getInstanceName();
    LOG.info("Setting EVACUATE on instance: {}", swapOutName);
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, swapOutName, InstanceConstants.InstanceOperation.EVACUATE);

    // Assignment should not change yet
    validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
        Collections.emptySet(), Collections.emptySet());

    // Add new instance with ENABLE
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    LOG.info("Adding new instance: {}", swapInName);
    swapOutToSwapIn.put(swapOutName, swapInName);
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.ENABLE, -1);

    // Exit maintenance mode
    LOG.info("Exiting maintenance mode");
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify swap-in has the same partitions swap-out had
    boolean swapValid = TestHelper.verify(() ->
        validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
            Collections.emptySet(), ImmutableSet.of(swapInName)),
        VERIFICATION_TIMEOUT);
    Assert.assertTrue(swapValid, "Swap validation failed");

    // Verify evacuation finished
    boolean evacFinished = _gSetupTool.getClusterManagementTool()
        .isEvacuateFinished(CLUSTER_NAME, swapOutName);
    LOG.info("isEvacuateFinished: {}", evacFinished);
    Assert.assertTrue(evacFinished, "Evacuation should be finished");

    // Set evacuate instance to UNKNOWN
    LOG.info("Setting UNKNOWN on evacuate instance");
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, swapOutName, InstanceConstants.InstanceOperation.UNKNOWN);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Final validation
    boolean finalValid = TestHelper.verify(() ->
        validateEVsCorrect(getEVs(), originalEVs, swapOutToSwapIn,
            Collections.emptySet(), ImmutableSet.of(swapInName)),
        VERIFICATION_TIMEOUT);
    Assert.assertTrue(finalValid, "Final validation failed");

    LOG.info("END testEvacuateWithAddDuringMaintenanceMode");
  }

  /**
   * Tests that removing EVACUATE when there's a matching swap-in throws HelixException.
   */
  @Test(dependsOnMethods = "testEvacuateWithAddDuringMaintenanceMode", expectedExceptions = HelixException.class)
  public void testCannotRemoveEvacuateWithMatchingSwapIn() throws Exception {
    LOG.info("START testCannotRemoveEvacuateWithMatchingSwapIn");

    String swapOutName = _participants.get(0).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    // Set evacuate
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, swapOutName, InstanceConstants.InstanceOperation.EVACUATE);

    // Add instance with same logicalId and ENABLE
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.ENABLE, -1);

    // Try to remove evacuate - should throw
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, swapOutName, InstanceConstants.InstanceOperation.ENABLE);
  }

  /**
   * Tests that UNKNOWN operation does not trigger rebalance.
   */
  @Test(dependsOnMethods = "testCannotRemoveEvacuateWithMatchingSwapIn")
  public void testUnknownDoesNotTriggerRebalance() throws Exception {
    LOG.info("START testUnknownDoesNotTriggerRebalance");

    Map<String, IdealState> idealStatesBefore = getISs();

    // Add instance with UNKNOWN - should not be considered in placement
    String instanceToAdd = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToAdd, "foo", "bar", InstanceConstants.InstanceOperation.UNKNOWN, -1);
    _participants.get(_participants.size() - 1).syncStop();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // No rebalance should occur
    Assert.assertEquals(idealStatesBefore, getISs(),
        "UNKNOWN operation should not trigger rebalance");

    // Same logicalId with UNKNOWN should also not trigger rebalance
    InstanceConfig existingConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, _participants.get(0).getInstanceName());

    String instanceToAdd2 = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(instanceToAdd2, existingConfig.getLogicalId(LOGICAL_ID),
        existingConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.UNKNOWN, -1);
    _participants.get(_participants.size() - 1).syncStop();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertEquals(idealStatesBefore, getISs(),
        "UNKNOWN with duplicate logicalId should not trigger rebalance");

    LOG.info("END testUnknownDoesNotTriggerRebalance");
  }

  // ==================== Disabled Partitions Tests ====================

  /**
   * Tests disabled partitions before swap is initiated.
   */
  @Test(dependsOnMethods = "testUnknownDoesNotTriggerRebalance")
  public void testDisabledPartitionsBeforeSwap() throws Exception {
    LOG.info("START testDisabledPartitionsBeforeSwap");

    enableTopologyAwareRebalance();

    String newParticipant = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    removeOfflineOrInactiveInstances();
    addParticipant(newParticipant);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> beforeEVs = getEVs();
    Map<String, String> swapOutToSwapIn = new HashMap<>();

    String swapOutName = _participants.get(_participants.size() - 1).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    // Add swap-in with UNKNOWN first
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    swapOutToSwapIn.put(swapOutName, swapInName);
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.UNKNOWN, -1);

    _clusterVerifier.verifyByPolling();

    // Disable all partitions on swap-in and set to SWAP_IN
    InstanceConfig swapInConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapInName);
    swapInConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", false);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, swapInName, swapInConfig);
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, swapInName, InstanceConstants.InstanceOperation.SWAP_IN);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // All states should be OFFLINE
    for (String resource : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, resource);
      ExternalView ev = beforeEVs.get(resource);
      for (String partition : is.getPartitionSet()) {
        if (ev.getStateMap(partition).containsKey(swapOutName)) {
          Assert.assertEquals(
              is.getInstanceStateMap(partition).get(swapInName),
              "OFFLINE",
              "Swap-in should be OFFLINE when all partitions disabled");
        }
      }
    }

    // Cannot complete swap because swap-in has no valid current states
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutName),
        "Should not be able to complete swap with all partitions disabled");

    // Re-enable partitions
    swapInConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapInName);
    swapInConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", true);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, swapInName, swapInConfig);

    // Now can complete swap
    Assert.assertTrue(TestHelper.verify(() ->
        _gSetupTool.getClusterManagementTool()
            .canCompleteSwap(CLUSTER_NAME, swapOutName),
    30000), "Should be able to complete swap after re-enabling partitions");

    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutName, false),
        "Swap should complete");

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), beforeEVs, swapOutToSwapIn,
        Collections.emptySet(), ImmutableSet.of(swapInName));

    // Cleanup
    _participants.get(_participants.size() - 1).syncStop();

    LOG.info("END testDisabledPartitionsBeforeSwap");
  }

  /**
   * Tests disabled partitions after swap is initiated.
   */
  @Test(dependsOnMethods = "testDisabledPartitionsBeforeSwap")
  public void testDisabledPartitionsAfterSwap() throws Exception {
    LOG.info("START testDisabledPartitionsAfterSwap");

    String newParticipant = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    removeOfflineOrInactiveInstances();
    addParticipant(newParticipant);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> beforeSwapDisableEVs = getEVs();

    String swapOutName = _participants.get(_participants.size() - 1).getInstanceName();
    InstanceConfig swapOutConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapOutName);

    // Add swap-in with SWAP_IN
    String swapInName = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    Map<String, String> swapOutToSwapIn = new HashMap<>();
    swapOutToSwapIn.put(swapOutName, swapInName);
    addParticipant(swapInName, swapOutConfig.getLogicalId(LOGICAL_ID),
        swapOutConfig.getDomainAsMap().get(ZONE),
        InstanceConstants.InstanceOperation.SWAP_IN, -1);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutName),
        "Should be able to complete swap initially");

    // Get EVs before disable for validation
    Map<String, ExternalView> beforeDisableEVs = getEVs();
    validateEVsCorrect(beforeDisableEVs, beforeSwapDisableEVs, swapOutToSwapIn,
        ImmutableSet.of(swapInName), Collections.emptySet());

    // Disable all partitions on swap-in
    InstanceConfig swapInConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapInName);
    swapInConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", false);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, swapInName, swapInConfig);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());

    // Cannot complete because swap-in has disabled partitions
    Assert.assertFalse(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutName),
        "Should not be able to complete with disabled partitions");

    // EV should have swap-in with OFFLINE states (extra replica)
    Map<String, ExternalView> currentEVs = getEVs();
    for (String resource : _allDBs) {
      validateEVCorrect(currentEVs.get(resource), beforeDisableEVs.get(resource),
          swapOutToSwapIn, ImmutableSet.of(swapInName), Collections.emptySet(),
          ImmutableSet.of(swapInName));
    }

    // Re-enable partitions
    swapInConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, swapInName);
    swapInConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", true);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, swapInName, swapInConfig);

    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .canCompleteSwap(CLUSTER_NAME, swapOutName),
        "Should be able to complete after re-enabling");
    Assert.assertTrue(_gSetupTool.getClusterManagementTool()
        .completeSwapIfPossible(CLUSTER_NAME, swapOutName, false),
        "Swap should complete");

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    validateEVsCorrect(getEVs(), beforeSwapDisableEVs, swapOutToSwapIn,
        Collections.emptySet(), ImmutableSet.of(swapInName));

    // Cleanup
    _participants.get(_participants.size() - 1).syncStop();

    LOG.info("END testDisabledPartitionsAfterSwap");
  }

  // ==================== Evacuation with Delayed Transitions ====================

  /**
   * Tests evacuate cancellation before bootstrap finishes.
   */
  @Test(dependsOnMethods = "testDisabledPartitionsAfterSwap", timeOut = 600000)
  public void testEvacuateCancelBeforeBootstrapFinishes() throws Exception {
    LOG.info("START testEvacuateCancelBeforeBootstrapFinishes");

    // Create resources with slow upward transitions
    String delayedCrushedDB = "TEST_DB_DELAYED_CRUSHED";
    createResourceWithDelayedRebalance(CLUSTER_NAME, delayedCrushedDB, "MasterSlave",
        PARTITIONS, REPLICA, REPLICA - 1, 200000, CrushEdRebalanceStrategy.class.getName());
    _allDBs.add(delayedCrushedDB);

    String delayedWagedDB = "TEST_DB_DELAYED_WAGED";
    createResourceWithWagedRebalance(CLUSTER_NAME, delayedWagedDB, "MasterSlave",
        PARTITIONS, REPLICA, REPLICA - 1);
    _allDBs.add(delayedWagedDB);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Set large upward delay
    _stateModelDelay = -10000L;

    // Evacuate an instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // Messages should be pending at other instances
    for (String participant : _participantNames) {
      if (!participant.equals(instanceToEvacuate)) {
        verifier(() -> _dataAccessor.getChildNames(
            _dataAccessor.keyBuilder().messages(participant)).isEmpty(),
        30000);
      }
    }

    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate),
        "Evacuation should not be finished yet");
    Assert.assertFalse(_admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, instanceToEvacuate),
        "Instance should not be ready for joining");

    // Let some state transitions start
    Thread.sleep(Math.abs(_stateModelDelay / 100));

    // Check EV before cancel
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
    }

    // Cancel evacuation
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);

    // Verify all instances still have valid assignments
    assignment = getEVs();
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
    }

    // Cluster should converge
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // All participants should be back
    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames),
          "All participants should be in EV after cancel");
      validateAssignmentInEv(assignment.get(resource));
    }

    // Cleanup
    _allDBs.remove(delayedCrushedDB);
    _allDBs.remove(delayedWagedDB);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, delayedCrushedDB);
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, delayedWagedDB);

    LOG.info("END testEvacuateCancelBeforeBootstrapFinishes");
  }

  /**
   * Tests evacuate cancellation before drop finishes.
   */
  @Test(dependsOnMethods = "testEvacuateCancelBeforeBootstrapFinishes", timeOut = 600000)
  public void testEvacuateCancelBeforeDropFinishes() throws Exception {
    LOG.info("START testEvacuateCancelBeforeDropFinishes");

    _stateModelDelay = 10000L;

    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // Messages should be pending
    verifier(() -> _dataAccessor.getChildNames(
        _dataAccessor.keyBuilder().messages(instanceToEvacuate)).isEmpty(),
    30000);

    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate),
        "Evacuation should not be finished");

    // Cancel evacuation
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.ENABLE);

    // Verify assignments
    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames),
          "All participants should be in EV");
      validateAssignmentInEv(assignment.get(resource));
    }

    _stateModelDelay = 3L;

    LOG.info("END testEvacuateCancelBeforeDropFinishes");
  }

  /**
   * Tests evacuation marking after maintenance mode.
   */
  @Test(dependsOnMethods = "testEvacuateCancelBeforeDropFinishes", timeOut = 600000)
  public void testEvacuateAfterMaintenanceMode() throws Exception {
    LOG.info("START testEvacuateAfterMaintenanceMode");

    _stateModelDelay = 1000L;

    Assert.assertFalse(_gSetupTool.getClusterManagementTool().isInMaintenanceMode(CLUSTER_NAME),
        "Should not be in maintenance mode");

    // Enter maintenance mode
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);

    // Add new participant during MM
    String newParticipant = PARTICIPANT_PREFIX + "_" + _nextStartPort;
    addParticipant(newParticipant);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> assignment = getEVs();
    for (String resource : _allDBs) {
      Assert.assertFalse(
          getParticipantsInEv(assignment.get(resource)).contains(newParticipant),
          "New participant should not be in EV during MM");
    }

    // Set evacuate on an instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // No evacuation should happen during MM
    for (String resource : _allDBs) {
      Assert.assertTrue(
          getParticipantsInEv(assignment.get(resource)).contains(instanceToEvacuate),
          "Evacuated instance should still be in EV during MM");
    }

    // Exit MM
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling(600000, 5000));

    // Now evacuation should complete
    assignment = getEVs();
    List<String> activeInstances = _participantNames.stream()
        .filter(n -> !n.equals(instanceToEvacuate))
        .collect(Collectors.toList());

    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
      Set<String> assignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(assignedParticipants.contains(instanceToEvacuate),
          "Evacuated instance should not be in EV");
      Assert.assertTrue(assignedParticipants.containsAll(activeInstances),
          "All active instances should be in EV");
    }

    Assert.assertTrue(_admin.isReadyForPreparingJoiningCluster(CLUSTER_NAME, instanceToEvacuate),
        "Instance should be ready for preparing to join");

    _stateModelDelay = 3L;

    LOG.info("END testEvacuateAfterMaintenanceMode");
  }

  /**
   * Tests evacuation with offline instances in cluster.
   */
  @Test(dependsOnMethods = "testEvacuateAfterMaintenanceMode")
  public void testEvacuationWithOfflineInstances() throws Exception {
    LOG.info("START testEvacuationWithOfflineInstances");

    // Stop two participants to simulate offline instances
    _participants.get(1).syncStop();
    _participants.get(2).syncStop();
    LOG.info("Stopped participants 1 and 2");

    String evacuateInstanceName = _participants.get(_participants.size() - 2).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, evacuateInstanceName, InstanceConstants.InstanceOperation.EVACUATE);
    LOG.info("Set EVACUATE on {}", evacuateInstanceName);

    // Verify evacuation completes despite offline instances
    verifier(() -> {
      Map<String, ExternalView> assignment = getEVs();
      for (String resource : _allDBs) {
        ExternalView ev = assignment.get(resource);
        if (ev == null) {
          continue;
        }
        for (String partition : ev.getPartitionSet()) {
          Map<String, String> stateMap = ev.getStateMap(partition);
          AtomicInteger activeReplicaCount = new AtomicInteger();
          stateMap.values().stream()
              .filter(v -> ACCEPTABLE_STATE_SET.contains(v))
              .forEach(v -> activeReplicaCount.getAndIncrement());

          // Check min active replicas (accounting for offline instances)
          if (activeReplicaCount.get() < REPLICA - 1) {
            LOG.info("Partition {} has only {} active replicas (required {})",
                partition, activeReplicaCount.get(), REPLICA - 1);
            return false;
          }

          // Check evacuating instance is not in top state
          if (stateMap.containsKey(evacuateInstanceName)) {
            String state = stateMap.get(evacuateInstanceName);
            if (state.equals("MASTER") || state.equals("LEADER")) {
              LOG.info("Evacuating instance still in top state {} for partition {}",
                  state, partition);
              return false;
            }
          }
        }
      }
      return true;
    }, 120000, CLUSTER_NAME);

    // Cleanup
    removeOfflineOrInactiveInstances();
    addParticipant(PARTICIPANT_PREFIX + "_" + _nextStartPort);
    addParticipant(PARTICIPANT_PREFIX + "_" + _nextStartPort);

    LOG.info("END testEvacuationWithOfflineInstances");
  }

  /**
   * Tests evacuate with disabled partitions.
   */
  @Test(dependsOnMethods = "testEvacuationWithOfflineInstances")
  public void testEvacuateWithDisabledPartition() throws Exception {
    LOG.info("START testEvacuateWithDisabledPartition");

    StateTransitionCountStateModelFactory stateModelFactory =
        new StateTransitionCountStateModelFactory();
    String testCrushedDB = "testEvacuateDisabledPartitions_CRUSHED";
    String testWagedDB = "testEvacuateDisabledPartitions_WAGED";
    String testInstanceName = "disable_then_evacuate_host";

    addParticipant(testInstanceName, stateModelFactory);
    MockParticipantManager testParticipant = _participants.get(_participants.size() - 1);

    // Create test resources
    createResourceWithDelayedRebalance(CLUSTER_NAME, testCrushedDB, "MasterSlave",
        PARTITIONS, REPLICA, REPLICA - 1, 200000, CrushEdRebalanceStrategy.class.getName());
    TEMP_TEST_RESOURCES.add(testCrushedDB);

    createResourceWithWagedRebalance(CLUSTER_NAME, testWagedDB, "MasterSlave",
        PARTITIONS, REPLICA, REPLICA - 1);
    TEMP_TEST_RESOURCES.add(testWagedDB);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Count transitions before disable
    int upwardBefore = stateModelFactory.getUpwardStateTransitionCounter();
    int downwardBefore = stateModelFactory.getDownwardStateTransitionCounter();

    // Disable all partitions
    InstanceConfig instanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, testInstanceName);
    instanceConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", false);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, testInstanceName, instanceConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Verify instance is OFFLINE in all partitions
    verifier(() -> {
      for (String resource : TEMP_TEST_RESOURCES) {
        ExternalView ev = _gSetupTool.getClusterManagementTool()
            .getResourceExternalView(CLUSTER_NAME, resource);
        for (String partition : ev.getPartitionSet()) {
          if (ev.getStateMap(partition).containsKey(testInstanceName) &&
              !ev.getStateMap(partition).get(testInstanceName).equals("OFFLINE")) {
            return false;
          }
        }
      }
      return true;
    }, VERIFICATION_TIMEOUT * 3);

    // Verify state transitions
    Assert.assertEquals(stateModelFactory.getUpwardStateTransitionCounter(), upwardBefore,
        "No upward transitions should occur when disabling");
    Assert.assertTrue(stateModelFactory.getDownwardStateTransitionCounter() > downwardBefore,
        "Downward transitions should occur when disabling");

    // Set evacuate
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, testInstanceName, InstanceConstants.InstanceOperation.EVACUATE);

    // Wait for evacuation
    boolean evacFinished = TestHelper.verify(() ->
        _admin.isEvacuateFinished(CLUSTER_NAME, testInstanceName),
    180000);
    Assert.assertTrue(evacFinished, "Evacuation should complete");

    // No upward transitions during evacuation of disabled node
    Assert.assertEquals(stateModelFactory.getUpwardStateTransitionCounter(), upwardBefore,
        "No upward transitions during evacuation of disabled node");

    // Re-enable partitions
    instanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, testInstanceName);
    instanceConfig.setInstanceEnabledForPartition(
        InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY, "", true);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, testInstanceName, instanceConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Still no upward transitions
    Assert.assertEquals(stateModelFactory.getUpwardStateTransitionCounter(), upwardBefore,
        "No upward transitions after re-enabling");

    // Cleanup
    testParticipant.syncStop();

    LOG.info("END testEvacuateWithDisabledPartition");
  }

  // ==================== Helper Methods ====================

  private void setupClusterConfig() {
    _stateModelDelay = 3L;
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(1800000L);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private void enableTopologyAwareRebalance() {
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
    LOG.info("removeOfflineOrInactiveInstances: Starting cleanup");

    for (int i = 0; i < _participants.size(); i++) {
      String participantName = _participantNames.get(i);
      InstanceConfig instanceConfig = _gSetupTool.getClusterManagementTool()
          .getInstanceConfig(CLUSTER_NAME, participantName);

      boolean shouldRemove = !_participants.get(i).isConnected()
          || !instanceConfig.getInstanceEnabled()
          || instanceConfig.getInstanceOperation().getOperation()
              .equals(InstanceConstants.InstanceOperation.SWAP_IN);

      if (shouldRemove) {
        LOG.info("Removing instance: {} (connected: {}, enabled: {}, op: {})",
            participantName, _participants.get(i).isConnected(),
            instanceConfig.getInstanceEnabled(),
            instanceConfig.getInstanceOperation().getOperation());

        if (_participants.get(i).isConnected()) {
          _participants.get(i).syncStop();
        }
        _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, instanceConfig);
        _participantNames.remove(i);
        _participants.remove(i);
        i--;
      }
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling(600000, 5000),
        "Cluster verification failed after removing offline instances");
    LOG.info("removeOfflineOrInactiveInstances: Completed. Remaining: {}",
        _participantNames);
  }

  private void createTestDBs(long delayTime) {
    createResourceWithDelayedRebalance(CLUSTER_NAME, "TEST_DB0_CRUSHED",
        BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA - 1, -1,
        CrushEdRebalanceStrategy.class.getName());
    _allDBs.add("TEST_DB0_CRUSHED");

    createResourceWithDelayedRebalance(CLUSTER_NAME, "TEST_DB1_CRUSHED",
        BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA - 1, 2000000,
        CrushEdRebalanceStrategy.class.getName());
    _allDBs.add("TEST_DB1_CRUSHED");

    createResourceWithWagedRebalance(CLUSTER_NAME, "TEST_DB2_WAGED",
        BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA - 1);
    _allDBs.add("TEST_DB2_WAGED");

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private MockParticipantManager createParticipant(String participantName,
      StateModelFactory stateModelFactory) throws Exception {
    MockParticipantManager participant = new MockParticipantManager(
        ZK_ADDR, CLUSTER_NAME, participantName, 10, null);
    StateMachineEngine stateMachine = participant.getStateMachineEngine();
    stateMachine.registerStateModelFactory("MasterSlave",
        stateModelFactory != null ? stateModelFactory : new StDelayMSStateModelFactory());
    return participant;
  }

  private void addParticipant(String participantName) throws Exception {
    addParticipant(participantName, UUID.randomUUID().toString(),
        "zone_" + _participants.size() % ZONE_COUNT, null, -1);
  }

  private void addParticipant(String participantName, StateModelFactory stateModelFactory)
      throws Exception {
    addParticipant(participantName, UUID.randomUUID().toString(),
        "zone_" + _participants.size() % ZONE_COUNT, null, -1, null, stateModelFactory);
  }

  private void addParticipant(String participantName, String logicalId, String zone,
      InstanceConstants.InstanceOperation instanceOperation, int capacity) throws Exception {
    addParticipant(participantName, logicalId, zone, instanceOperation, capacity, null, null);
  }

  private void addParticipant(String participantName, String logicalId, String zone,
      InstanceConstants.InstanceOperation instanceOperation, int capacity,
      InstanceConfigChangeListener listener, StateModelFactory stateModelFactory)
      throws Exception {
    InstanceConfig config = new InstanceConfig.Builder()
        .setDomain(String.format("%s=%s, %s=%s, %s=%s",
            ZONE, zone, HOST, participantName, LOGICAL_ID, logicalId))
        .setInstanceOperation(instanceOperation)
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

  private Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<>();
    for (String db : _allDBs) {
      ExternalView ev = _gSetupTool.getClusterManagementTool()
          .getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  private Map<String, IdealState> getISs() {
    Map<String, IdealState> idealStates = new HashMap<>();
    for (String db : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, db);
      idealStates.put(db, is);
    }
    return idealStates;
  }

  private Set<String> getParticipantsInEv(ExternalView ev) {
    Set<String> assignedParticipants = new HashSet<>();
    for (String partition : ev.getPartitionSet()) {
      ev.getStateMap(partition).entrySet().stream()
          .filter(e -> !e.getValue().equals("OFFLINE"))
          .forEach(e -> assignedParticipants.add(e.getKey()));
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
    Map<String, Map<String, String>> stateByResource = new HashMap<>();
    for (String resourceEV : evs.keySet()) {
      for (String partition : evs.get(resourceEV).getPartitionSet()) {
        if (evs.get(resourceEV).getStateMap(partition).containsKey(instanceName)) {
          stateByResource.computeIfAbsent(resourceEV, k -> new HashMap<>())
              .put(partition, evs.get(resourceEV).getStateMap(partition).get(instanceName));
        }
      }
    }
    return stateByResource;
  }

  private Set<String> getInstanceNames(Collection<InstanceConfig> instanceConfigs) {
    return instanceConfigs.stream().map(InstanceConfig::getInstanceName)
        .collect(Collectors.toSet());
  }

  private void validateRoutingTablesInstance(Map<String, ExternalView> evs,
      String instanceName, boolean shouldContain) throws Exception {
    validateRoutingTablesInstance(evs, instanceName, shouldContain, VERIFICATION_TIMEOUT);
  }

  private void validateRoutingTablesInstance(Map<String, ExternalView> evs,
      String instanceName, boolean shouldContain, long timeout) throws Exception {
    RoutingTableProvider[] routingTableProviders = {
        _routingTableProviderDefault, _routingTableProviderEV, _routingTableProviderCS};

    Map<String, Map<String, String>> resourcePartitionState =
        getResourcePartitionStateOnInstance(evs, instanceName);

    if (resourcePartitionState.isEmpty()) {
      Arrays.stream(routingTableProviders).forEach(rtp -> {
        Assert.assertEquals(getInstanceNames(rtp.getInstanceConfigs()).contains(instanceName),
            shouldContain);
      });
      return;
    }

    verifier(() -> {
      try {
        for (String resource : resourcePartitionState.keySet()) {
          Map<String, String> partitions = resourcePartitionState.get(resource);
          for (String partition : partitions.keySet()) {
            String state = partitions.get(partition);
            for (RoutingTableProvider rtp : routingTableProviders) {
              boolean contains = getInstanceNames(
                  rtp.getInstancesForResource(resource, partition, state))
                  .contains(instanceName);
              if (contains != shouldContain) {
                return false;
              }
            }
          }
        }
        for (RoutingTableProvider rtp : routingTableProviders) {
          boolean contains = getInstanceNames(rtp.getInstanceConfigs()).contains(instanceName);
          if (contains != shouldContain) {
            return false;
          }
        }
        return true;
      } catch (Exception e) {
        return false;
      }
    }, timeout);
  }

  private void validateEVCorrect(ExternalView actual, ExternalView original,
      Map<String, String> swapOutToSwapIn, Set<String> inFlightSwapIn,
      Set<String> completedSwapIn, Set<String> allDisabledSwapIn) {
    Assert.assertEquals(actual.getPartitionSet(), original.getPartitionSet(),
        "Partition set should match");

    IdealState is = _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, original.getResourceName());
    StateModelDefinition stateModelDef = _gSetupTool.getClusterManagementTool()
        .getStateModelDef(CLUSTER_NAME, is.getStateModelDefRef());

    for (String partition : actual.getPartitionSet()) {
      Map<String, String> expectedStateMap = new HashMap<>(original.getStateMap(partition));

      for (String swapOutInstance : swapOutToSwapIn.keySet()) {
        String swapInInstance = swapOutToSwapIn.get(swapOutInstance);

        if (expectedStateMap.containsKey(swapOutInstance) && inFlightSwapIn.contains(swapInInstance)) {
          // Swap in progress - swap-in gets same or second state
          String expectedState = expectedStateMap.get(swapOutInstance)
              .equals(stateModelDef.getTopState())
              ? stateModelDef.getSecondTopStates().iterator().next()
              : expectedStateMap.get(swapOutInstance);

          if (allDisabledSwapIn.contains(swapInInstance)) {
            expectedState = stateModelDef.getInitialState();
          }
          expectedStateMap.put(swapInInstance, expectedState);

        } else if (expectedStateMap.containsKey(swapOutInstance)
            && completedSwapIn.contains(swapInInstance)) {
          // Swap completed - swap-in takes swap-out's state
          expectedStateMap.put(swapInInstance, expectedStateMap.get(swapOutInstance));
          expectedStateMap.remove(swapOutInstance);
        }
      }

      Assert.assertEquals(actual.getStateMap(partition), expectedStateMap,
          String.format("State map mismatch for partition %s in resource %s",
              partition, actual.getResourceName()));
    }
  }

  private boolean validateEVsCorrect(Map<String, ExternalView> actuals,
      Map<String, ExternalView> originals, Map<String, String> swapOutToSwapIn,
      Set<String> inFlightSwapIn, Set<String> completedSwapIn) {
    Assert.assertEquals(actuals.keySet(), originals.keySet(),
        "Resource key sets should match");

    for (String resource : actuals.keySet()) {
      try {
        validateEVCorrect(actuals.get(resource), originals.get(resource),
            swapOutToSwapIn, inFlightSwapIn, completedSwapIn, Collections.emptySet());
      } catch (AssertionError e) {
        ExternalView original = originals.get(resource);
        ExternalView actual = actuals.get(resource);
        LOG.error("EV validation failed for resource: {}", resource);
        LOG.error("Original partitions: {}", original.getPartitionSet());
        for (String partition : original.getPartitionSet()) {
          LOG.error("Original {} state: {}", partition, original.getStateMap(partition));
        }
        for (String partition : actual.getPartitionSet()) {
          LOG.error("Actual {} state: {}", partition, actual.getStateMap(partition));
        }
        LOG.error("Swap mapping: {}", swapOutToSwapIn);
        LOG.error("Completed swap instances: {}", completedSwapIn);
        throw e;
      }
    }
    return true;
  }

  private void validateAssignmentInEv(ExternalView ev) {
    validateAssignmentInEv(ev, REPLICA);
  }

  private void validateAssignmentInEv(ExternalView ev, int expectedActiveReplicas) {
    for (String partition : ev.getPartitionSet()) {
      AtomicInteger activeCount = new AtomicInteger();
      ev.getStateMap(partition).values().stream()
          .filter(ACCEPTABLE_STATE_SET::contains)
          .forEach(v -> activeCount.getAndIncrement());
      Assert.assertTrue(activeCount.get() >= expectedActiveReplicas,
          String.format("Partition %s should have >= %d active replicas, has %d",
              partition, expectedActiveReplicas, activeCount.get()));
    }
  }

  private void setUpWagedBaseline() {
    _assignmentMetadataStore = new AssignmentMetadataStore(
        new ZkBucketDataAccessor(ZK_ADDR), CLUSTER_NAME) {
      @Override
      public Map<String, ResourceAssignment> getBaseline() {
        super.reset();
        return super.getBaseline();
      }

      @Override
      public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
        super.reset();
        return super.getBestPossibleAssignment();
      }
    };

    ClusterConfig clusterConfig = _dataAccessor.getProperty(
        _dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(TEST_CAPACITY_KEY));
    clusterConfig.setDefaultInstanceCapacityMap(
        Collections.singletonMap(TEST_CAPACITY_KEY, TEST_CAPACITY_VALUE));
    clusterConfig.setDefaultPartitionWeightMap(
        Collections.singletonMap(TEST_CAPACITY_KEY, 1));
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().clusterConfig(), clusterConfig);
  }

  /**
   * Verifies that the given verifier returns true within the given timeout.
   */
  private static void verifier(TestHelper.Verifier verifier, long timeout) throws Exception {
    verifier(verifier, timeout, null);
  }

  /**
   * Verifies that the given verifier returns true within the given timeout.
   * Optionally checks controller availability for debugging.
   */
  private static void verifier(TestHelper.Verifier verifier, long timeout, String clusterName)
      throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      try {
        if (clusterName != null) {
          try {
            Object leader = org.apache.helix.controller.GenericHelixController
                .getLeaderController(clusterName);
            if (leader == null) {
              LOG.error("Controller not available for cluster {}", clusterName);
            }
          } catch (Exception e) {
            LOG.warn("Failed to check controller for {}: {}", clusterName, e.getMessage());
          }
        }
        return verifier.verify();
      } catch (AssertionError e) {
        LOG.error("Verifier assertion failed: {}", e.getMessage());
        return false;
      }
    }, timeout));
  }

  // ==================== Inner Classes ====================

  private static class CustomIndividualInstanceConfigChangeListener
      implements InstanceConfigChangeListener {
    private boolean _throttlesEnabled = true;

    public boolean isThrottlesEnabled() {
      return _throttlesEnabled;
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> configs,
        NotificationContext context) {
      if (!configs.isEmpty() &&
          configs.get(0).getInstanceOperation().getOperation()
              .equals(InstanceConstants.InstanceOperation.SWAP_IN)) {
        _throttlesEnabled = false;
      } else {
        _throttlesEnabled = true;
      }
    }
  }

  /**
   * State model with configurable delay for state transitions.
   * Use positive _stateModelDelay for slow downward transitions.
   * Use negative _stateModelDelay for slow upward transitions.
   */
  public class StDelayMSStateModelFactory extends StateModelFactory<StDelayMSStateModel> {
    @Override
    public StDelayMSStateModel createNewStateModel(String resourceName, String partitionKey) {
      return new StDelayMSStateModel();
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {"MASTER", "SLAVE", "ERROR"})
  public class StDelayMSStateModel extends StateModel {
    private boolean _cancelled = false;

    public StDelayMSStateModel() {
      _cancelled = false;
    }

    private void sleepWhileNotCanceled(long sleepTime) throws InterruptedException {
      while (sleepTime > 0 && !isCancelled()) {
        Thread.sleep(VERIFICATION_TIMEOUT);
        sleepTime -= VERIFICATION_TIMEOUT;
      }
      if (isCancelled()) {
        _cancelled = false;
        // Don't throw HelixRollbackException - just return normally.
        // Helix will handle the cancellation and won't retry this message.
        // Throwing causes infinite retry loops when isCancelled() stays true.
        return;
      }
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay < 0) {
        sleepWhileNotCanceled(Math.abs(_stateModelDelay));
      }
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay < 0) {
        sleepWhileNotCanceled(Math.abs(_stateModelDelay));
      }
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay > 0) {
        sleepWhileNotCanceled(_stateModelDelay);
      }
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay > 0) {
        sleepWhileNotCanceled(_stateModelDelay);
      }
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay > 0) {
        sleepWhileNotCanceled(_stateModelDelay);
      }
    }
  }

  /**
   * State model factory that counts state transitions for verification.
   */
  public class StateTransitionCountStateModelFactory
      extends StateModelFactory<StateTransitionCountStateModel> {
    private final AtomicInteger _upwardCounter = new AtomicInteger(0);
    private final AtomicInteger _downwardCounter = new AtomicInteger(0);

    @Override
    public StateTransitionCountStateModel createNewStateModel(String resourceName,
        String partitionKey) {
      return new StateTransitionCountStateModel(_upwardCounter, _downwardCounter);
    }

    public int getUpwardStateTransitionCounter() {
      return _upwardCounter.get();
    }

    public int getDownwardStateTransitionCounter() {
      return _downwardCounter.get();
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {"MASTER", "SLAVE", "ERROR"})
  public class StateTransitionCountStateModel extends StateModel {
    private final AtomicInteger _upwardCounter;
    private final AtomicInteger _downwardCounter;

    public StateTransitionCountStateModel(AtomicInteger upwardCounter,
        AtomicInteger downwardCounter) {
      _upwardCounter = upwardCounter;
      _downwardCounter = downwardCounter;
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      _upwardCounter.incrementAndGet();
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      _upwardCounter.incrementAndGet();
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      _downwardCounter.incrementAndGet();
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      _downwardCounter.incrementAndGet();
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      _downwardCounter.incrementAndGet();
    }
  }
}
