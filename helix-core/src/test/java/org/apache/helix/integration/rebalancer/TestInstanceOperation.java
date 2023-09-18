package org.apache.helix.integration.rebalancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixRollbackException;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestInstanceOperation extends ZkTestBase {
  protected final int NUM_NODE = 6;
  protected static final int START_PORT = 12918;
  protected static final int PARTITIONS = 20;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private int REPLICA = 3;
  protected ClusterControllerManager _controller;
  List<MockParticipantManager> _participants = new ArrayList<>();
  List<String> _participantNames = new ArrayList<>();
  private Set<String> _allDBs = new HashSet<>();
  private ZkHelixClusterVerifier _clusterVerifier;
  private ConfigAccessor _configAccessor;
  private long _stateModelDelay = 3L;

  private HelixAdmin _admin;
  protected AssignmentMetadataStore _assignmentMetadataStore;
  HelixDataAccessor _dataAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String participantName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
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
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    _configAccessor = new ConfigAccessor(_gZkClient);
    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    createTestDBs(200);

    setUpWagedBaseline();

    _admin = new ZKHelixAdmin(_gZkClient);
  }

  @Test
  public void testEvacuate() throws Exception {
    // EV should contain all participants, check resources one by one
    Map<String, ExternalView> assignment = getEV();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
    }

    // evacuated instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    System.out.println("123");
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // New ev should contain all instances but the evacuated one
    assignment = getEV();
    List<String> currentActiveInstances =
        _participantNames.stream().filter(n -> !n.equals(instanceToEvacuate)).collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
      Set<String> newPAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newPAssignedParticipants.contains(instanceToEvacuate));
      Assert.assertTrue(newPAssignedParticipants.containsAll(currentActiveInstances));
    }

    Assert.assertTrue(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));
    Assert.assertTrue(_admin.isPrepopulateReady(CLUSTER_NAME, instanceToEvacuate));
  }

  @Test(dependsOnMethods = "testEvacuate")
  public void testRevertEvacuation() throws Exception {

    // revert an evacuate instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // EV should contain all participants, check resources one by one
    Map<String, ExternalView> assignment = getEV();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test(dependsOnMethods = "testRevertEvacuation")
  public void testAddingNodeWithEvacuationTag() throws Exception {
    // first disable and instance, and wait for all replicas to be moved out
    String mockNewInstance = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, mockNewInstance, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    //ev should contain all instances but the disabled one
    Map<String, ExternalView> assignment = getEV();
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
    assignment = getEV();
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
     assignment = getEV();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test(dependsOnMethods = "testAddingNodeWithEvacuationTag")
  public void testEvacuateAndCancelBeforeBootstrapFinish() throws Exception {
    // add a resource where downward state transition is slow
    createResourceWithDelayedRebalance(CLUSTER_NAME, "TEST_DB3_DELAYED_CRUSHED", "MasterSlave", PARTITIONS, REPLICA,
        REPLICA - 1, 200, CrushEdRebalanceStrategy.class.getName());
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
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);
    // Messages should be pending at all instances besides the evacuate one
    for (String participant : _participantNames) {
      if (participant.equals(instanceToEvacuate)) {
        continue;
      }
      TestHelper.verify(
          () -> ((_dataAccessor.getChildNames(_dataAccessor.keyBuilder().messages(participant))).isEmpty()), 30000);
    }
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));

    // sleep a bit so ST messages can start executing
    Thread.sleep(Math.abs(_stateModelDelay / 100));
    // before we cancel, check current EV
    Map<String, ExternalView> assignment = getEV();
    for (String resource : _allDBs) {
      // check every replica has >= 3 partitions and a top state partition
      validateAssignmentInEv(assignment.get(resource));
    }

    // cancel the evacuation
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, null);

    assignment = getEV();
    for (String resource : _allDBs) {
      // check every replica has >= 3 active replicas, even before cluster converge
      validateAssignmentInEv(assignment.get(resource));
    }

    // check cluster converge. We have longer delay for ST then verifier timeout. It will only converge if we cancel ST.
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // EV should contain all participants, check resources one by one
    assignment = getEV();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      // check every replica has >= 3 active replicas again
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test(dependsOnMethods = "testEvacuateAndCancelBeforeBootstrapFinish")
  public void testEvacuateAndCancelBeforeDropFinish() throws Exception {

    // set DROP ST delay to a large number
    _stateModelDelay = 10000L;

    // evacuate an instance
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // message should be pending at the to evacuate participant
    TestHelper.verify(
        () -> ((_dataAccessor.getChildNames(_dataAccessor.keyBuilder().messages(instanceToEvacuate))).isEmpty()), 30000);
    Assert.assertFalse(_admin.isEvacuateFinished(CLUSTER_NAME, instanceToEvacuate));

    // cancel evacuation
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, null);
    // check every replica has >= 3 active replicas, even before cluster converge
    Map<String, ExternalView> assignment = getEV();
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // EV should contain all participants, check resources one by one
    assignment = getEV();
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).containsAll(_participantNames));
      // check every replica has >= 3 active replicas
      validateAssignmentInEv(assignment.get(resource));
    }
  }

  @Test(dependsOnMethods = "testEvacuateAndCancelBeforeDropFinish")
  public void testMarkEvacuationAfterEMM() throws Exception {
    _stateModelDelay = 1000L;
    Assert.assertFalse(_gSetupTool.getClusterManagementTool().isInMaintenanceMode(CLUSTER_NAME));
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null,
        null);
    addParticipant(PARTICIPANT_PREFIX + "_" + (START_PORT + NUM_NODE));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Map<String, ExternalView> assignment = getEV();
    for (String resource : _allDBs) {
      Assert.assertFalse(getParticipantsInEv(assignment.get(resource)).contains(_participantNames.get(NUM_NODE)));
    }

    // set evacuate operation
    String instanceToEvacuate = _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, instanceToEvacuate, InstanceConstants.InstanceOperation.EVACUATE);

    // there should be no evacuation happening
    for (String resource : _allDBs) {
      Assert.assertTrue(getParticipantsInEv(assignment.get(resource)).contains(instanceToEvacuate));
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // exit MM
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    assignment = getEV();
    List<String> currentActiveInstances =
        _participantNames.stream().filter(n -> !n.equals(instanceToEvacuate)).collect(Collectors.toList());
    for (String resource : _allDBs) {
      validateAssignmentInEv(assignment.get(resource));
      Set<String> newPAssignedParticipants = getParticipantsInEv(assignment.get(resource));
      Assert.assertFalse(newPAssignedParticipants.contains(instanceToEvacuate));
      Assert.assertTrue(newPAssignedParticipants.containsAll(currentActiveInstances));
    }
    Assert.assertTrue(_admin.isPrepopulateReady(CLUSTER_NAME, instanceToEvacuate));
  }

  @Test(dependsOnMethods = "testMarkEvacuationAfterEMM")
  public void testEvacuationWithOfflineInstancesInCluster() throws Exception {
    _participants.get(2).syncStop();
    _participants.get(3).syncStop();
    // wait for converge, and set evacuate on instance 0
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String evacuateInstanceName =  _participants.get(0).getInstanceName();
    _gSetupTool.getClusterManagementTool()
        .setInstanceOperation(CLUSTER_NAME, evacuateInstanceName, InstanceConstants.InstanceOperation.EVACUATE);

    Map<String, IdealState> assignment;
    List<String> currentActiveInstances =
        _participantNames.stream().filter(n -> (!n.equals(evacuateInstanceName) && !n.equals(_participants.get(3).getInstanceName()))).collect(Collectors.toList());
    TestHelper.verify( ()-> {return verifyIS(evacuateInstanceName);}, TestHelper.WAIT_DURATION);

    _participants.get(3).syncStart();
    _participants.get(2).syncStart();
  }


  private void addParticipant(String participantName) {
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, participantName);

    // start dummy participants
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, participantName);
    StateMachineEngine stateMachine = participant.getStateMachineEngine();
    // Using a delayed state model
    StDelayMSStateModelFactory delayFactory = new StDelayMSStateModelFactory();
    stateMachine.registerStateModelFactory("MasterSlave", delayFactory);

    participant.syncStart();
    _participants.add(participant);
    _participantNames.add(participantName);
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

  private Map<String, ExternalView> getEV() {
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

  // verify that each partition has >=REPLICA (3 in this case) replicas

  private void validateAssignmentInEv(ExternalView ev) {
    validateAssignmentInEv(ev, REPLICA);
  }

  private void validateAssignmentInEv(ExternalView ev, int expectedNumber) {
    Set<String> partitionSet = ev.getPartitionSet();
    for (String partition : partitionSet) {
      AtomicInteger activeReplicaCount = new AtomicInteger();
      ev.getStateMap(partition)
          .values()
          .stream()
          .filter(v -> v.equals("MASTER") || v.equals("LEADER") || v.equals("SLAVE") || v.equals("FOLLOWER") || v.equals("STANDBY"))
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
    String testCapacityKey = "TestCapacityKey";
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(testCapacityKey));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(testCapacityKey, 100));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(testCapacityKey, 1));
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
        Thread.sleep(5000);
        sleepTime = sleepTime - 5000;
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
