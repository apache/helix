package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPrioritizationMessageGeneration extends MessageGenerationPhase {

  private static final String TEST_CLUSTER = "TestCluster";
  private static final String TEST_RESOURCE = "TestDB";
  private static final String PARTITION_0 = "TestDB_0";
  private static final String INSTANCE_0 = "localhost_0";
  private static final String INSTANCE_1 = "localhost_1";
  private static final String INSTANCE_2 = "localhost_2";
  private static final String INSTANCE_3 = "localhost_3";
  private static final String SESSION_ID = "123";

  @Test
  public void testCurrentReplicaCountForUpwardTransitions() throws Exception {

    // Test prioritization for upward state transitions from non-second top states
    // to second top or top states
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Set up current states
    Map<String, CurrentState> currentStateMap = setupCurrentStatesForPrioritizationCase1();
    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);

    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants to move some nodes to SLAVE
    Map<String, String> partitionMap = new HashMap<>();
    // No ST messages should be generated as current state is same as desired state
    partitionMap.put(INSTANCE_0, "MASTER"); // Master -> Master
    partitionMap.put(INSTANCE_1, "SLAVE"); // Slave -> Slave

    // upward to second top
    partitionMap.put(INSTANCE_2, "SLAVE"); // Offline -> Slave
    partitionMap.put(INSTANCE_3, "SLAVE"); // Offline -> Slave

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    // Should generate 2 messages
    Assert.assertEquals(messages.size(), 2);

    // Both messages should have the same currentActiveReplicaNumber = current active replica count
    // Current active replicas: 1 MASTER + 1 SLAVE = 2
    for (Message msg : messages) {
      Assert.assertEquals(msg.getCurrentActiveReplicaNumber(), 2,
          "All upward transitions should have currentActiveReplicaNumber = current active replica count (2)");
      Assert.assertTrue(msg.getTgtName().equals(INSTANCE_2) || msg.getTgtName().equals(INSTANCE_3),
          "Messages should be for instances transitioning from OFFLINE to SLAVE");
    }
  }

  @Test
  public void testZeroReplicaScenario() throws Exception {
    // Test scenario with 0 current active replicas (all OFFLINE)
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Current state: All instances in OFFLINE state
    Map<String, CurrentState> currentStateMap = setupCurrentStatesForPrioritizationCase2();
    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);

    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants to create 1 MASTER, 2 SLAVE
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "SLAVE"); // Offline -> Slave (upward to second top)
    partitionMap.put(INSTANCE_1, "MASTER"); // Offline -> Master (upward to top)
    partitionMap.put(INSTANCE_2, "SLAVE"); // Offline -> Slave (upward to second top)

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    // Should generate 3 messages
    Assert.assertEquals(messages.size(), 3);

    // All messages should have currentActiveReplicaNumber = 0 (current active replica count)
    for (Message msg : messages) {
      Assert.assertEquals(msg.getCurrentActiveReplicaNumber(), 0,
          "All upward transitions should have currentActiveReplicaNumber = 0");
    }
  }

  @Test
  public void testNoReplicaNumberForNonUpwardTransitions() throws Exception {
    // Test that non-upward transitions don't get currentActiveReplicaNumber assigned
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Current state: Instance in SLAVE state
    Map<String, CurrentState> currentStateMap = new HashMap<>();
    CurrentState currentState = new CurrentState(TEST_RESOURCE);
    currentState.setState(PARTITION_0, "SLAVE");
    currentState.setSessionId(SESSION_ID);
    currentState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_0, currentState);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);
    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants to move to OFFLINE (downward transition)
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "OFFLINE");

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    Assert.assertEquals(messages.size(), 1);

    // Should have default currentActiveReplicaNumber (-1) for non-upward transition
    Message msg = messages.get(0);
    Assert.assertEquals(msg.getCurrentActiveReplicaNumber(), -1,
        "Non-upward state transitions should not have currentActiveReplicaNumber assigned");
  }

  @Test
  public void testNoReplicaNumberForSecondTopToTopTransitions() throws Exception {
    // Tests No currentActiveReplicaNumber assigned for Second Top -> Top State Transitions for Single-Top-State
    // State Model
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Current state: Instance in SLAVE state (second top)
    Map<String, CurrentState> currentStateMap = new HashMap<>();
    CurrentState currentState = new CurrentState(TEST_RESOURCE);
    currentState.setState(PARTITION_0, "SLAVE");
    currentState.setSessionId(SESSION_ID);
    currentState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_0, currentState);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);
    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants to promote to MASTER (second top -> top transition)
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "MASTER");

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    Assert.assertEquals(messages.size(), 1);

    // Should have default currentActiveReplicaNumber (-1) as it's from second top to top state
    Message msg = messages.get(0);
    Assert.assertEquals(msg.getCurrentActiveReplicaNumber(), -1,
        "Second top to top state transitions should not have currentActiveReplicaNumber assigned");
  }

  @Test
  public void testPendingMessagesDoNotAffectCurrentReplicaCount() throws Exception {
    // Test that pending messages don't affect the current active replica count calculation
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Current state: 1 MASTER, 1 OFFLINE
    Map<String, CurrentState> currentStateMap = new HashMap<>();

    CurrentState masterState = new CurrentState(TEST_RESOURCE);
    masterState.setState(PARTITION_0, "MASTER");
    masterState.setSessionId(SESSION_ID);
    masterState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_0, masterState);

    CurrentState offlineState1 = new CurrentState(TEST_RESOURCE);
    offlineState1.setState(PARTITION_0, "OFFLINE");
    offlineState1.setSessionId(SESSION_ID);
    offlineState1.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_1, offlineState1);

    CurrentState offlineState2 = new CurrentState(TEST_RESOURCE);
    offlineState2.setState(PARTITION_0, "OFFLINE");
    offlineState2.setSessionId(SESSION_ID);
    offlineState2.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_2, offlineState2);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);

    // Add pending message for INSTANCE_1: OFFLINE->SLAVE
    Message pendingMsg = createMessage("OFFLINE", "SLAVE", INSTANCE_1);
    pendingMsg.setMsgId(UUID.randomUUID().toString());
    currentStateOutput.setPendingMessage(TEST_RESOURCE, new Partition(PARTITION_0), INSTANCE_1,
        pendingMsg);

    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants both offline instances to become SLAVE
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "MASTER"); // No change
    partitionMap.put(INSTANCE_1, "SLAVE"); // OFFLINE -> SLAVE (but already has pending message)
    partitionMap.put(INSTANCE_2, "SLAVE"); // OFFLINE -> SLAVE (new transition)

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    // Should only generate message for INSTANCE_2 (INSTANCE_1 already has pending message)
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getTgtName(), INSTANCE_2);

    // The new message should have currentActiveReplicaNumber = current active replica count = 1 (only
    // MASTER currently active)
    // Note: pending messages should NOT affect the current active replica count calculation
    Assert.assertEquals(messages.get(0).getCurrentActiveReplicaNumber(), 1,
        "currentActiveReplicaNumber should be based on current active replicas only, not including pending transitions");
  }

  @Test
  public void testMultiTopStateModel() throws Exception {
    // Test multi-top state model (e.g., OFFLINE->ONLINE where ONLINE is the only top state, this
    // example does not include ERROR states.)
    StateModelDefinition stateModelDefinition =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline());

    // Current state: 1 ONLINE, 2 OFFLINE
    Map<String, CurrentState> currentStateMap = new HashMap<>();

    CurrentState onlineState = new CurrentState(TEST_RESOURCE);
    onlineState.setState(PARTITION_0, "ONLINE");
    onlineState.setSessionId(SESSION_ID);
    onlineState.setStateModelDefRef("OfflineOnline");
    currentStateMap.put(INSTANCE_0, onlineState);

    CurrentState offlineState1 = new CurrentState(TEST_RESOURCE);
    offlineState1.setState(PARTITION_0, "OFFLINE");
    offlineState1.setSessionId(SESSION_ID);
    offlineState1.setStateModelDefRef("OfflineOnline");
    currentStateMap.put(INSTANCE_1, offlineState1);

    CurrentState offlineState2 = new CurrentState(TEST_RESOURCE);
    offlineState2.setState(PARTITION_0, "OFFLINE");
    offlineState2.setSessionId(SESSION_ID);
    offlineState2.setStateModelDefRef("OfflineOnline");
    currentStateMap.put(INSTANCE_2, offlineState2);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);
    ClusterEvent event = prepareClusterEventForMultiTop(stateModelDefinition, currentStateOutput);

    // Best possible state wants to add 2 more ONLINE replica
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "ONLINE"); // ONLINE -> ONLINE (no change)
    partitionMap.put(INSTANCE_1, "ONLINE"); // OFFLINE -> ONLINE (upward transition)
    partitionMap.put(INSTANCE_2, "ONLINE"); // OFFLINE -> ONLINE (upward transition)

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    // Should generate 2 message for OFFLINE->ONLINE transition
    Assert.assertEquals(messages.size(), 2);
    Assert.assertEquals(messages.get(0).getTgtName(), INSTANCE_2);
    Assert.assertEquals(messages.get(1).getTgtName(), INSTANCE_1);

    // For multi-top state model, only top states (ONLINE) and ERROR state count as active
    // Current active replicas: 1 ONLINE = 1
    Assert.assertEquals(messages.get(0).getCurrentActiveReplicaNumber(), 1,
        "For multi-top state model, currentActiveReplicaNumber should count only top states");
    Assert.assertEquals(messages.get(1).getCurrentActiveReplicaNumber(), 1,
        "For multi-top state model, currentActiveReplicaNumber should count only top states");
  }

  @Test
  public void testErrorStateIncludedInActiveCount() throws Exception {
    // Test that ERROR state replicas are included in active replica count
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Current state: 1 MASTER, 1 ERROR, 1 OFFLINE
    Map<String, CurrentState> currentStateMap = new HashMap<>();

    CurrentState masterState = new CurrentState(TEST_RESOURCE);
    masterState.setState(PARTITION_0, "MASTER");
    masterState.setSessionId(SESSION_ID);
    masterState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_0, masterState);

    CurrentState errorState = new CurrentState(TEST_RESOURCE);
    errorState.setState(PARTITION_0, "ERROR");
    errorState.setSessionId(SESSION_ID);
    errorState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_1, errorState);

    CurrentState offlineState = new CurrentState(TEST_RESOURCE);
    offlineState.setState(PARTITION_0, "OFFLINE");
    offlineState.setSessionId(SESSION_ID);
    offlineState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_2, offlineState);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);
    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants OFFLINE to become SLAVE
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "MASTER"); // MASTER -> MASTER (no change)
    partitionMap.put(INSTANCE_1, "ERROR"); // ERROR -> ERROR (no change)
    partitionMap.put(INSTANCE_2, "SLAVE"); // OFFLINE -> SLAVE (upward transition)

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    // Should generate 1 message for OFFLINE->SLAVE transition
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getTgtName(), INSTANCE_2);

    // Current active replicas should include ERROR state: 1 MASTER + 1 ERROR = 2
    Assert.assertEquals(messages.get(0).getCurrentActiveReplicaNumber(), 2,
        "currentActiveReplicaNumber should include ERROR state replicas in active count");
  }

  @Test
  public void testTransitionFromErrorToOffline() throws Exception {
    // Test ERROR→OFFLINE transition (standard recovery path) - should NOT get currentReplicaNumber
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Current state: Instance in ERROR state
    Map<String, CurrentState> currentStateMap = new HashMap<>();
    CurrentState currentState = new CurrentState(TEST_RESOURCE);
    currentState.setState(PARTITION_0, "ERROR");
    currentState.setSessionId(SESSION_ID);
    currentState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_0, currentState);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);
    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants to move ERROR to OFFLINE (standard recovery pattern)
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "OFFLINE");

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    Assert.assertEquals(messages.size(), 1);

    // Should have default replica number (-1) since it's a downward transition (active to inactive)
    Message msg = messages.get(0);
    Assert.assertEquals(msg.getCurrentActiveReplicaNumber(), -1,
        "ERROR→OFFLINE transitions should not have currentActiveReplicaNumber assigned since it's downward (active to inactive)");
  }

  @Test
  public void testDroppedReplicasExcludedFromActiveCount() throws Exception {
    // Test that DROPPED replicas are properly excluded from calculations
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Current state: 1 MASTER, 1 SLAVE, 1 OFFLINE
    Map<String, CurrentState> currentStateMap = new HashMap<>();

    CurrentState masterState = new CurrentState(TEST_RESOURCE);
    masterState.setState(PARTITION_0, "MASTER");
    masterState.setSessionId(SESSION_ID);
    masterState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_0, masterState);

    CurrentState slaveState = new CurrentState(TEST_RESOURCE);
    slaveState.setState(PARTITION_0, "OFFLINE");
    slaveState.setSessionId(SESSION_ID);
    slaveState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_1, slaveState);

    CurrentState offlineState = new CurrentState(TEST_RESOURCE);
    offlineState.setState(PARTITION_0, "OFFLINE");
    offlineState.setSessionId(SESSION_ID);
    offlineState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_2, offlineState);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);
    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state: MASTER stays, SLAVE gets DROPPED, OFFLINE becomes SLAVE
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "MASTER"); // MASTER -> MASTER (no change)
    partitionMap.put(INSTANCE_1, "DROPPED"); // OFFLINE -> DROPPED (downward transition)
    partitionMap.put(INSTANCE_2, "SLAVE"); // OFFLINE -> SLAVE (upward transition)

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    // Should generate 2 messages: OFFLINE->DROPPED and OFFLINE->SLAVE
    Assert.assertEquals(messages.size(), 2);

    Message upwardTransitionMsg = null;
    Message droppedTransitionMsg = null;
    for (Message msg : messages) {
      if (msg.getTgtName().equals(INSTANCE_2) && msg.getToState().equals("SLAVE")) {
        upwardTransitionMsg = msg;
      } else if (msg.getTgtName().equals(INSTANCE_1) && msg.getToState().equals("DROPPED")) {
        droppedTransitionMsg = msg;
      }
    }

    Assert.assertNotNull(upwardTransitionMsg, "Should have upward state transition message");
    Assert.assertNotNull(droppedTransitionMsg, "Should have dropped state transition message");

    // Upward transition should get currentActiveReplicaNumber = current active replicas
    // Current active replicas: 1 (MASTER = 1)
    Assert.assertEquals(upwardTransitionMsg.getCurrentActiveReplicaNumber(), 1,
        "Upward transition should have currentActiveReplicaNumber = current active replica count");

    // DROPPED transition should not get currentActiveReplicaNumber (downward transition)
    Assert.assertEquals(droppedTransitionMsg.getCurrentActiveReplicaNumber(), -1,
        "DROPPED transition should not have currentActiveReplicaNumber assigned");
  }

  private ClusterEvent prepareClusterEventForMultiTop(StateModelDefinition stateModelDefinition,
      CurrentStateOutput currentStateOutput) {
    ClusterEvent event = new ClusterEvent(TEST_CLUSTER, ClusterEventType.Unknown);

    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);

    // Mock HelixManager
    HelixManager manager = mock(HelixManager.class);
    when(manager.getInstanceName()).thenReturn("Controller");
    when(manager.getSessionId()).thenReturn(SESSION_ID);

    // Mock HelixDataAccessor
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(manager.getHelixDataAccessor()).thenReturn(dataAccessor);

    event.addAttribute(AttributeName.helixmanager.name(), manager);

    // Setup ResourceControllerDataProvider
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getClusterConfig()).thenReturn(new ClusterConfig("TestCluster"));
    when(cache.getStateModelDef("OfflineOnline")).thenReturn(stateModelDefinition);

    // Mock live instances
    Map<String, LiveInstance> liveInstances = new HashMap<>();
    liveInstances.put(INSTANCE_0, createLiveInstance(INSTANCE_0));
    liveInstances.put(INSTANCE_1, createLiveInstance(INSTANCE_1));
    liveInstances.put(INSTANCE_2, createLiveInstance(INSTANCE_2));
    when(cache.getLiveInstances()).thenReturn(liveInstances);

    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Setup resources
    Map<String, Resource> resourceMap = new HashMap<>();
    Resource resource = new Resource(TEST_RESOURCE);
    resource.setStateModelDefRef("OfflineOnline");
    resource.addPartition(PARTITION_0);
    resourceMap.put(TEST_RESOURCE, resource);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    return event;
  }

  @Test
  public void testReplicaCountCalculationWithDroppedReplicas() throws Exception {
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Set up current states
    Map<String, CurrentState> currentStateMap = new HashMap<>();

    // Instance 0: MASTER -> will stay MASTER
    CurrentState currentState0 = new CurrentState(TEST_RESOURCE);
    currentState0.setState(PARTITION_0, "MASTER");
    currentState0.setSessionId(SESSION_ID);
    currentState0.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_0, currentState0);

    // Instance 1: SLAVE -> will be DROPPED
    CurrentState currentState1 = new CurrentState(TEST_RESOURCE);
    currentState1.setState(PARTITION_0, "SLAVE");
    currentState1.setSessionId(SESSION_ID);
    currentState1.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_1, currentState1);

    // Instance 2: OFFLINE -> will become SLAVE
    CurrentState currentState2 = new CurrentState(TEST_RESOURCE);
    currentState2.setState(PARTITION_0, "OFFLINE");
    currentState2.setSessionId(SESSION_ID);
    currentState2.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_2, currentState2);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);
    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state: MASTER stays, one SLAVE gets DROPPED, one OFFLINE becomes SLAVE
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "MASTER"); // No change
    partitionMap.put(INSTANCE_1, "DROPPED"); // SLAVE -> DROPPED (excluded from active count)
    partitionMap.put(INSTANCE_2, "SLAVE"); // OFFLINE -> SLAVE (upward transition)

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    // Should generate 2 messages: SLAVE->DROPPED and OFFLINE->SLAVE
    Assert.assertEquals(messages.size(), 2);

    Message upwardTransitionMsg = null;
    Message droppedTransitionMsg = null;
    for (Message msg : messages) {
      if (msg.getTgtName().equals(INSTANCE_2)) {
        upwardTransitionMsg = msg;
      } else if (msg.getTgtName().equals(INSTANCE_1)) {
        droppedTransitionMsg = msg;
      }
    }

    Assert.assertNotNull(upwardTransitionMsg, "Should have upward state transition message");
    Assert.assertNotNull(droppedTransitionMsg, "Should have dropped state transition message");

    // Upward transition should get currentActiveReplicaNumber
    Assert.assertEquals(upwardTransitionMsg.getCurrentActiveReplicaNumber(), 2,
        "Upward state transition message should have current active replica number assigned based on active replica count excluding DROPPED");

    // DROPPED transition should not get current active replica number (it's a downward transition anyway)
    Assert.assertEquals(droppedTransitionMsg.getCurrentActiveReplicaNumber(), -1,
        "DROPPED state transition message should not have current active replica number assigned.");
  }

  private ClusterEvent prepareClusterEvent(StateModelDefinition stateModelDef,
      CurrentStateOutput currentStateOutput) {
    ClusterEvent event = new ClusterEvent(TEST_CLUSTER, ClusterEventType.Unknown);

    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);

    // Mock HelixManager
    HelixManager manager = mock(HelixManager.class);
    when(manager.getInstanceName()).thenReturn("Controller");
    when(manager.getSessionId()).thenReturn(SESSION_ID);

    // Mock HelixDataAccessor
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(manager.getHelixDataAccessor()).thenReturn(dataAccessor);

    event.addAttribute(AttributeName.helixmanager.name(), manager);

    // Setup ResourceControllerDataProvider
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getClusterConfig()).thenReturn(new ClusterConfig("TestCluster"));
    when(cache.getStateModelDef("MasterSlave")).thenReturn(stateModelDef);

    // Mock live instances
    Map<String, LiveInstance> liveInstances = new HashMap<>();
    liveInstances.put(INSTANCE_0, createLiveInstance(INSTANCE_0));
    liveInstances.put(INSTANCE_1, createLiveInstance(INSTANCE_1));
    liveInstances.put(INSTANCE_2, createLiveInstance(INSTANCE_2));
    liveInstances.put(INSTANCE_3, createLiveInstance(INSTANCE_3));
    when(cache.getLiveInstances()).thenReturn(liveInstances);

    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Setup resources
    Map<String, Resource> resourceMap = new HashMap<>();
    Resource resource = new Resource(TEST_RESOURCE);
    resource.setStateModelDefRef("MasterSlave");
    resource.addPartition(PARTITION_0);
    resourceMap.put(TEST_RESOURCE, resource);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    return event;
  }

  private Map<String, CurrentState> setupCurrentStatesForPrioritizationCase1() {
    Map<String, CurrentState> currentStateMap = new HashMap<>();

    // All instances in OFFLINE state
    for (String instance : new String[] {
        INSTANCE_0, INSTANCE_1, INSTANCE_2, INSTANCE_3
    }) {
      CurrentState currentState = new CurrentState(TEST_RESOURCE);
      if (instance.equals(INSTANCE_0)) {
        currentState.setState(PARTITION_0, "MASTER");
      } else if (instance.equals(INSTANCE_1)) {
        currentState.setState(PARTITION_0, "SLAVE");
      } else {
        currentState.setState(PARTITION_0, "OFFLINE");
      }
      currentState.setSessionId(SESSION_ID);
      currentState.setStateModelDefRef("MasterSlave");
      currentStateMap.put(instance, currentState);
    }

    return currentStateMap;
  }

  private Map<String, CurrentState> setupCurrentStatesForPrioritizationCase2() {
    Map<String, CurrentState> currentStateMap = new HashMap<>();

    // All instances in OFFLINE state
    for (String instance : new String[] {
        INSTANCE_0, INSTANCE_1, INSTANCE_2
    }) {
      CurrentState currentState = new CurrentState(TEST_RESOURCE);
      currentState.setState(PARTITION_0, "OFFLINE");
      currentState.setSessionId(SESSION_ID);
      currentState.setStateModelDefRef("MasterSlave");
      currentStateMap.put(instance, currentState);
    }

    return currentStateMap;
  }

  private CurrentStateOutput setupCurrentStateOutput(Map<String, CurrentState> currentStateMap) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    for (Map.Entry<String, CurrentState> entry : currentStateMap.entrySet()) {
      String instance = entry.getKey();
      CurrentState currentState = entry.getValue();

      currentStateOutput.setCurrentState(TEST_RESOURCE, new Partition(PARTITION_0), instance,
          currentState.getState(PARTITION_0));
    }

    return currentStateOutput;
  }

  private LiveInstance createLiveInstance(String instanceName) {
    // Create LiveInstance with proper ZNRecord initialization
    ZNRecord znRecord = new ZNRecord(instanceName);
    znRecord.setEphemeralOwner(Long.parseLong(SESSION_ID));

    LiveInstance liveInstance = new LiveInstance(znRecord);
    liveInstance.setSessionId(SESSION_ID);
    liveInstance.setHelixVersion("1.0.0");
    liveInstance.setLiveInstance(instanceName);

    return liveInstance;
  }

  private Message createMessage(String fromState, String toState, String tgtName) {
    Message message =
        new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
    message.setFromState(fromState);
    message.setToState(toState);
    message.setTgtName(tgtName);
    message.setTgtSessionId(SESSION_ID);
    message.setResourceName(TEST_RESOURCE);
    message.setPartitionName(PARTITION_0);
    message.setStateModelDef("MasterSlave");
    message.setSrcName("Controller");
    message.setSrcSessionId(SESSION_ID);
    return message;
  }
}
