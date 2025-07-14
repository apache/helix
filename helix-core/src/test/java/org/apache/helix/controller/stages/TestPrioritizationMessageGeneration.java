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
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.OnlineOfflineWithBootstrapSMD;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.helix.tools.StateModelConfigGenerator.generateConfigForMasterSlave;
import static org.apache.helix.tools.StateModelConfigGenerator.generateConfigForOnlineOffline;
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
  private static final String INSTANCE_4 = "localhost_4";
  private static final String SESSION_ID = "123";

  // === Tests for upward transitions and replica counting ===

  @Test
  public void testCurrentReplicaCountForUpwardTransitions() throws Exception {
    // Test: Upward transitions from non-second top states should receive currentActiveReplicaNumber
    StateModelDefinition stateModelDef = new StateModelDefinition(generateConfigForMasterSlave());
    // Setup: 1 MASTER, 1 SLAVE, 2 OFFLINE (current active = 2)
    Map<String, CurrentState> currentStates = createCurrentStates(Map.of(INSTANCE_0, "MASTER",
        INSTANCE_1, "SLAVE", INSTANCE_2, "OFFLINE", INSTANCE_3, "OFFLINE"), "MasterSlave");
    // Action: Move 2 OFFLINE instances to SLAVE (upward transitions)
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "MASTER", // No change
        INSTANCE_1, "SLAVE", // No change
        INSTANCE_2, "SLAVE", // OFFLINE -> SLAVE (upward)
        INSTANCE_3, "SLAVE" // OFFLINE -> SLAVE (upward)
    );

    List<Message> messages = processAndGetMessages(stateModelDef, currentStates, bestPossible, 4);

    // Verify: 2 messages generated, both with current active replica count = 2
    // Current active replicas: 1 MASTER + 1 SLAVE = 2
    Assert.assertEquals(messages.size(), 2);
    for (Message msg : messages) {
      Assert.assertEquals(msg.getCurrentActiveReplicaNumber(), 2,
          "Upward transitions should have currentActiveReplicaNumber = current active replica count");
      Assert.assertTrue(msg.getTgtName().equals(INSTANCE_2) || msg.getTgtName().equals(INSTANCE_3));
    }
  }

  @Test
  public void testZeroReplicaScenario() throws Exception {
    // Test: All instances starting from OFFLINE (0 active replicas)
    StateModelDefinition stateModelDef = new StateModelDefinition(generateConfigForMasterSlave());

    // Setup: All instances OFFLINE (current active = 0)
    Map<String, CurrentState> currentStates = createCurrentStates(
        Map.of(INSTANCE_0, "OFFLINE", INSTANCE_1, "OFFLINE", INSTANCE_2, "OFFLINE"), "MasterSlave");

    // Action: Create 1 MASTER, 2 SLAVE from all OFFLINE
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "SLAVE", // OFFLINE -> SLAVE (upward)
        INSTANCE_1, "MASTER", // OFFLINE -> MASTER (upward)
        INSTANCE_2, "SLAVE" // OFFLINE -> SLAVE (upward)
    );

    List<Message> messages = processAndGetMessages(stateModelDef, currentStates, bestPossible, 3);

    // Verify: All messages have currentActiveReplicaNumber = 0
    Assert.assertEquals(messages.size(), 3);
    for (Message msg : messages) {
      Assert.assertEquals(msg.getCurrentActiveReplicaNumber(), 0,
          "All upward transitions should have currentActiveReplicaNumber = 0");
    }
  }

  // === Tests for non-upward transitions ===

  @Test
  public void testNoReplicaNumberForNonUpwardTransitions() throws Exception {
    // Test: Downward transitions should not receive currentActiveReplicaNumber
    StateModelDefinition stateModelDef = new StateModelDefinition(generateConfigForMasterSlave());

    Map<String, CurrentState> currentStates =
        createCurrentStates(Map.of(INSTANCE_0, "SLAVE"), "MasterSlave");

    // Action: SLAVE -> OFFLINE (downward transition)
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "OFFLINE");

    List<Message> messages = processAndGetMessages(stateModelDef, currentStates, bestPossible, 1);

    // Verify: Downward transition gets default value (-1)
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getCurrentActiveReplicaNumber(), -1,
        "Non-upward state transitions should not have currentActiveReplicaNumber assigned");
  }

  // === Tests for pending messages ===

  @Test
  public void testPendingMessagesDoNotAffectCurrentReplicaCount() throws Exception {
    // Test: SLAVE -> MASTER (second top to top) should not receive currentActiveReplicaNumber
    StateModelDefinition stateModelDef = new StateModelDefinition(generateConfigForMasterSlave());

    // Setup: 1 MASTER, 2 OFFLINE (current active = 1)
    Map<String, CurrentState> currentStates = createCurrentStates(
        Map.of(INSTANCE_0, "MASTER", INSTANCE_1, "OFFLINE", INSTANCE_2, "OFFLINE"), "MasterSlave");

    CurrentStateOutput currentStateOutput = createCurrentStateOutput(currentStates);

    // Add pending message for INSTANCE_1: OFFLINE->SLAVE
    Message pendingMsg = createMessage("OFFLINE", "SLAVE", INSTANCE_1);
    currentStateOutput.setPendingMessage(TEST_RESOURCE, new Partition(PARTITION_0), INSTANCE_1,
        pendingMsg);

    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput, 3);

    // Action: Both offline instances should become SLAVE
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "MASTER", // No change
        INSTANCE_1, "SLAVE", // Has pending message, no new message
        INSTANCE_2, "SLAVE" // OFFLINE -> SLAVE (new transition)
    );

    setBestPossibleState(event, bestPossible);
    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    // Verify: Only new message for INSTANCE_2, with current active count = 1 (ignoring pending)
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getTgtName(), INSTANCE_2);
    Assert.assertEquals(messages.get(0).getCurrentActiveReplicaNumber(), 1,
        "currentActiveReplicaNumber should be based on current active replicas only, not including pending transitions");
  }

  // === Tests for different state model types ===

  @Test
  public void testSingleTopStateModelWithoutSecondaryTop() throws Exception {
    // Test: ONLINE-OFFLINE model (single top without secondary) - only top + ERROR count as active
    StateModelDefinition onlineOfflineStateModel = CustomOnlineOfflineSMD.build(1);

    // Verify this is a single-top state model
    Assert.assertTrue(onlineOfflineStateModel.isSingleTopStateModel(),
        "ONLINE-OFFLINE should be a single-top state model");

    // Setup: 0 ONLINE, 1 ERROR, 1 OFFLINE (current active = 1: ERROR only)
    Map<String, CurrentState> currentStates = createCurrentStates(
        Map.of(INSTANCE_0, "OFFLINE", INSTANCE_1, "ERROR", INSTANCE_2, "OFFLINE"), "OnlineOffline");

    // Action: One OFFLINE becomes ONLINE
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "ONLINE", // OFFLINE -> ONLINE (upward)
        INSTANCE_1, "ERROR", // No change
        INSTANCE_2, "OFFLINE" // No change
    );

    List<Message> messages = processAndGetMessagesForOnlineOffline(onlineOfflineStateModel,
        currentStates, bestPossible, 3);

    // Verify: Current active = 1 (0 ONLINE + 1 ERROR)
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getCurrentActiveReplicaNumber(), 1,
        "Single-top without secondary: only top states + ERROR count as active");
  }

  @Test
  public void testSingleTopStateModelWithSecondaryTop() throws Exception {
    // Test: MASTER-SLAVE model (single top with secondary) - top + secondary + ERROR count as
    // active
    StateModelDefinition stateModelDef = new StateModelDefinition(generateConfigForMasterSlave());

    // Setup: 1 MASTER, 2 SLAVE, 1 ERROR, 1 OFFLINE (current active = 4)
    Map<String, CurrentState> currentStates =
        createCurrentStates(Map.of(INSTANCE_0, "MASTER", INSTANCE_1, "SLAVE", INSTANCE_2, "SLAVE",
            INSTANCE_3, "ERROR", INSTANCE_4, "OFFLINE"), "MasterSlave");

    // Action: OFFLINE becomes SLAVE
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "MASTER", INSTANCE_1, "SLAVE", INSTANCE_2,
        "SLAVE", INSTANCE_3, "ERROR", INSTANCE_4, "SLAVE" // OFFLINE -> SLAVE (upward)
    );

    List<Message> messages = processAndGetMessages(stateModelDef, currentStates, bestPossible, 5);

    // Verify: Current active = 4 (1 MASTER + 2 SLAVE + 1 ERROR)
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getCurrentActiveReplicaNumber(), 4,
        "Single-top with secondary: top + secondary top + ERROR count as active");
  }

  @Test
  public void testMultiTopStateModelWithoutSecondaryTop() throws Exception {
    StateModelDefinition stateModelDef = new StateModelDefinition(generateConfigForOnlineOffline());

    // Setup: 1 ONLINE, 2 OFFLINE (current active = 1)
    Map<String, CurrentState> currentStates = createCurrentStates(
        Map.of(INSTANCE_0, "ONLINE", INSTANCE_1, "OFFLINE", INSTANCE_2, "OFFLINE"),
        "OfflineOnline");

    // Action: Both OFFLINE become ONLINE
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "ONLINE", // No change
        INSTANCE_1, "ONLINE", // OFFLINE -> ONLINE (upward)
        INSTANCE_2, "ONLINE" // OFFLINE -> ONLINE (upward)
    );

    List<Message> messages =
        processAndGetMessagesForOnlineOffline(stateModelDef, currentStates, bestPossible, 3);

    // Verify: Current active = 1 (only ONLINE states count)
    Assert.assertEquals(messages.size(), 2);
    for (Message msg : messages) {
      Assert.assertEquals(msg.getCurrentActiveReplicaNumber(), 1,
          "Multi-top state model without secondary top: only top states count as active");
    }
  }

  @Test
  public void testMultiTopStateModelWithSecondaryTop() throws Exception {
    StateModelDefinition stateModelDef = OnlineOfflineWithBootstrapSMD.build();

    // Setup: 2 ONLINE, 1 BOOTSTRAP, 1 OFFLINE (current active = 3)
    Map<String, CurrentState> currentStates =
        createCurrentStates(Map.of(INSTANCE_0, "ONLINE", INSTANCE_1, "ONLINE", INSTANCE_2,
            "BOOTSTRAP", INSTANCE_3, "OFFLINE"), "OnlineOfflineWithBootstrap");

    // Action: OFFLINE becomes BOOTSTRAP.
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "ONLINE", // No change
        INSTANCE_1, "ONLINE", // No change
        INSTANCE_2, "BOOTSTRAP", // No change
        INSTANCE_3, "BOOTSTRAP" // OFFLINE -> BOOTSTRAP (upward)
    );

    List<Message> messages =
        processAndGetMessagesForOnlineOffline(stateModelDef, currentStates, bestPossible, 4);

    // Verify: Current active = 3 (ONLINE + BOOTSTRAP state counts)
    Assert.assertEquals(messages.size(), 1);
    for (Message msg : messages) {
      Assert.assertEquals(msg.getCurrentActiveReplicaNumber(), 3,
          "Multi-top state model without secondary top: top states and secondary top states count as active");
    }
  }

  // === Tests for ERROR state handling ===

  @Test
  public void testErrorStateIncludedInActiveCount() throws Exception {
    // Test: ERROR states are always counted as active
    StateModelDefinition stateModelDef = new StateModelDefinition(generateConfigForMasterSlave());

    // Setup: 1 MASTER, 1 ERROR, 1 OFFLINE (current active = 2)
    Map<String, CurrentState> currentStates = createCurrentStates(
        Map.of(INSTANCE_0, "MASTER", INSTANCE_1, "ERROR", INSTANCE_2, "OFFLINE"), "MasterSlave");

    // Action: OFFLINE becomes SLAVE
    Map<String, String> bestPossible =
        Map.of(INSTANCE_0, "MASTER", INSTANCE_1, "ERROR", INSTANCE_2, "SLAVE");

    List<Message> messages = processAndGetMessages(stateModelDef, currentStates, bestPossible, 3);

    // Verify: Current active = 2 (1 MASTER + 1 ERROR)
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getCurrentActiveReplicaNumber(), 2,
        "ERROR state replicas should be included in active count");
  }

  @Test
  public void testTransitionFromErrorToOffline() throws Exception {
    // Test: ERROR -> OFFLINE is a downward transition (active to inactive)
    StateModelDefinition stateModelDef = new StateModelDefinition(generateConfigForMasterSlave());

    Map<String, CurrentState> currentStates =
        createCurrentStates(Map.of(INSTANCE_0, "ERROR"), "MasterSlave");

    // Action: ERROR -> OFFLINE (standard recovery pattern)
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "OFFLINE");

    List<Message> messages = processAndGetMessages(stateModelDef, currentStates, bestPossible, 1);

    // Verify: Downward transition gets default value (-1)
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getCurrentActiveReplicaNumber(), -1,
        "ERROR→OFFLINE transitions should not have currentActiveReplicaNumber assigned (downward)");
  }

  // === Tests for DROPPED state handling ===

  @Test
  public void testDroppedReplicasExcludedFromActiveCount() throws Exception {
    // Test: DROPPED replicas are excluded from active count calculations
    StateModelDefinition stateModelDef = new StateModelDefinition(generateConfigForMasterSlave());

    // Setup: 1 MASTER, 1 OFFLINE, 1 OFFLINE (current active = 1)
    Map<String, CurrentState> currentStates = createCurrentStates(
        Map.of(INSTANCE_0, "MASTER", INSTANCE_1, "OFFLINE", INSTANCE_2, "OFFLINE"), "MasterSlave");

    // Action: One OFFLINE drops, other becomes SLAVE
    Map<String, String> bestPossible = Map.of(INSTANCE_0, "MASTER", // No change
        INSTANCE_1, "DROPPED", // OFFLINE -> DROPPED (downward)
        INSTANCE_2, "SLAVE" // OFFLINE -> SLAVE (upward)
    );

    List<Message> messages = processAndGetMessages(stateModelDef, currentStates, bestPossible, 3);

    // Verify: 2 messages, upward gets active count, DROPPED doesn't
    Assert.assertEquals(messages.size(), 2);

    Message upwardMsg = messages.stream()
        .filter(m -> m.getTgtName().equals(INSTANCE_2) && m.getToState().equals("SLAVE"))
        .findFirst().orElse(null);
    Message droppedMsg = messages.stream()
        .filter(m -> m.getTgtName().equals(INSTANCE_1) && m.getToState().equals("DROPPED"))
        .findFirst().orElse(null);

    Assert.assertNotNull(upwardMsg, "Should have upward transition message");
    Assert.assertNotNull(droppedMsg, "Should have dropped transition message");

    Assert.assertEquals(upwardMsg.getCurrentActiveReplicaNumber(), 1,
        "Upward transition should have current active replica count");
    Assert.assertEquals(droppedMsg.getCurrentActiveReplicaNumber(), -1,
        "DROPPED transition should not have currentActiveReplicaNumber");
  }

  // === Helper methods ===

  /**
   * Creates current state map for multiple instances with specified states.
   */
  private Map<String, CurrentState> createCurrentStates(Map<String, String> instanceStates,
      String stateModelRef) {
    Map<String, CurrentState> currentStateMap = new HashMap<>();
    for (Map.Entry<String, String> entry : instanceStates.entrySet()) {
      CurrentState currentState = new CurrentState(TEST_RESOURCE);
      currentState.setState(PARTITION_0, entry.getValue());
      currentState.setSessionId(SESSION_ID);
      currentState.setStateModelDefRef(stateModelRef);
      currentStateMap.put(entry.getKey(), currentState);
    }
    return currentStateMap;
  }

  /**
   * Creates CurrentStateOutput from current state map.
   */
  private CurrentStateOutput createCurrentStateOutput(Map<String, CurrentState> currentStateMap) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    for (Map.Entry<String, CurrentState> entry : currentStateMap.entrySet()) {
      String instance = entry.getKey();
      CurrentState currentState = entry.getValue();
      currentStateOutput.setCurrentState(TEST_RESOURCE, new Partition(PARTITION_0), instance,
          currentState.getState(PARTITION_0));
    }
    return currentStateOutput;
  }

  /**
   * Processes state transitions and returns generated messages.
   */
  private List<Message> processAndGetMessages(StateModelDefinition stateModelDef,
      Map<String, CurrentState> currentStates, Map<String, String> bestPossible, int instanceCount)
      throws Exception {
    CurrentStateOutput currentStateOutput = createCurrentStateOutput(currentStates);
    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput, instanceCount);
    setBestPossibleState(event, bestPossible);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    return output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));
  }

  /**
   * Processes state transitions for OnlineOffline state model.
   */
  private List<Message> processAndGetMessagesForOnlineOffline(StateModelDefinition stateModelDef,
      Map<String, CurrentState> currentStates, Map<String, String> bestPossible, int instanceCount)
      throws Exception {
    CurrentStateOutput currentStateOutput = createCurrentStateOutput(currentStates);
    ClusterEvent event =
        prepareClusterEventForOnlineOffline(stateModelDef, currentStateOutput, instanceCount);
    setBestPossibleState(event, bestPossible);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    return output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));
  }

  /**
   * Sets best possible state in cluster event.
   */
  private void setBestPossibleState(ClusterEvent event, Map<String, String> partitionMap) {
    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);
  }

  /**
   * Prepares cluster event with necessary mock objects and configurations.
   */
  private ClusterEvent prepareClusterEvent(StateModelDefinition stateModelDef,
      CurrentStateOutput currentStateOutput, int instanceCount) {
    ClusterEvent event = createBaseClusterEvent(currentStateOutput);

    // Setup ResourceControllerDataProvider
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getClusterConfig()).thenReturn(new ClusterConfig(TEST_CLUSTER));
    when(cache.getStateModelDef("MasterSlave")).thenReturn(stateModelDef);
    when(cache.getLiveInstances()).thenReturn(createLiveInstances(instanceCount));
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Setup resources
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        createResourceMap("MasterSlave"));

    return event;
  }

  /**
   * Prepares cluster event for OnlineOffline state model.
   */
  private ClusterEvent prepareClusterEventForOnlineOffline(StateModelDefinition stateModelDef,
      CurrentStateOutput currentStateOutput, int instanceCount) {
    ClusterEvent event = createBaseClusterEvent(currentStateOutput);

    // Setup ResourceControllerDataProvider for OnlineOffline
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getClusterConfig()).thenReturn(new ClusterConfig(TEST_CLUSTER));
    when(cache.getStateModelDef("OfflineOnline")).thenReturn(stateModelDef);
    when(cache.getLiveInstances()).thenReturn(createLiveInstances(instanceCount));
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Setup resources for OnlineOffline
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        createResourceMap("OfflineOnline"));

    return event;
  }

  /**
   * Creates base cluster event with common attributes.
   */
  private ClusterEvent createBaseClusterEvent(CurrentStateOutput currentStateOutput) {
    ClusterEvent event = new ClusterEvent(TEST_CLUSTER, ClusterEventType.Unknown);

    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);

    // Mock HelixManager and HelixDataAccessor
    HelixManager manager = mock(HelixManager.class);
    when(manager.getInstanceName()).thenReturn("Controller");
    when(manager.getSessionId()).thenReturn(SESSION_ID);
    when(manager.getHelixDataAccessor()).thenReturn(mock(HelixDataAccessor.class));
    event.addAttribute(AttributeName.helixmanager.name(), manager);

    return event;
  }

  /**
   * Creates mock live instances for the specified count.
   */
  private Map<String, LiveInstance> createLiveInstances(int instanceCount) {
    Map<String, LiveInstance> liveInstances = new HashMap<>();
    for (int i = 0; i < instanceCount; i++) {
      String instanceName = "localhost_" + i;
      ZNRecord znRecord = new ZNRecord(instanceName);
      znRecord.setEphemeralOwner(Long.parseLong(SESSION_ID));

      LiveInstance liveInstance = new LiveInstance(znRecord);
      liveInstance.setSessionId(SESSION_ID);
      liveInstance.setHelixVersion("1.0.0");
      liveInstance.setLiveInstance(instanceName);

      liveInstances.put(instanceName, liveInstance);
    }
    return liveInstances;
  }

  /**
   * Creates resource map with specified state model reference.
   */
  private Map<String, Resource> createResourceMap(String stateModelRef) {
    Map<String, Resource> resourceMap = new HashMap<>();
    Resource resource = new Resource(TEST_RESOURCE);
    resource.setStateModelDefRef(stateModelRef);
    resource.addPartition(PARTITION_0);
    resourceMap.put(TEST_RESOURCE, resource);
    return resourceMap;
  }

  /**
   * Creates a state transition message.
   */
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

  /**
   * Custom OnlineOffline state model with configurable upper bounds for ONLINE state.
   * Enables testing scenarios with specific replica count constraints.
   */
  private static final class CustomOnlineOfflineSMD {
    private static final String STATE_MODEL_NAME = "CustomOnlineOffline";

    /**
     * States for the CustomOnlineOffline state model
     */
    private enum States {
      ONLINE,
      OFFLINE
    }

    /**
     * Build OnlineOffline state model definition with custom instance count
     * @param instanceCount the maximum number of instances that can be in ONLINE state
     * @return StateModelDefinition for OnlineOffline model with custom bounds
     */
    public static StateModelDefinition build(int instanceCount) {
      if (instanceCount <= 0) {
        throw new IllegalArgumentException(
            "Instance count must be positive, got: " + instanceCount);
      }

      StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);

      // init state
      builder.initialState(States.OFFLINE.name());

      // add states
      builder.addState(States.ONLINE.name(), 0);
      builder.addState(States.OFFLINE.name(), 1);
      for (final HelixDefinedState state : HelixDefinedState.values()) {
        builder.addState(state.name());
      }

      // add transitions
      builder.addTransition(States.ONLINE.name(), States.OFFLINE.name(), 0);
      builder.addTransition(States.OFFLINE.name(), States.ONLINE.name(), 1);
      builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name());

      // bounds - uses the instanceCount parameter
      builder.dynamicUpperBound(States.ONLINE.name(), String.valueOf(instanceCount));

      return builder.build();
    }
  }
}
