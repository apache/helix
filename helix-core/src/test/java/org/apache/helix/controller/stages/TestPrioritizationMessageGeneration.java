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
  public void testPrioritizationForUpwardStateTransition() throws Exception {

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

    // Messages should have replica numbers assigned based on priority
    Map<String, Integer> replicaNumbers = new HashMap<>();
    for (Message msg : messages) {
      int replicaNumber = msg.getCurrentReplicaNumber();
      replicaNumbers.put(msg.getTgtName(), replicaNumber);
    }

    // Verify replica numbers are assigned (not -1) for upward transitions
    for (String instance : new String[] {
        INSTANCE_2, INSTANCE_3
    }) {
      Assert.assertTrue(replicaNumbers.get(instance) >= 0,
          "Replica number should be assigned for " + instance);
    }

    // Verify the replica numbers are decreasing (higher number = higher priority)
    Assert.assertTrue(replicaNumbers.get(INSTANCE_3) > replicaNumbers.get(INSTANCE_2),
        "Earlier transitions should have higher replica numbers");
  }

  @Test
  public void testPrioritizationForUpwardStateTransitionWithAllOfflineInstances() throws Exception {

    // Test prioritization for upward state transitions from non-second top states
    // to second top or top states
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Set up current states - multiple instances in OFFLINE state
    // We want to test that they get prioritized when transitioning to SLAVE (second top) or MASTER
    // (top)
    Map<String, CurrentState> currentStateMap = setupCurrentStatesForPrioritizationCase2();
    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);

    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants to move some nodes to SLAVE/MASTER
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

    // Messages should have replica numbers assigned based on priority
    Map<String, Integer> replicaNumbers = new HashMap<>();
    for (Message msg : messages) {
      int replicaNumber = msg.getCurrentReplicaNumber();
      replicaNumbers.put(msg.getTgtName(), replicaNumber);
    }

    // Verify replica numbers are assigned (not -1) for upward transitions
    for (String instance : new String[] {
        INSTANCE_0, INSTANCE_1, INSTANCE_2
    }) {
      Assert.assertTrue(replicaNumbers.get(instance) >= 0,
          "Replica number should be assigned for " + instance);
    }

    // Verify the replica numbers are decreasing (higher number = higher priority)
    Assert.assertTrue(replicaNumbers.get(INSTANCE_2) > replicaNumbers.get(INSTANCE_0),
        "Earlier transitions should have higher replica numbers");
  }

  @Test
  public void testNoPrioritizationForNonUpwardTransitions() throws Exception {
    // Test that non-upward transitions don't get prioritized
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Set up current states - instance already in SLAVE (second top)
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

    // Should have default replica number (-1) for non-upward transition
    Message msg = messages.get(0);
    Assert.assertEquals(msg.getCurrentReplicaNumber(), -1,
        "Non-upward transitions should not be prioritized");
  }

  @Test
  public void testPrioritizationWithPendingMessages() throws Exception {
    // Test prioritization when there are already pending messages
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    Map<String, CurrentState> currentStateMap = new HashMap<>();

    // Instance 0: OFFLINE with pending OFFLINE->SLAVE message
    CurrentState currentState0 = new CurrentState(TEST_RESOURCE);
    currentState0.setState(PARTITION_0, "OFFLINE");
    currentState0.setSessionId(SESSION_ID);
    currentState0.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_0, currentState0);

    // Create pending message separately
    Message pendingMsg = createMessage("OFFLINE", "SLAVE", INSTANCE_0);
    pendingMsg.setMsgId(UUID.randomUUID().toString());

    // Instance 1: OFFLINE with no pending message
    CurrentState currentState1 = new CurrentState(TEST_RESOURCE);
    currentState1.setState(PARTITION_0, "OFFLINE");
    currentState1.setSessionId(SESSION_ID);
    currentState1.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_1, currentState1);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);

    // Add pending message to the CurrentStateOutput, not CurrentState
    currentStateOutput.setPendingMessage(TEST_RESOURCE, new Partition(PARTITION_0), INSTANCE_0,
        pendingMsg);
    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants both to be SLAVE
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "SLAVE");
    partitionMap.put(INSTANCE_1, "SLAVE");

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    // Should only generate message for instance 1 (instance 0 already has pending message)
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getTgtName(), INSTANCE_1);

    // The message should have a replica number since pending messages are considered
    Assert.assertTrue(messages.get(0).getCurrentReplicaNumber() >= 0,
        "Should assign replica number considering pending messages");
  }

  @Test
  public void testTransitionFromSecondTopState() throws Exception {
    // Test that transitions from second top states are not prioritized
    StateModelDefinition stateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());

    // Set up current state - instance in SLAVE state (second top)
    Map<String, CurrentState> currentStateMap = new HashMap<>();
    CurrentState currentState = new CurrentState(TEST_RESOURCE);
    currentState.setState(PARTITION_0, "SLAVE");
    currentState.setSessionId(SESSION_ID);
    currentState.setStateModelDefRef("MasterSlave");
    currentStateMap.put(INSTANCE_0, currentState);

    CurrentStateOutput currentStateOutput = setupCurrentStateOutput(currentStateMap);
    ClusterEvent event = prepareClusterEvent(stateModelDef, currentStateOutput);

    // Best possible state wants to move to MASTER (top state)
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_0, "MASTER");

    BestPossibleStateOutput bestPossibleOutput = new BestPossibleStateOutput();
    bestPossibleOutput.setState(TEST_RESOURCE, new Partition(PARTITION_0), partitionMap);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleOutput);

    process(event);

    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    List<Message> messages = output.getMessages(TEST_RESOURCE, new Partition(PARTITION_0));

    Assert.assertEquals(messages.size(), 1);

    // Should have default replica number (-1) as it's from second top state
    Message msg = messages.get(0);
    Assert.assertEquals(msg.getCurrentReplicaNumber(), -1,
        "Transitions from second top states should not be prioritized");
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
