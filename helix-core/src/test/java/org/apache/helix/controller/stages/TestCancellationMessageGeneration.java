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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCancellationMessageGeneration extends MessageGenerationPhase {
  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_RESOURCE = "resource0";
  private static final String TEST_INSTANCE = "instance0";
  private static final String TEST_PARTITION = "partition0";

  /*
   * This test checks the cancellation message generation when currentState=null and desiredState=DROPPED
   */
  @Test
  public void TestOFFLINEToDROPPED() throws Exception {

    ClusterEvent event = new ClusterEvent(TEST_CLUSTER, ClusterEventType.Unknown);


    // Set current state to event
    CurrentStateOutput currentStateOutput = mock(CurrentStateOutput.class);
    Partition partition = mock(Partition.class);
    when(partition.getPartitionName()).thenReturn(TEST_PARTITION);
    when(currentStateOutput.getCurrentState(TEST_RESOURCE, partition, TEST_INSTANCE)).thenReturn(null);
    Message message = mock(Message.class);
    when(message.getFromState()).thenReturn("OFFLINE");
    when(message.getToState()).thenReturn("SLAVE");
    when(currentStateOutput.getPendingMessage(TEST_RESOURCE, partition, TEST_INSTANCE)).thenReturn(message);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    // Set helix manager to event
    event.addAttribute(AttributeName.helixmanager.name(), mock(HelixManager.class));

    // Set controller data provider to event
    BaseControllerDataProvider cache = mock(BaseControllerDataProvider.class);
    StateModelDefinition stateModelDefinition = new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    when(cache.getStateModelDef(TaskConstants.STATE_MODEL_NAME)).thenReturn(stateModelDefinition);
    Map<String, LiveInstance> liveInstances= mock(Map.class);
    LiveInstance mockLiveInstance = mock(LiveInstance.class);
    when(mockLiveInstance.getInstanceName()).thenReturn(TEST_INSTANCE);
    when(mockLiveInstance.getEphemeralOwner()).thenReturn("TEST");
    when(liveInstances.values()).thenReturn(Arrays.asList(mockLiveInstance));
    when(cache.getLiveInstances()).thenReturn(liveInstances);
    ClusterConfig clusterConfig = mock(ClusterConfig.class);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(clusterConfig.isStateTransitionCancelEnabled()).thenReturn(true);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);


    // Set resources to rebalance to event
    Map<String, Resource> resourceMap = new HashMap<>();
    Resource resource = mock(Resource.class);
    when(resource.getResourceName()).thenReturn(TEST_RESOURCE);
    List<Partition> partitions = Arrays.asList(partition);
    when(resource.getPartitions()).thenReturn(partitions);
    when(resource.getStateModelDefRef()).thenReturn(TaskConstants.STATE_MODEL_NAME);
    resourceMap.put(TEST_RESOURCE, resource);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);

    // set up resource state map
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    PartitionStateMap partitionStateMap = new PartitionStateMap(TEST_RESOURCE);
    Map<Partition, Map<String, String>> stateMap = partitionStateMap.getStateMap();
    Map<String, String> instanceStateMap = new HashMap<>();
    instanceStateMap.put(TEST_INSTANCE, HelixDefinedState.DROPPED.name());
    stateMap.put(partition, instanceStateMap);
    bestPossibleStateOutput.setState(TEST_RESOURCE, partition, instanceStateMap);

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    process(event);
    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    Assert.assertEquals(output.getMessages(TEST_RESOURCE, partition).size(), 1);
  }

  /*
   * Tests that no cancellation message is created for
   * pending ST message of error partition reset.
   */
  @Test
  public void testNoCancellationForErrorReset() throws Exception {
    List<Message> messages = generateMessages("ERROR", "ERROR", "OFFLINE");

    Assert.assertTrue(messages.isEmpty(), "Should not create cancellation message");
  }

  /*
   * Tests that controller should be able to cancel ST: ONLINE -> OFFLINE
   */
  @Test
  public void testCancelOnlineToOffline() throws Exception {
    List<Message> messages = generateMessages("ONLINE", "ONLINE", "OFFLINE");

    Assert.assertEquals(messages.size(), 1, "Should create cancellation message");

    Message msg = messages.get(0);
    Assert.assertEquals(msg.getMsgType(), Message.MessageType.STATE_TRANSITION_CANCELLATION.name());
    Assert.assertEquals(msg.getFromState(), "ONLINE");
    Assert.assertEquals(msg.getToState(), "OFFLINE");
  }

  private List<Message> generateMessages(String currentState, String fromState, String toState)
      throws Exception {
    ClusterEvent event = new ClusterEvent(TEST_CLUSTER, ClusterEventType.Unknown);

    // Set current state to event
    CurrentStateOutput currentStateOutput = mock(CurrentStateOutput.class);
    Partition partition = mock(Partition.class);
    when(partition.getPartitionName()).thenReturn(TEST_PARTITION);
    when(currentStateOutput.getCurrentState(TEST_RESOURCE, partition, TEST_INSTANCE))
        .thenReturn(currentState);

    // Pending message for error partition reset
    Message pendingMessage = mock(Message.class);
    when(pendingMessage.getFromState()).thenReturn(fromState);
    when(pendingMessage.getToState()).thenReturn(toState);
    when(currentStateOutput.getPendingMessage(TEST_RESOURCE, partition, TEST_INSTANCE))
        .thenReturn(pendingMessage);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    // Set helix manager to event
    event.addAttribute(AttributeName.helixmanager.name(), mock(HelixManager.class));

    StateModelDefinition stateModelDefinition = new StateModelDefinition.Builder("TestStateModel")
        .addState("ONLINE", 1).addState("OFFLINE")
        .addState("DROPPED").addState("ERROR")
        .initialState("OFFLINE")
        .addTransition("ERROR", "OFFLINE", 1).addTransition("ONLINE", "OFFLINE", 2)
        .addTransition("OFFLINE", "DROPPED", 3).addTransition("OFFLINE", "ONLINE", 4)
        .build();

    // Set controller data provider to event
    BaseControllerDataProvider cache = mock(BaseControllerDataProvider.class);
    when(cache.getStateModelDef(TaskConstants.STATE_MODEL_NAME)).thenReturn(stateModelDefinition);
    Map<String, LiveInstance> liveInstances = mock(Map.class);
    LiveInstance mockLiveInstance = mock(LiveInstance.class);
    when(mockLiveInstance.getInstanceName()).thenReturn(TEST_INSTANCE);
    when(mockLiveInstance.getEphemeralOwner()).thenReturn("TEST");
    when(liveInstances.values()).thenReturn(Collections.singletonList(mockLiveInstance));
    when(cache.getLiveInstances()).thenReturn(liveInstances);
    ClusterConfig clusterConfig = mock(ClusterConfig.class);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(clusterConfig.isStateTransitionCancelEnabled()).thenReturn(true);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Set event attribute: resources to rebalance
    Map<String, Resource> resourceMap = new HashMap<>();
    Resource resource = mock(Resource.class);
    when(resource.getResourceName()).thenReturn(TEST_RESOURCE);
    List<Partition> partitions = Collections.singletonList(partition);
    when(resource.getPartitions()).thenReturn(partitions);
    when(resource.getStateModelDefRef()).thenReturn(TaskConstants.STATE_MODEL_NAME);
    resourceMap.put(TEST_RESOURCE, resource);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);

    // set up resource state map
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    PartitionStateMap partitionStateMap = new PartitionStateMap(TEST_RESOURCE);
    Map<Partition, Map<String, String>> stateMap = partitionStateMap.getStateMap();
    Map<String, String> instanceStateMap = new HashMap<>();
    instanceStateMap.put(TEST_INSTANCE, currentState);
    stateMap.put(partition, instanceStateMap);
    bestPossibleStateOutput.setState(TEST_RESOURCE, partition, instanceStateMap);

    // Process the event
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    process(event);
    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());

    return output.getMessages(TEST_RESOURCE, partition);
  }
}
