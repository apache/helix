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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixManager;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.util.RebalanceUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestManagementMessageGeneration extends ManagementMessageGenerationPhase {
  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_RESOURCE = "resource0";
  private static final String TEST_INSTANCE = "instance0";
  private static final String TEST_PARTITION = "partition0";

  @Test
  public void testCancelPendingSTMessage() throws Exception {
    List<Message> messages = generateMessages("ONLINE", "ONLINE", "OFFLINE", true);

    Assert.assertEquals(messages.size(), 1, "Should create cancellation message");

    Message msg = messages.get(0);
    Assert.assertEquals(msg.getMsgType(), Message.MessageType.STATE_TRANSITION_CANCELLATION.name());
    Assert.assertEquals(msg.getFromState(), "ONLINE");
    Assert.assertEquals(msg.getToState(), "OFFLINE");

    messages = generateMessages("ONLINE", "ONLINE", "OFFLINE", false);
    Assert.assertEquals(messages.size(), 0);
  }

  private List<Message> generateMessages(String currentState, String fromState, String toState,
      boolean cancelPendingST) throws Exception {
    ClusterEvent event = new ClusterEvent(TEST_CLUSTER, ClusterEventType.Unknown);

    // Set current state to event
    CurrentStateOutput currentStateOutput = mock(CurrentStateOutput.class);
    Partition partition = mock(Partition.class);
    when(partition.getPartitionName()).thenReturn(TEST_PARTITION);
    when(currentStateOutput.getCurrentState(TEST_RESOURCE, partition, TEST_INSTANCE))
        .thenReturn(currentState);
    when(currentStateOutput.getCurrentStateMap(TEST_RESOURCE))
        .thenReturn(ImmutableMap.of(partition, ImmutableMap.of(TEST_INSTANCE, currentState)));

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
    ManagementControllerDataProvider cache = mock(ManagementControllerDataProvider.class);
    when(cache.getStateModelDef(TaskConstants.STATE_MODEL_NAME)).thenReturn(stateModelDefinition);
    Map<String, LiveInstance> liveInstances = mock(Map.class);
    LiveInstance mockLiveInstance = mock(LiveInstance.class);
    when(mockLiveInstance.getInstanceName()).thenReturn(TEST_INSTANCE);
    when(mockLiveInstance.getEphemeralOwner()).thenReturn("TEST");
    when(liveInstances.values()).thenReturn(Collections.singletonList(mockLiveInstance));
    when(cache.getLiveInstances()).thenReturn(liveInstances);
    ClusterConfig clusterConfig = mock(ClusterConfig.class);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(clusterConfig.isStateTransitionCancelEnabled()).thenReturn(cancelPendingST);
    PauseSignal pauseSignal = mock(PauseSignal.class);
    when(pauseSignal.getCancelPendingST()).thenReturn(true);
    when(cache.getPauseSignal()).thenReturn(pauseSignal);
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
    BestPossibleStateOutput bestPossibleStateOutput =
        RebalanceUtil.buildBestPossibleState(resourceMap.keySet(), currentStateOutput);

    // Process the event
    ClusterManagementMode mode = new ClusterManagementMode(ClusterManagementMode.Type.CLUSTER_FREEZE,
        ClusterManagementMode.Status.IN_PROGRESS);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.CLUSTER_STATUS.name(), mode);
    process(event);
    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());

    return output.getMessages(TEST_RESOURCE, partition);
  }
}
