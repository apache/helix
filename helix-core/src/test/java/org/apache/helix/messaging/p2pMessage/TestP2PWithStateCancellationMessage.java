package org.apache.helix.messaging.p2pMessage;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BaseStageTest;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.IntermediateStateOutput;
import org.apache.helix.controller.stages.MessageOutput;
import org.apache.helix.controller.stages.resource.ResourceMessageGenerationPhase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestP2PWithStateCancellationMessage extends BaseStageTest {
  private final static String CLUSTER_NAME = "MockCluster";
  private final static String RESOURCE_NAME = "MockResource";

  @Test
  public void testP2PWithStateCancellationMessage() {
    ClusterEvent event = generateClusterEvent();
    runStage(event, new ResourceMessageGenerationPhase());
    MessageOutput messageOutput = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    // No message should be sent for partition 0
    Assert.assertEquals(messageOutput.getMessages(RESOURCE_NAME, new Partition("0")).size(), 0);

    // One cancellation message should be sent out for partition 1
    List<Message> messages = messageOutput.getMessages(RESOURCE_NAME, new Partition("1"));
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getMsgType(),
        Message.MessageType.STATE_TRANSITION_CANCELLATION.name());
  }

  private ClusterEvent generateClusterEvent() {
    Mock mock = new Mock();
    ClusterEvent event =
        new ClusterEvent(CLUSTER_NAME, ClusterEventType.IdealStateChange, "randomId");
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);

    // mock manager
    event.addAttribute(AttributeName.helixmanager.name(), mock.manager);
    when(mock.manager.getHelixDataAccessor()).thenReturn(mock.accessor);
    when(mock.manager.getSessionId()).thenReturn(UUID.randomUUID().toString());
    when(mock.manager.getInstanceName()).thenReturn("CONTROLLER");

    // mock resource
    ResourceConfig resourceConfig = new ResourceConfig(RESOURCE_NAME);
    Resource resource = new Resource(RESOURCE_NAME, clusterConfig, resourceConfig);
    resource.addPartition("0");
    resource.addPartition("1");
    resource.setStateModelDefRef("MasterSlave");
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        Collections.singletonMap(RESOURCE_NAME, resource));

    // mock cache with two live instances and session id.
    LiveInstance l1 = new LiveInstance("localhost_1");
    l1.setSessionId(UUID.randomUUID().toString());
    LiveInstance l2 = new LiveInstance("localhost_2");
    l2.setSessionId(UUID.randomUUID().toString());
    event.addAttribute(AttributeName.ControllerDataProvider.name(), mock.cache);
    when(mock.cache.getStateModelDef("MasterSlave")).thenReturn(MasterSlaveSMD.build());
    when(mock.cache.getClusterConfig()).thenReturn(clusterConfig);
    when(mock.cache.getLiveInstances()).thenReturn(Arrays.asList(l1, l2).stream().collect(
        Collectors.toMap(LiveInstance::getId, Function.identity())));

    // mock current state output. Generate 3 messages:
    // 1. main message staying ZK contains #2 p2p message.
    // 2. p2p message that should be hide in #1 message
    // 3. message should be cancelled since target state changed.
    Message message =
        new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
    message.setSrcName(manager.getInstanceName());
    message.setTgtName("localhost_1");
    message.setMsgState(Message.MessageState.NEW);
    message.setPartitionName("0");
    message.setResourceName(resource.getResourceName());
    message.setFromState("MASTER");
    message.setToState("SLAVE");
    message.setTgtSessionId(UUID.randomUUID().toString());
    message.setSrcSessionId(manager.getSessionId());
    message.setStateModelDef("MasterSlave");
    message.setTgtSessionId(UUID.randomUUID().toString());

    Message relayMessage =
        new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
    relayMessage.setSrcName("localhost_1");
    relayMessage.setTgtName("localhost_2");
    relayMessage.setMsgState(Message.MessageState.NEW);
    relayMessage.setPartitionName("0");
    relayMessage.setResourceName(resource.getResourceName());
    relayMessage.setFromState("SLAVE");
    relayMessage.setToState("MASTER");
    relayMessage.setTgtSessionId(UUID.randomUUID().toString());
    relayMessage.setSrcSessionId(manager.getSessionId());
    relayMessage.setStateModelDef("MasterSlave");
    relayMessage.setTgtSessionId(UUID.randomUUID().toString());

    Message messageToBeCancelled =
        new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());

    messageToBeCancelled.setSrcName(manager.getInstanceName());
    messageToBeCancelled.setTgtName("localhost_2");
    messageToBeCancelled.setMsgState(Message.MessageState.NEW);
    messageToBeCancelled.setPartitionName("1");
    messageToBeCancelled.setResourceName(resource.getResourceName());
    messageToBeCancelled.setFromState("MASTER");
    messageToBeCancelled.setToState("SLAVE");
    messageToBeCancelled.setTgtSessionId(UUID.randomUUID().toString());
    messageToBeCancelled.setSrcSessionId(manager.getSessionId());
    messageToBeCancelled.setStateModelDef("MasterSlave");
    messageToBeCancelled.setTgtSessionId(UUID.randomUUID().toString());

    // mock current state & intermediate state output
    // Keep partition 0 same target state to make sure p2p message not be cancelled.
    // Make partition 1 target state change so Helix should send cancellation message.

    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setPendingMessage(RESOURCE_NAME, new Partition("0"), "localhost_1", message);
    currentStateOutput.setPendingMessage(RESOURCE_NAME, new Partition("0"), "localhost_2", relayMessage);
    currentStateOutput
        .setPendingMessage(RESOURCE_NAME, new Partition("1"), "localhost_2", messageToBeCancelled);
    currentStateOutput.setCurrentState(RESOURCE_NAME, new Partition("0"), "localhost_1", "MASTER");
    currentStateOutput.setCurrentState(RESOURCE_NAME, new Partition("0"), "localhost_2", "SLAVE");
    currentStateOutput.setCurrentState(RESOURCE_NAME, new Partition("1"), "localhost_2", "MASTER");
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    IntermediateStateOutput intermediateStateOutput = new IntermediateStateOutput();
    intermediateStateOutput.setState(RESOURCE_NAME, new Partition("0"), "localhost_1", "SLAVE");
    intermediateStateOutput.setState(RESOURCE_NAME, new Partition("0"), "localhost_2", "MASTER");
    intermediateStateOutput.setState(RESOURCE_NAME, new Partition("1"), "localhost_2", "MASTER");
    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), intermediateStateOutput);

    return event;
  }

  private final class Mock {
    private ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    private HelixManager manager = mock(ZKHelixManager.class);
    private HelixDataAccessor accessor = mock(ZKHelixDataAccessor.class);
  }
}
