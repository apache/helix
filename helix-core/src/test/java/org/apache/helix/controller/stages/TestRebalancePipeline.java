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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRebalancePipeline extends ZkUnitTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestRebalancePipeline.class.getName());
  final String _className = getShortClassName();

  @Test
  public void testDuplicateMsg() {
    String clusterName = "CLUSTER_" + _className + "_dup";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    refreshClusterConfig(clusterName, accessor);
    HelixManager manager = new DummyClusterManager(clusterName, accessor);
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);

    final String resourceName = "testResource_dup";
    String[] resourceGroups = new String[] {
      resourceName
    };
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[] {
        0
    }, resourceGroups, 1, 1);
    setupLiveInstances(clusterName, new int[] {
        0
    });
    setupStateModel(clusterName);

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new IntermediateStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());

    // round1: set node0 currentState to OFFLINE
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "OFFLINE");

    runPipeline(event, dataRefresh);
    runPipeline(event, rebalancePipeline);
    MessageSelectionStageOutput msgSelOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: OFFLINE-SLAVE for node0");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    // round2: updates node0 currentState to SLAVE but keep the
    // message, make sure controller should not send S->M until removal is done
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_1",
        "SLAVE");

    runPipeline(event, dataRefresh);
    refreshClusterConfig(clusterName, accessor);
    runPipeline(event, rebalancePipeline);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 0, "Should NOT output 1 message: SLAVE-MASTER for node1");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testMsgTriggeredRebalance() throws Exception {
    String clusterName = "CLUSTER_" + _className + "_msgTrigger";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    refreshClusterConfig(clusterName, accessor);
    final String resourceName = "testResource_dup";
    String[] resourceGroups = new String[] {
      resourceName
    };

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[] {
        0, 1
    }, resourceGroups, 1, 2);
    setupStateModel(clusterName);
    setupInstances(clusterName, new int[] {
        0, 1
    });
    setupLiveInstances(clusterName, new int[] {
        0, 1
    });

    long msgPurgeDelay = MessageGenerationPhase.DEFAULT_OBSELETE_MSG_PURGE_DELAY;

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();
    // round1: controller sends O->S to both node0 and node1
    Thread.sleep(1000);

    Builder keyBuilder = accessor.keyBuilder();
    List<String> messages = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    Assert.assertEquals(messages.size(), 1);
    messages = accessor.getChildNames(keyBuilder.messages("localhost_1"));
    Assert.assertEquals(messages.size(), 1);

    // round2: node0 and node1 update current states but not removing messages
    // Since controller's rebalancer pipeline will GC pending messages after timeout, and both hosts
    // update current states to SLAVE, controller will send out rebalance message to
    // have one host to become master
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "SLAVE", true);
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", "session_1",
        "SLAVE", true);

    // Controller has timeout > 1sec, so after 1s, controller should not have GCed message
    Thread.sleep(1000);
    Assert.assertEquals(accessor.getChildValues(keyBuilder.messages("localhost_0")).size(), 1);
    Assert.assertEquals(accessor.getChildValues(keyBuilder.messages("localhost_1")).size(), 1);

    // After another purge delay, controller should cleanup messages and continue to rebalance
    Thread.sleep(msgPurgeDelay);
    // Manually trigger another rebalance by touching current state
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "SLAVE");
    Thread.sleep(1000);

    List<Message> host0Msg = accessor.getChildValues(keyBuilder.messages("localhost_0"));
    List<Message> host1Msg = accessor.getChildValues(keyBuilder.messages("localhost_1"));
    List<Message> allMsgs = new ArrayList<>(host0Msg);
    allMsgs.addAll(host1Msg);
    Assert.assertEquals(allMsgs.size(), 1);
    Assert.assertEquals(allMsgs.get(0).getToState(), "MASTER");
    Assert.assertEquals(allMsgs.get(0).getFromState(), "SLAVE");

    // round3: node0 changes state to master, but failed to delete message,
    // controller will clean it up
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "MASTER", true);
    Thread.sleep(msgPurgeDelay);
    // touch current state to trigger rebalance
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "MASTER", false);
    Thread.sleep(1000);
    Assert.assertTrue(accessor.getChildNames(keyBuilder.messages("localhost_0")).isEmpty());


    // round4: node0 has duplicated but valid message, i.e. there is a P2P message sent to it
    // due to error in the triggered pipeline, controller should remove duplicated message
    // immediately as the partition has became master 3 sec ago (there is already a timeout)
    Message sourceMsg = allMsgs.get(0);
    Message dupMsg = new Message(sourceMsg.getMsgType(), UUID.randomUUID().toString());
    dupMsg.getRecord().setSimpleFields(sourceMsg.getRecord().getSimpleFields());
    dupMsg.getRecord().setListFields(sourceMsg.getRecord().getListFields());
    dupMsg.getRecord().setMapFields(sourceMsg.getRecord().getMapFields());
    accessor.setProperty(dupMsg.getKey(accessor.keyBuilder(), dupMsg.getTgtName()), dupMsg);
    Thread.sleep(1000);

    messages = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    Assert.assertTrue(messages.isEmpty());

    // round5: node0 has completely invalid message, controller should immediately delete it
    dupMsg.setFromState("SLAVE");
    dupMsg.setToState("OFFLINE");
    accessor.setProperty(dupMsg.getKey(accessor.keyBuilder(), dupMsg.getTgtName()), dupMsg);
    Thread.sleep(1000);
    messages = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    Assert.assertTrue(messages.isEmpty());

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testChangeIdealStateWithPendingMsg() {
    String clusterName = "CLUSTER_" + _className + "_pending";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    HelixManager manager = new DummyClusterManager(clusterName, accessor);
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);

    ClusterDataCache cache = new ClusterDataCache();
    event.addAttribute(AttributeName.ClusterDataCache.name(), cache);
    refreshClusterConfig(clusterName, accessor);

    final String resourceName = "testResource_pending";
    String[] resourceGroups = new String[] {
      resourceName
    };
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[] {
        0
    }, resourceGroups, 1, 1);
    setupLiveInstances(clusterName, new int[] {
        0
    });
    setupStateModel(clusterName);

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new IntermediateStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());

    // round1: set node0 currentState to OFFLINE and node1 currentState to SLAVE
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "OFFLINE");

    runPipeline(event, dataRefresh);
    runPipeline(event, rebalancePipeline);
    MessageSelectionStageOutput msgSelOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: OFFLINE-SLAVE for node0");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    // round2: drop resource, but keep the
    // message, make sure controller should not send O->DROPPED until O->S is done
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.dropResource(clusterName, resourceName);
    List<IdealState> idealStates = accessor.getChildValues(accessor.keyBuilder().idealStates());
    cache.setIdealStates(idealStates);

    runPipeline(event, dataRefresh);
    cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    cache.setClusterConfig(new ClusterConfig(clusterName));
    runPipeline(event, rebalancePipeline);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 0,
        "Should not output only 1 message: OFFLINE->DROPPED for localhost_0");

    // round3: remove O->S message for localhost_0, localhost_0 still in OFFLINE
    // controller should now send O->DROPPED to localhost_0
    Builder keyBuilder = accessor.keyBuilder();
    List<String> msgIds = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    accessor.removeProperty(keyBuilder.message("localhost_0", msgIds.get(0)));
    runPipeline(event, dataRefresh);
    runPipeline(event, rebalancePipeline);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1,
        "Should output 1 message: OFFLINE->DROPPED for localhost_0");
    message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "DROPPED");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMasterXfer() {
    String clusterName = "CLUSTER_" + _className + "_xfer";

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    HelixManager manager = new DummyClusterManager(clusterName, accessor);
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    refreshClusterConfig(clusterName, accessor);

    final String resourceName = "testResource_xfer";
    String[] resourceGroups = new String[] {
      resourceName
    };
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[] {
        0, 1
    }, resourceGroups, 1, 2);
    setupLiveInstances(clusterName, new int[] {
      1
    });
    setupStateModel(clusterName);

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new IntermediateStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());

    // round1: set node1 currentState to SLAVE
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", "session_1",
        "SLAVE");

    runPipeline(event, dataRefresh);
    runPipeline(event, rebalancePipeline);
    MessageSelectionStageOutput msgSelOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: SLAVE-MASTER for node1");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "SLAVE");
    Assert.assertEquals(message.getToState(), "MASTER");
    Assert.assertEquals(message.getTgtName(), "localhost_1");

    // round2: updates node0 currentState to SLAVE but keep the
    // message, make sure controller should not send S->M until removal is done
    setupLiveInstances(clusterName, new int[] {
      0
    });
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "SLAVE");

    runPipeline(event, dataRefresh);
    runPipeline(event, rebalancePipeline);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 0, "Should NOT output 1 message: SLAVE-MASTER for node0");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testNoDuplicatedMaster() {
    String clusterName = "CLUSTER_" + _className + "_no_duplicated_master";

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    HelixManager manager = new DummyClusterManager(clusterName, accessor);
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    refreshClusterConfig(clusterName, accessor);

    final String resourceName = "testResource_no_duplicated_master";
    String[] resourceGroups = new String[] {
        resourceName
    };
    // ideal state: node0 is SLAVE, node1 is MASTER
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[] {
        0, 1
    }, resourceGroups, 1, 2);
    setupLiveInstances(clusterName, new int[] {
        0, 1
    });
    setupStateModel(clusterName);

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new IntermediateStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());

    // set node0 currentState to SLAVE, node1 currentState to MASTER
    // Helix will try to switch the state of the two instances, but it should not be two MASTER at the same time
    // so it should first transit M->S, then transit another instance S->M
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "SLAVE");
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", "session_1",
        "MASTER");

    runPipeline(event, dataRefresh);
    runPipeline(event, rebalancePipeline);
    MessageSelectionStageOutput msgSelOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: MASTER-SLAVE for localhost_1");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "MASTER");
    Assert.assertEquals(message.getToState(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_1");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  protected void setCurrentState(String clusterName, String instance, String resourceGroupName,
      String resourceKey, String sessionId, String state) {
    setCurrentState(clusterName, instance, resourceGroupName, resourceKey, sessionId, state, false);
  }

  private void setCurrentState(String clusterName, String instance, String resourceGroupName,
      String resourceKey, String sessionId, String state, boolean updateTimestamp) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    CurrentState curState = new CurrentState(resourceGroupName);
    curState.setState(resourceKey, state);
    curState.setSessionId(sessionId);
    curState.setStateModelDefRef("MasterSlave");
    if (updateTimestamp) {
      curState.setEndTime(resourceKey, System.currentTimeMillis());
    }
    accessor.setProperty(keyBuilder.currentState(instance, sessionId, resourceGroupName), curState);
  }

  private void refreshClusterConfig(String clusterName, HelixDataAccessor accessor) {
    accessor.setProperty(accessor.keyBuilder().clusterConfig(), new ClusterConfig(clusterName));
  }
}
