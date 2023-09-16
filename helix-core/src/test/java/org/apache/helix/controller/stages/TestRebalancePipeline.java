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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.stages.resource.ResourceMessageDispatchStage;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRebalancePipeline extends ZkTestBase {
  private final String _className = getShortClassName();

  @Test
  public void testDuplicateMsg() throws Exception {
    String clusterName = "CLUSTER_" + _className + "_dup";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    refreshClusterConfig(clusterName, accessor);
    HelixManager manager =
        new DummyClusterManager(clusterName, accessor, Long.toHexString(_gZkClient.getSessionId()));
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    ResourceControllerDataProvider dataCache = new ResourceControllerDataProvider();
    // The AsyncTasksThreadPool needs to be set, otherwise to start pending message cleanup job
    // will throw NPE and stop the pipeline. TODO: https://github.com/apache/helix/issues/1158
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    dataCache.setAsyncTasksThreadPool(executorService);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), dataCache);

    final String resourceName = "testResource_dup";
    String[] resourceGroups = new String[] {
        resourceName
    };
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    setupIdealState(clusterName, new int[] {
        0
    }, resourceGroups, 1, 1);
    List<LiveInstance> liveInstances = setupLiveInstances(clusterName, new int[] {
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
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new IntermediateStateCalcStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new ResourceMessageDispatchStage());

    // round1: set node0 currentState to OFFLINE
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", liveInstances.get(0).getEphemeralOwner(),
        "OFFLINE");

    runPipeline(event, dataRefresh, false);
    runPipeline(event, rebalancePipeline, false);
    MessageOutput msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: OFFLINE-SLAVE for node0");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    // round2: updates node0 currentState to SLAVE but keep the
    // message, make sure controller should not wait for the message to be deleted, but should
    // send out a S -> M message to node0
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0",
        liveInstances.get(0).getEphemeralOwner(), "SLAVE");

    runPipeline(event, dataRefresh, false);
    refreshClusterConfig(clusterName, accessor);

    Pipeline computationPipeline = new Pipeline();
    computationPipeline.addStage(new ResourceComputationStage());
    computationPipeline.addStage(new CurrentStateComputationStage());

    Pipeline messagePipeline = new Pipeline();
    messagePipeline.addStage(new BestPossibleStateCalcStage());
    messagePipeline.addStage(new MessageGenerationPhase());
    messagePipeline.addStage(new MessageSelectionStage());
    messagePipeline.addStage(new IntermediateStateCalcStage());
    messagePipeline.addStage(new MessageThrottleStage());
    messagePipeline.addStage(new ResourceMessageDispatchStage());

    runPipeline(event, computationPipeline, false);

    Map<String, Map<String, Message>> staleMessages = dataCache.getStaleMessages();
    Assert.assertEquals(staleMessages.size(), 1);
    Assert.assertTrue(staleMessages.containsKey("localhost_0"));
    Assert.assertTrue(staleMessages.get("localhost_0").containsKey(message.getMsgId()));

    runPipeline(event, messagePipeline, false);

    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: SLAVE-MASTER for node0");
    Assert.assertTrue(messages.get(0).getTgtName().equalsIgnoreCase("localhost_0"));
    Assert.assertTrue(messages.get(0).getFromState().equalsIgnoreCase("SLAVE"));
    Assert.assertTrue(messages.get(0).getToState().equalsIgnoreCase("MASTER"));

    Thread.sleep(2 * MessageGenerationPhase.DEFAULT_OBSELETE_MSG_PURGE_DELAY);
    runPipeline(event, dataRefresh, false);

    // Verify the stale message should be deleted
    Assert.assertTrue(TestHelper.verify(() -> {
      if (dataCache.getStaleMessages().size() != 0) {
        return false;
      }
      return true;
    }, TestHelper.WAIT_DURATION));

    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
    executorService.shutdown();
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMsgTriggeredRebalance() throws Exception {
    String clusterName = "CLUSTER_" + _className + "_msgTrigger";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
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
    List<LiveInstance> liveInstances = setupLiveInstances(clusterName, new int[] {
        0, 1
    });

    long msgPurgeDelay = MessageGenerationPhase.DEFAULT_OBSELETE_MSG_PURGE_DELAY;

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // round1: controller sends O->S to both node0 and node1
    Builder keyBuilder = accessor.keyBuilder();
    Assert.assertTrue(TestHelper.verify(() -> {
      for (LiveInstance liveInstance : liveInstances) {
        List<String> messages =
            accessor.getChildNames(keyBuilder.messages(liveInstance.getInstanceName()));
        if (messages.size() < 1) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));

    // round2: node0 and node1 update current states but not removing messages
    // Since controller's rebalancer pipeline will GC pending messages after timeout, and both hosts
    // update current states to SLAVE, controller will send out rebalance message to
    // have one host to become master
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", liveInstances.get(0).getEphemeralOwner(),
        "SLAVE", true);
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", liveInstances.get(1).getEphemeralOwner(),
        "SLAVE", true);

    // Controller has timeout > 1sec, so within 1s, controller should not have GCed message
    Assert.assertTrue(msgPurgeDelay > 1000);
    Assert.assertFalse(TestHelper.verify(() -> {
      for (LiveInstance liveInstance : liveInstances) {
        List<String> messages =
            accessor.getChildNames(keyBuilder.messages(liveInstance.getInstanceName()));
        if (messages.size() >= 1) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));

    // After another purge delay, controller should cleanup messages and continue to rebalance
    Thread.sleep(msgPurgeDelay);
    // Manually trigger another rebalance by touching current state
    List<Message> allMsgs = new ArrayList<>();
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0",
        liveInstances.get(0).getEphemeralOwner(), "SLAVE");
    Assert.assertTrue(TestHelper.verify(() -> {
      allMsgs.clear();
      for (LiveInstance liveInstance : liveInstances) {
        allMsgs.addAll(accessor.getChildValues(keyBuilder.messages(liveInstance.getInstanceName()),
            true));
      }
      if (allMsgs.size() != 1 || !allMsgs.get(0).getToState().equals("MASTER") || !allMsgs.get(0)
          .getFromState().equals("SLAVE")) {
        return false;
      }
      return true;
    }, TestHelper.WAIT_DURATION));

    // round3: node0 changes state to master, but failed to delete message,
    // controller will clean it up
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", liveInstances.get(0).getEphemeralOwner(),
        "MASTER", true);
    Thread.sleep(msgPurgeDelay);
    // touch current state to trigger rebalance
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", liveInstances.get(0).getEphemeralOwner(),
        "MASTER", false);
    Assert.assertTrue(TestHelper.verify(() -> accessor.getChildNames(keyBuilder.messages("localhost_0")).isEmpty(), 2000));

    // round4: node0 has duplicated but valid message, i.e. there is a P2P message sent to it
    // due to error in the triggered pipeline, controller should remove duplicated message
    // immediately as the partition has became master 3 sec ago (there is already a timeout)
    Message sourceMsg = allMsgs.get(0);
    Message dupMsg = new Message(sourceMsg.getMsgType(), UUID.randomUUID().toString());
    dupMsg.getRecord().setSimpleFields(sourceMsg.getRecord().getSimpleFields());
    dupMsg.getRecord().setListFields(sourceMsg.getRecord().getListFields());
    dupMsg.getRecord().setMapFields(sourceMsg.getRecord().getMapFields());
    accessor.setProperty(dupMsg.getKey(accessor.keyBuilder(), dupMsg.getTgtName()), dupMsg);
    Assert.assertTrue(TestHelper.verify(() -> accessor.getChildNames(keyBuilder.messages("localhost_0")).isEmpty(), 1500));

    // round5: node0 has completely invalid message, controller should immediately delete it
    dupMsg.setFromState("SLAVE");
    dupMsg.setToState("OFFLINE");
    accessor.setProperty(dupMsg.getKey(accessor.keyBuilder(), dupMsg.getTgtName()), dupMsg);
    Assert.assertTrue(TestHelper.verify(() -> accessor.getChildNames(keyBuilder.messages("localhost_0")).isEmpty(), 1500));

    if (controller.isConnected()) {
      controller.syncStop();
    }
    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testChangeIdealStateWithPendingMsg() throws Exception {
    String clusterName = "CLUSTER_" + _className + "_pending";
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    admin.addCluster(clusterName);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    HelixManager manager =
        new DummyClusterManager(clusterName, accessor, Long.toHexString(_gZkClient.getSessionId()));
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);

    ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
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
    List<LiveInstance> liveInstances = setupLiveInstances(clusterName, new int[] {
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
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new IntermediateStateCalcStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new ResourceMessageDispatchStage());

    // round1: set node0 currentState to OFFLINE and node1 currentState to SLAVE
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", liveInstances.get(0).getEphemeralOwner(),
        "OFFLINE");

    runPipeline(event, dataRefresh, false);
    runPipeline(event, rebalancePipeline, false);
    MessageOutput msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: OFFLINE-SLAVE for node0");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    // round2: drop resource, but keep the
    // message, make sure controller should not send O->DROPPED until O->S is done
    admin.dropResource(clusterName, resourceName);
    List<IdealState> idealStates = accessor.getChildValues(accessor.keyBuilder().idealStates(), true);
    cache.setIdealStates(idealStates);

    runPipeline(event, dataRefresh, false);
    cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    cache.setClusterConfig(new ClusterConfig(clusterName));
    runPipeline(event, rebalancePipeline, false);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 0,
        "Should not output only 1 message: OFFLINE->DROPPED for localhost_0");

    // round3: remove O->S message for localhost_0, localhost_0 still in OFFLINE
    // controller should now send O->DROPPED to localhost_0
    Builder keyBuilder = accessor.keyBuilder();
    List<String> msgIds = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    accessor.removeProperty(keyBuilder.message("localhost_0", msgIds.get(0)));
    runPipeline(event, dataRefresh, false);
    runPipeline(event, rebalancePipeline, false);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1,
        "Should output 1 message: OFFLINE->DROPPED for localhost_0");
    message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "DROPPED");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMasterXfer() throws Exception {
    String clusterName = "CLUSTER_" + _className + "_xfer";

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    HelixManager manager =
        new DummyClusterManager(clusterName, accessor, Long.toHexString(_gZkClient.getSessionId()));
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
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
    List<LiveInstance> liveInstances = setupLiveInstances(clusterName, new int[] {
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
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new IntermediateStateCalcStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new ResourceMessageDispatchStage());

    // round1: set node1 currentState to SLAVE
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", liveInstances.get(0).getEphemeralOwner(),
        "SLAVE");

    runPipeline(event, dataRefresh, false);
    runPipeline(event, rebalancePipeline, false);
    MessageOutput msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
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

    runPipeline(event, dataRefresh, false);
    runPipeline(event, rebalancePipeline, false);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 0, "Should NOT output 1 message: SLAVE-MASTER for node0");

    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testNoDuplicatedMaster() throws Exception {
    String clusterName = "CLUSTER_" + _className + "_no_duplicated_master";

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    HelixManager manager =
        new DummyClusterManager(clusterName, accessor, Long.toHexString(_gZkClient.getSessionId()));
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
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
    List<LiveInstance> liveInstances = setupLiveInstances(clusterName, new int[] {
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
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new IntermediateStateCalcStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new ResourceMessageDispatchStage());

    // set node0 currentState to SLAVE, node1 currentState to MASTER
    // Helix will try to switch the state of the two instances, but it should not be two MASTER at
    // the same time
    // so it should first transit M->S, then transit another instance S->M
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", liveInstances.get(0).getEphemeralOwner(),
        "SLAVE");
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", liveInstances.get(1).getEphemeralOwner(),
        "MASTER");

    runPipeline(event, dataRefresh, false);
    runPipeline(event, rebalancePipeline, false);
    MessageOutput msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1,
        "Should output 1 message: MASTER-SLAVE for localhost_1");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "MASTER");
    Assert.assertEquals(message.getToState(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_1");

    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /*
   * Tests if controller loses leadership when pipeline is still running, messages should not be
   * sent to participants. No message sent on leadership loss prevents double masters issue.
   * This test simulates that controller leader's zk session changes after ReadClusterDataStage
   * and so state transition messages are not sent out and stage exception is thrown to terminate
   * the pipeline.
   */
  @Test
  public void testNoMessageSentOnControllerLeadershipLoss() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    String clusterName = _className + "_" + methodName;

    final String resourceName = "testResource_" + methodName;
    final String partitionName = resourceName + "_0";
    String[] resourceGroups = new String[] {
        resourceName
    };

    // ideal state: localhost_0 is MASTER
    // replica=1 means 1 master
    setupIdealState(clusterName, new int[] {
        0
    }, resourceGroups, 1, 1);
    List<LiveInstance> liveInstances = setupLiveInstances(clusterName, new int[] {
        0
    });
    setupStateModel(clusterName);

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    DummyClusterManager manager =
        new DummyClusterManager(clusterName, accessor, Long.toHexString(_gZkClient.getSessionId()));
    ClusterEvent event = new ClusterEvent(clusterName, ClusterEventType.OnDemandRebalance);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
    event.addAttribute(AttributeName.EVENT_SESSION.name(), Optional.of(manager.getSessionId()));
    refreshClusterConfig(clusterName, accessor);

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new IntermediateStateCalcStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new ResourceMessageDispatchStage());

    // set node0 currentState to SLAVE, node1 currentState to MASTER
    // Helix will try to switch the state of the instance, so it should transit S->M
    setCurrentState(clusterName, "localhost_0", resourceName, partitionName,
        liveInstances.get(0).getEphemeralOwner(), "SLAVE");

    runPipeline(event, dataRefresh, false);

    // After data refresh, controller loses leadership and its session id changes.
    manager.setSessionId(manager.getSessionId() + "_new");

    try {
      // Because leader loses leadership, StageException should be thrown and message is not sent.
      runPipeline(event, rebalancePipeline, true);
      Assert.fail("StageException should be thrown because controller leader session changed.");
    } catch (StageException e) {
      Assert.assertTrue(
          e.getMessage().matches("Event session doesn't match controller .* Expected session: .*"));
    }

    // Verify the ST message not being sent out is the expected one to
    // transit the replica SLAVE->MASTER
    MessageOutput msgThrottleOutput = event.getAttribute(AttributeName.MESSAGES_THROTTLE.name());
    List<Message> messages =
        msgThrottleOutput.getMessages(resourceName, new Partition(partitionName));
    Assert.assertEquals(messages.size(), 1,
        "Should output 1 message: SLAVE->MASTER for localhost_0");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "SLAVE");
    Assert.assertEquals(message.getToState(), "MASTER");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    deleteLiveInstances(clusterName);
    deleteCluster(clusterName);
  }

  protected void setCurrentState(String clusterName, String instance, String resourceGroupName,
      String resourceKey, String sessionId, String state) {
    setCurrentState(clusterName, instance, resourceGroupName, resourceKey, sessionId, state, false);
  }

  private void setCurrentState(String clusterName, String instance, String resourceGroupName,
      String resourceKey, String sessionId, String state, boolean updateTimestamp) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
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
