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

import java.util.Date;
import java.util.List;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.testutil.HelixTestUtil;
import org.apache.helix.testutil.TestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRebalancePipeline extends ZkTestBase {
  @Test
  public void testDuplicateMsg() {

    String clusterName = TestUtil.getTestName();
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupEmptyCluster(_zkclient, clusterName);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);

    HelixManager manager = new DummyClusterManager(clusterName, accessor);
    ClusterEvent event = new ClusterEvent("testEvent");
    event.addAttribute("helixmanager", manager);

    final String resourceName = "testResource_dup";
    String[] resourceGroups = new String[] {
      resourceName
    };
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    HelixTestUtil.setupIdealState(_baseAccessor, clusterName, new int[] {
        0, 1
    }, resourceGroups, 1, 2);
    HelixTestUtil.setupInstances(_baseAccessor, clusterName, new int[] {
        0, 1
    });
    HelixTestUtil.setupLiveInstances(_baseAccessor, clusterName, new int[] {
        0, 1
    });
    HelixTestUtil.setupStateModel(_baseAccessor, clusterName);

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationStage());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());

    // round1: set node0 currentState to OFFLINE and node1 currentState to OFFLINE
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "OFFLINE");
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", "session_1",
        "SLAVE");

    HelixTestUtil.runPipeline(event, dataRefresh);
    HelixTestUtil.runPipeline(event, rebalancePipeline);
    MessageOutput msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    List<Message> messages =
        msgSelOutput.getMessages(ResourceId.from(resourceName),
            PartitionId.from(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: OFFLINE-SLAVE for node0");
    Message message = messages.get(0);
    Assert.assertEquals(message.getTypedFromState().toString(), "OFFLINE");
    Assert.assertEquals(message.getTypedToState().toString(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    // round2: updates node0 currentState to SLAVE but keep the
    // message, make sure controller should not send S->M until removal is done
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_1",
        "SLAVE");

    HelixTestUtil.runPipeline(event, dataRefresh);
    HelixTestUtil.runPipeline(event, rebalancePipeline);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    messages =
        msgSelOutput.getMessages(ResourceId.from(resourceName),
            PartitionId.from(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 0, "Should NOT output 1 message: SLAVE-MASTER for node1");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testMsgTriggeredRebalance() throws Exception {
    String clusterName = TestUtil.getTestName();
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupEmptyCluster(_zkclient, clusterName);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);

    final String resourceName = "testResource_dup";
    String[] resourceGroups = new String[] {
      resourceName
    };

    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    HelixTestUtil.setupIdealState(_baseAccessor, clusterName, new int[] {
        0, 1
    }, resourceGroups, 1, 2);
    HelixTestUtil.setupStateModel(_baseAccessor, clusterName);
    HelixTestUtil.setupInstances(_baseAccessor, clusterName, new int[] {
        0, 1
    });
    HelixTestUtil.setupLiveInstances(_baseAccessor, clusterName, new int[] {
        0, 1
    });

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // round1: controller sends O->S to both node0 and node1
    Thread.sleep(1000);

    Builder keyBuilder = accessor.keyBuilder();
    List<String> messages = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    Assert.assertEquals(messages.size(), 1);
    messages = accessor.getChildNames(keyBuilder.messages("localhost_1"));
    Assert.assertEquals(messages.size(), 1);

    // round2: node0 and node1 update current states but not removing messages
    // controller's rebalance pipeline should be triggered but since messages are not
    // removed
    // no new messages will be sent
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "SLAVE");
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", "session_1",
        "SLAVE");
    Thread.sleep(1000);
    messages = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    Assert.assertEquals(messages.size(), 1);

    messages = accessor.getChildNames(keyBuilder.messages("localhost_1"));
    Assert.assertEquals(messages.size(), 1);

    // round3: node0 removes message and controller's rebalance pipeline should be
    // triggered
    // and sends S->M to node0
    messages = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    accessor.removeProperty(keyBuilder.message("localhost_0", messages.get(0)));
    Thread.sleep(1000);

    messages = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    Assert.assertEquals(messages.size(), 1);
    ZNRecord msg =
        accessor.getProperty(keyBuilder.message("localhost_0", messages.get(0))).getRecord();
    String toState = msg.getSimpleField(Attributes.TO_STATE.toString());
    Assert.assertEquals(toState, "MASTER");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testChangeIdealStateWithPendingMsg() {
    String clusterName = TestUtil.getTestName();
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupEmptyCluster(_zkclient, clusterName);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    HelixManager manager = new DummyClusterManager(clusterName, accessor);
    ClusterEvent event = new ClusterEvent("testEvent");
    event.addAttribute("helixmanager", manager);

    ClusterDataCache cache = new ClusterDataCache();
    event.addAttribute("ClusterDataCache", cache);

    final String resourceName = "testResource_pending";
    String[] resourceGroups = new String[] {
      resourceName
    };
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    HelixTestUtil.setupIdealState(_baseAccessor, clusterName, new int[] {
        0, 1
    }, resourceGroups, 1, 2);
    HelixTestUtil.setupInstances(_baseAccessor, clusterName, new int[] {
        0, 1
    });
    HelixTestUtil.setupLiveInstances(_baseAccessor, clusterName, new int[] {
        0, 1
    });
    HelixTestUtil.setupStateModel(_baseAccessor, clusterName);

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationStage());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());

    // round1: set node0 currentState to OFFLINE and node1 currentState to SLAVE
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "OFFLINE");
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", "session_1",
        "SLAVE");

    HelixTestUtil.runPipeline(event, dataRefresh);
    HelixTestUtil.runPipeline(event, rebalancePipeline);
    MessageOutput msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    List<Message> messages =
        msgSelOutput.getMessages(ResourceId.from(resourceName),
            PartitionId.from(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: OFFLINE-SLAVE for node0");
    Message message = messages.get(0);
    Assert.assertEquals(message.getTypedFromState().toString(), "OFFLINE");
    Assert.assertEquals(message.getTypedToState().toString(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    // round2: drop resource, but keep the
    // message, make sure controller should not send O->DROPPEDN until O->S is done
    HelixAdmin admin = new ZKHelixAdmin(_zkclient);
    admin.dropResource(clusterName, resourceName);
    List<IdealState> idealStates = accessor.getChildValues(accessor.keyBuilder().idealStates());
    cache.setIdealStates(idealStates);

    HelixTestUtil.runPipeline(event, dataRefresh);
    HelixTestUtil.runPipeline(event, rebalancePipeline);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    messages =
        msgSelOutput.getMessages(ResourceId.from(resourceName),
            PartitionId.from(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1,
        "Should output only 1 message: OFFLINE->DROPPED for localhost_1");

    message = messages.get(0);
    Assert.assertEquals(message.getTypedFromState().toString(), "SLAVE");
    Assert.assertEquals(message.getTypedToState().toString(), "OFFLINE");
    Assert.assertEquals(message.getTgtName(), "localhost_1");

    // round3: remove O->S for localhost_0, controller should now send O->DROPPED to
    // localhost_0
    Builder keyBuilder = accessor.keyBuilder();
    List<String> msgIds = accessor.getChildNames(keyBuilder.messages("localhost_0"));
    accessor.removeProperty(keyBuilder.message("localhost_0", msgIds.get(0)));
    HelixTestUtil.runPipeline(event, dataRefresh);
    HelixTestUtil.runPipeline(event, rebalancePipeline);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    messages =
        msgSelOutput.getMessages(ResourceId.from(resourceName),
            PartitionId.from(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1,
        "Should output 1 message: OFFLINE->DROPPED for localhost_0");
    message = messages.get(0);
    Assert.assertEquals(message.getTypedFromState().toString(), "OFFLINE");
    Assert.assertEquals(message.getTypedToState().toString(), "DROPPED");
    Assert.assertEquals(message.getTgtName(), "localhost_0");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testMasterXfer() {
    String clusterName = TestUtil.getTestName();

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupEmptyCluster(_zkclient, clusterName);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    HelixManager manager = new DummyClusterManager(clusterName, accessor);
    ClusterEvent event = new ClusterEvent("testEvent");
    event.addAttribute("helixmanager", manager);

    final String resourceName = "testResource_xfer";
    String[] resourceGroups = new String[] {
      resourceName
    };
    // ideal state: node0 is MASTER, node1 is SLAVE
    // replica=2 means 1 master and 1 slave
    HelixTestUtil.setupIdealState(_baseAccessor, clusterName, new int[] {
        0, 1
    }, resourceGroups, 1, 2);
    HelixTestUtil.setupInstances(_baseAccessor, clusterName, new int[] {
      1
    });
    HelixTestUtil.setupLiveInstances(_baseAccessor, clusterName, new int[] {
      1
    });
    HelixTestUtil.setupStateModel(_baseAccessor, clusterName);

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationStage());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new MessageThrottleStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());

    // round1: set node1 currentState to SLAVE
    setCurrentState(clusterName, "localhost_1", resourceName, resourceName + "_0", "session_1",
        "SLAVE");

    HelixTestUtil.runPipeline(event, dataRefresh);
    HelixTestUtil.runPipeline(event, rebalancePipeline);
    MessageOutput msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    List<Message> messages =
        msgSelOutput.getMessages(ResourceId.from(resourceName),
            PartitionId.from(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 1, "Should output 1 message: SLAVE-MASTER for node1");
    Message message = messages.get(0);
    Assert.assertEquals(message.getTypedFromState().toString(), "SLAVE");
    Assert.assertEquals(message.getTypedToState().toString(), "MASTER");
    Assert.assertEquals(message.getTgtName(), "localhost_1");

    // round2: updates node0 currentState to SLAVE but keep the
    // message, make sure controller should not send S->M until removal is done
    HelixTestUtil.setupInstances(_baseAccessor, clusterName, new int[] {
      0
    });
    HelixTestUtil.setupLiveInstances(_baseAccessor, clusterName, new int[] {
      0
    });
    setCurrentState(clusterName, "localhost_0", resourceName, resourceName + "_0", "session_0",
        "SLAVE");

    HelixTestUtil.runPipeline(event, dataRefresh);
    HelixTestUtil.runPipeline(event, rebalancePipeline);
    msgSelOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    messages =
        msgSelOutput.getMessages(ResourceId.from(resourceName),
            PartitionId.from(resourceName + "_0"));
    Assert.assertEquals(messages.size(), 0, "Should NOT output 1 message: SLAVE-MASTER for node0");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  private void setCurrentState(String clusterName, String instance, String resourceGroupName,
      String resourceKey, String sessionId, String state) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();

    CurrentState curState = new CurrentState(resourceGroupName);
    curState.setState(PartitionId.from(resourceKey), State.from(state));
    curState.setSessionId(SessionId.from(sessionId));
    curState.setStateModelDefRef("MasterSlave");
    accessor.setProperty(keyBuilder.currentState(instance, sessionId, resourceGroupName), curState);
  }

}
