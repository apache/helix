package org.apache.helix.integration.controller;

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
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.api.status.ClusterManagementModeRequest;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.ClusterManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.ClusterStatus;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.util.MessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterFreezeMode extends ZkTestBase {
  private HelixManager _manager;
  private HelixDataAccessor _accessor;
  private String _clusterName;
  private int _numNodes;
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 3;
    _clusterName = "CLUSTER_" + TestHelper.getTestClassName();
    _participants = new MockParticipantManager[_numNodes];
    TestHelper.setupCluster(_clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        2, // partitions per resource
        _numNodes, // number of nodes
        3, // replicas
        "MasterSlave", true);

    _manager = HelixManagerFactory
        .getZKHelixManager(_clusterName, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _accessor = _manager.getHelixDataAccessor();

    // start controller
    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    _controller.syncStart();

    Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>() {
      {
        put("OFFLINE-SLAVE", TestHelper.setOf("TestDB0_0"));
      }
    };

    // start participants
    for (int i = 0; i < _numNodes; i++) {
      String instanceName = "localhost_" + (12918 + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _clusterName, instanceName);
      if (i == 0) {
        // Make TestDB0_0 be error state on participant_0
        _participants[i].setTransition(new ErrTransition(errPartitions));
      }
      _participants[i].syncStart();
    }

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, _clusterName));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() {
    _manager.disconnect();
    _controller.syncStop();
    Arrays.stream(_participants).forEach(ClusterManager::syncStop);
    deleteCluster(_clusterName);
  }

  /*
   * Tests below scenarios:
   * 1. cluster is in progress to freeze mode if there is a pending state transition message;
   * 2. after state transition is completed, cluster freeze mode is completed
   *
   * Also tests cluster status and management mode history recording.
   */
  @Test
  public void testEnableFreezeMode() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    // Not in freeze mode
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();
    PauseSignal pauseSignal = _accessor.getProperty(keyBuilder.pause());
    Assert.assertNull(pauseSignal);

    // Block state transition for participants[1]
    CountDownLatch latch = new CountDownLatch(1);
    _participants[1].setTransition(new BlockingTransition(latch));

    // Send a state transition message to participants[1]
    Resource resource = new Resource("TestDB0");
    resource.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
    Message message = MessageUtil
        .createStateTransitionMessage(_manager.getInstanceName(), _manager.getSessionId(), resource,
            "TestDB0_1", _participants[1].getInstanceName(), "SLAVE", "OFFLINE",
            _participants[1].getSessionId(), "MasterSlave");
    Assert.assertTrue(_accessor
        .updateProperty(keyBuilder.message(message.getTgtName(), message.getMsgId()), message));

    // Freeze cluster
    ClusterManagementModeRequest request = ClusterManagementModeRequest.newBuilder()
        .withClusterName(_clusterName)
        .withMode(ClusterManagementMode.Type.CLUSTER_FREEZE)
        .withReason(methodName)
        .build();
    _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);

    // Wait for all live instances are marked as frozen
    verifyLiveInstanceStatus(_participants, LiveInstance.LiveInstanceStatus.FROZEN);

    // Pending ST message exists
    Assert.assertTrue(
        _gZkClient.exists(keyBuilder.message(message.getTgtName(), message.getMsgId()).getPath()));

    // Even live instance status is marked as frozen, Cluster is in progress to cluster freeze
    // because there is a pending state transition message
    ClusterStatus expectedClusterStatus = new ClusterStatus();
    expectedClusterStatus.setManagementMode(ClusterManagementMode.Type.CLUSTER_FREEZE);
    expectedClusterStatus.setManagementModeStatus(ClusterManagementMode.Status.IN_PROGRESS);
    verifyClusterStatus(expectedClusterStatus);

    // Verify management mode history is empty
    ControllerHistory controllerHistory =
        _accessor.getProperty(_accessor.keyBuilder().controllerLeaderHistory());
    List<String> managementHistory = controllerHistory.getManagementModeHistory();
    Assert.assertTrue(managementHistory.isEmpty());

    // Unblock to finish state transition and delete the ST message
    latch.countDown();

    // Verify live instance status and cluster status
    verifyLiveInstanceStatus(_participants, LiveInstance.LiveInstanceStatus.FROZEN);

    expectedClusterStatus = new ClusterStatus();
    expectedClusterStatus.setManagementMode(ClusterManagementMode.Type.CLUSTER_FREEZE);
    expectedClusterStatus.setManagementModeStatus(ClusterManagementMode.Status.COMPLETED);
    verifyClusterStatus(expectedClusterStatus);

    // Verify management mode history
    Assert.assertTrue(TestHelper.verify(() -> {
      ControllerHistory tmpControllerHistory =
          _accessor.getProperty(keyBuilder.controllerLeaderHistory());
      List<String> tmpManagementHistory = tmpControllerHistory.getManagementModeHistory();
      if (tmpManagementHistory == null || tmpManagementHistory.isEmpty()) {
        return false;
      }
      // Should not have duplicate entries
      if (tmpManagementHistory.size() > 1) {
        return false;
      }
      String lastHistory = tmpManagementHistory.get(0);
      return lastHistory.contains("MODE=" + ClusterManagementMode.Type.CLUSTER_FREEZE)
          && lastHistory.contains("STATUS=" + ClusterManagementMode.Status.COMPLETED)
          && lastHistory.contains("REASON=" + methodName);
    }, TestHelper.WAIT_DURATION));
  }

  @Test(dependsOnMethods = "testEnableFreezeMode")
  public void testNewLiveInstanceAddedWhenFrozen() throws Exception {
    // Add a new live instance. Simulate an instance is rebooted and back to online
    String newInstanceName = "localhost_" + (12918 + _numNodes + 1);
    _gSetupTool.addInstancesToCluster(_clusterName, new String[]{newInstanceName});
    MockParticipantManager newParticipant =
        new MockParticipantManager(ZK_ADDR, _clusterName, newInstanceName);
    newParticipant.syncStart();

    // The new participant/live instance should be frozen by controller
    verifyLiveInstanceStatus(new MockParticipantManager[]{newParticipant},
        LiveInstance.LiveInstanceStatus.FROZEN);

    newParticipant.syncStop();
  }

  // Simulates instance is restarted and the in-memory status is gone.
  // When instance comes back alive, it'll reset state model, carry over
  // and set current state to init state.
  @Test(dependsOnMethods = "testNewLiveInstanceAddedWhenFrozen")
  public void testRestartParticipantWhenFrozen() throws Exception {
    String instanceName = _participants[1].getInstanceName();
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();
    List<CurrentState> originCurStates = _accessor
        .getChildValues(keyBuilder.currentStates(instanceName, _participants[1].getSessionId()),
            false);
    String oldSession = _participants[1].getSessionId();

    // Restart participants[1]
    _participants[1].syncStop();
    _participants[1] = new MockParticipantManager(ZK_ADDR, _participants[1].getClusterName(),
        instanceName);
    _participants[1].syncStart();

    Assert.assertTrue(TestHelper.verify(() ->
            _gZkClient.exists(keyBuilder.liveInstance(instanceName).getPath()),
        TestHelper.WAIT_DURATION));
    LiveInstance liveInstance = _accessor.getProperty(keyBuilder.liveInstance(instanceName));

    // New live instance ephemeral node
    Assert.assertEquals(liveInstance.getEphemeralOwner(), _participants[1].getSessionId());
    // Status is frozen because controller sends a freeze message.
    verifyLiveInstanceStatus(new MockParticipantManager[]{_participants[1]},
        LiveInstance.LiveInstanceStatus.FROZEN);

    // Old session current state is deleted because of current state carry-over
    Assert.assertTrue(TestHelper.verify(
        () -> !_gZkClient.exists(keyBuilder.currentStates(instanceName, oldSession).getPath()),
        TestHelper.WAIT_DURATION));

    // Current states are set to init states (OFFLINE)
    List<CurrentState> curStates = _accessor
        .getChildValues(keyBuilder.currentStates(instanceName, _participants[1].getSessionId()),
            false);
    Assert.assertEquals(curStates.size(), 1);
    Assert.assertTrue(TestHelper.verify(() -> {
      for (CurrentState cs : originCurStates) {
        String stateModelDefRef = cs.getStateModelDefRef();
        for (String partition : cs.getPartitionStateMap().keySet()) {
          StateModelDefinition stateModelDef =
              _accessor.getProperty(keyBuilder.stateModelDef(stateModelDefRef));
          String initState = stateModelDef.getInitialState();
          if (!initState.equals(curStates.get(0).getPartitionStateMap().get(partition))) {
            return false;
          }
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  // Partition reset is allowed when cluster is frozen
  @Test(dependsOnMethods = "testRestartParticipantWhenFrozen")
  public void testResetPartitionWhenFrozen() throws Exception {
    String instanceName = _participants[0].getInstanceName();
    // Remove errTransition
    _participants[0].setTransition(null);
    _gSetupTool.getClusterManagementTool().resetPartition(_clusterName, instanceName, "TestDB0",
        Collections.singletonList("TestDB0_0"));

    // Error partition is reset: ERROR -> OFFLINE
    Assert.assertTrue(TestHelper.verify(() -> {
      CurrentState currentState = _accessor.getProperty(_accessor.keyBuilder()
          .currentState(instanceName, _participants[0].getSessionId(), "TestDB0"));
      return "OFFLINE".equals(currentState.getPartitionStateMap().get("TestDB0_0"));
    }, TestHelper.WAIT_DURATION));
  }

  @Test(dependsOnMethods = "testResetPartitionWhenFrozen")
  public void testCreateResourceWhenFrozen() {
    // Add a new resource
    _gSetupTool.addResourceToCluster(_clusterName, "TestDB1", 2, "MasterSlave");
    _gSetupTool.rebalanceStorageCluster(_clusterName, "TestDB1", 3);

    // TestDB1 external view is empty
    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 1000, _clusterName, "TestDB1",
        TestHelper.setOf("localhost_12918", "localhost_12919", "localhost_12920"), ZK_ADDR);
  }

  @Test(dependsOnMethods = "testCreateResourceWhenFrozen")
  public void testUnfreezeCluster() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    // Unfreeze cluster
    ClusterManagementModeRequest request = ClusterManagementModeRequest.newBuilder()
        .withClusterName(_clusterName)
        .withMode(ClusterManagementMode.Type.NORMAL)
        .withReason(methodName)
        .build();
    _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);

    verifyLiveInstanceStatus(_participants, LiveInstance.LiveInstanceStatus.NORMAL);

    ClusterStatus expectedClusterStatus = new ClusterStatus();
    expectedClusterStatus.setManagementMode(ClusterManagementMode.Type.NORMAL);
    expectedClusterStatus.setManagementModeStatus(ClusterManagementMode.Status.COMPLETED);
    verifyClusterStatus(expectedClusterStatus);

    // Verify management mode history: NORMAL + COMPLETED
    Assert.assertTrue(TestHelper.verify(() -> {
      ControllerHistory history =
          _accessor.getProperty(_accessor.keyBuilder().controllerLeaderHistory());
      List<String> managementHistory = history.getManagementModeHistory();
      if (managementHistory == null || managementHistory.isEmpty()) {
        return false;
      }
      String lastHistory = managementHistory.get(managementHistory.size() - 1);
      return lastHistory.contains("MODE=" + ClusterManagementMode.Type.NORMAL)
          && lastHistory.contains("STATUS=" + ClusterManagementMode.Status.COMPLETED);
    }, TestHelper.WAIT_DURATION));

    // Verify cluster's normal rebalance ability after unfrozen.
    Assert.assertTrue(ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, _clusterName)));
  }

  private void verifyLiveInstanceStatus(MockParticipantManager[] participants,
      LiveInstance.LiveInstanceStatus status) throws Exception {
    final PropertyKey.Builder keyBuilder = _accessor.keyBuilder();
    Assert.assertTrue(TestHelper.verify(() -> {
      for (MockParticipantManager participant : participants) {
        String instanceName = participant.getInstanceName();
        LiveInstance liveInstance = _accessor.getProperty(keyBuilder.liveInstance(instanceName));
        if (status != liveInstance.getStatus()) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  private void verifyClusterStatus(ClusterStatus expectedMode) throws Exception {
    final PropertyKey statusPropertyKey = _accessor.keyBuilder().clusterStatus();
    TestHelper.verify(() -> {
      ClusterStatus clusterStatus = _accessor.getProperty(statusPropertyKey);
      return clusterStatus != null
          && expectedMode.getManagementMode().equals(clusterStatus.getManagementMode())
          && expectedMode.getManagementModeStatus().equals(clusterStatus.getManagementModeStatus());
    }, TestHelper.WAIT_DURATION);
  }

  private static class BlockingTransition extends MockTransition {
    private static final Logger LOG = LoggerFactory.getLogger(BlockingTransition.class);
    private final CountDownLatch _countDownLatch;

    private BlockingTransition(CountDownLatch countDownLatch) {
      _countDownLatch = countDownLatch;
    }

    @Override
    public void doTransition(Message message, NotificationContext context)
        throws InterruptedException {
      LOG.info("Transition is blocked");
      _countDownLatch.await();
      LOG.info("Transition is completed");
    }
  }
}
