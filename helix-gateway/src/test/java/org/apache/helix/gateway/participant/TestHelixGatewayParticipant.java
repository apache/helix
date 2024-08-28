package org.apache.helix.gateway.participant;

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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.gateway.api.service.HelixGatewayServiceChannel;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardChangeRequests;


public class TestHelixGatewayParticipant extends ZkTestBase {
  private static final String CLUSTER_NAME = TestHelixGatewayParticipant.class.getSimpleName();
  private static final int START_NUM_NODE = 3;
  private static final String TEST_DB = "TestDB";
  private static final String TEST_STATE_MODEL = "OnlineOffline";
  private static final String CONTROLLER_PREFIX = "controller";
  private static final String PARTICIPANT_PREFIX = "participant";

  private ZkHelixClusterVerifier _clusterVerifier;
  private ClusterControllerManager _controller;
  private int _nextStartPort = 12000;
  private final List<HelixGatewayParticipant> _participants = Lists.newArrayList();
  private final Map<String, ShardChangeRequests> _pendingMessageMap = new ConcurrentHashMap<>();
  private final AtomicInteger _onDisconnectCallbackCount = new AtomicInteger();

  @BeforeClass
  public void beforeClass() {
    // Set up the Helix cluster
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord().setSimpleField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Start initial participants
    for (int i = 0; i < START_NUM_NODE; i++) {
      addParticipant();
    }

    // Start the controller
    String controllerName = CONTROLLER_PREFIX + '_' + CLUSTER_NAME;
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Enable best possible assignment persistence
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
  }

  @AfterClass
  public void afterClass() {
    // Clean up by disconnecting the controller and participants
    _controller.disconnect();
    for (HelixGatewayParticipant participant : _participants) {
      participant.disconnect();
    }
  }

  /**
   * Add a participant with a specific initial state map.
   */
  private HelixGatewayParticipant addParticipant(String participantName,
      Map<String, Map<String, String>> initialShardMap) {
    HelixGatewayParticipant participant =
        new HelixGatewayParticipant.Builder(new MockHelixGatewayServiceChannel(_pendingMessageMap), participantName,
            CLUSTER_NAME, ZK_ADDR, _onDisconnectCallbackCount::incrementAndGet).addMultiTopStateStateModelDefinition(
            TEST_STATE_MODEL).setInitialShardState(initialShardMap).build();
    _participants.add(participant);
    return participant;
  }

  /**
   * Add a participant with an empty initial state map.
   */
  private HelixGatewayParticipant addParticipant() {
    String participantName = PARTICIPANT_PREFIX + "_" + _nextStartPort++;
    return addParticipant(participantName, Collections.emptyMap());
  }

  /**
   * Remove a participant from the cluster.
   */
  private void deleteParticipant(HelixGatewayParticipant participant) {
    participant.disconnect();
    _participants.remove(participant);
  }

  /**
   * Add a participant to the IdealState's preference list.
   */
  private void addToPreferenceList(HelixGatewayParticipant participant) {
    IdealState idealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    idealState.getPreferenceLists()
        .values()
        .forEach(preferenceList -> preferenceList.add(participant.getInstanceName()));
    idealState.setReplicas(String.valueOf(Integer.parseInt(idealState.getReplicas()) + 1));
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, TEST_DB, idealState);
  }

  /**
   * Remove a participant from the IdealState's preference list.
   */
  private void removeFromPreferenceList(HelixGatewayParticipant participant) {
    IdealState idealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    idealState.getPreferenceLists()
        .values()
        .forEach(preferenceList -> preferenceList.remove(participant.getInstanceName()));
    idealState.setReplicas(String.valueOf(Integer.parseInt(idealState.getReplicas()) - 1));
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, TEST_DB, idealState);
  }

  /**
   * Create a test database in the cluster with a semi-automatic state model.
   */
  private void createDB() {
    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, TEST_DB,
        _participants.stream().map(HelixGatewayParticipant::getInstanceName).collect(Collectors.toList()),
        TEST_STATE_MODEL, 1, _participants.size());

    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setResources(new HashSet<>(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)))
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
  }

  /**
   * Retrieve a pending message for a specific participant.
   */
  private ShardChangeRequests getPendingMessage(String instanceName) {
    return _pendingMessageMap.get(instanceName);
  }

  /**
   * Process the pending message for a participant.
   */
  private void processPendingMessage(HelixGatewayParticipant participant, boolean isSuccess, String toState) {
    ShardChangeRequests requests = _pendingMessageMap.remove(participant.getInstanceName());

    participant.completeStateTransition(requests.getRequest(0).getResourceName(),requests.getRequest(0).getShardName(),
        isSuccess ? toState : "WRONG_STATE");
  }

  /**
   * Get the current state of a Helix shard.
   */
  private String getHelixCurrentState(String instanceName, String resourceName, String shardId) {
    return _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, resourceName)
        .getStateMap(shardId)
        .getOrDefault(instanceName, HelixGatewayParticipant.UNASSIGNED_STATE);
  }

  /**
   * Verify that all specified participants have pending messages.
   */
  private void verifyPendingMessages(List<HelixGatewayParticipant> participants) throws Exception {
    Assert.assertTrue(TestHelper.verify(
        () -> participants.stream().allMatch(participant -> getPendingMessage(participant.getInstanceName()) != null),
        TestHelper.WAIT_DURATION));
  }

  /**
   * Verify that the gateway state matches the Helix state for all participants.
   */
  private void verifyGatewayStateMatchesHelixState() throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> _participants.stream().allMatch(participant -> {
      String instanceName = participant.getInstanceName();
      for (String resourceName : _gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)) {
        for (String shardId : _gSetupTool.getClusterManagementTool()
            .getResourceIdealState(CLUSTER_NAME, resourceName)
            .getPartitionSet()) {
          String helixCurrentState = getHelixCurrentState(instanceName, resourceName, shardId);
          if (!participant.getCurrentState(resourceName, shardId).equals(helixCurrentState)) {
            return false;
          }
        }
      }
      return true;
    }), TestHelper.WAIT_DURATION));
  }

  /**
   * Verify that all shards for a given instance are in a specific state.
   */
  private void verifyHelixPartitionStates(String instanceName, String state) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      for (String resourceName : _gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)) {
        for (String shardId : _gSetupTool.getClusterManagementTool()
            .getResourceIdealState(CLUSTER_NAME, resourceName)
            .getPartitionSet()) {
          if (!getHelixCurrentState(instanceName, resourceName, shardId).equals(state)) {
            return false;
          }
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  @Test
  public void testProcessStateTransitionMessageSuccess() throws Exception {
    createDB();
    verifyPendingMessages(_participants);

    // Verify that all pending messages have the toState "ONLINE"
    for (HelixGatewayParticipant participant : _participants) {
     HelixGatewayServiceOuterClass.SingleShardChangeRequest request = getPendingMessage(participant.getInstanceName()).getRequest(0);
      Assert.assertNotNull(request);
      Assert.assertEquals(request.getTargetState(), "ONLINE");
    }

    // Process all pending messages successfully
    for (HelixGatewayParticipant participant : _participants) {
      processPendingMessage(participant, true, "ONLINE");
    }

    // Verify that the cluster converges and all states are "ONLINE"
    Assert.assertTrue(_clusterVerifier.verify());
    verifyGatewayStateMatchesHelixState();
  }

  @Test(dependsOnMethods = "testProcessStateTransitionMessageSuccess")
  public void testProcessStateTransitionMessageFailure() throws Exception {
    // Add a new participant and include it in the preference list
    HelixGatewayParticipant participant = addParticipant();
    addToPreferenceList(participant);
    verifyPendingMessages(List.of(participant));

    // Verify the pending message has the toState "ONLINE"
    HelixGatewayServiceOuterClass.SingleShardChangeRequest request = getPendingMessage(participant.getInstanceName()).getRequest(0);
    Assert.assertNotNull(request);
    Assert.assertEquals(request.getTargetState(), "ONLINE");

    // Process the message with failure
    processPendingMessage(participant, false, "ONLINE");

    // Verify that the cluster converges and states reflect the failure (e.g., "OFFLINE")
    Assert.assertTrue(_clusterVerifier.verify());
    verifyGatewayStateMatchesHelixState();

    // Remove the participant from the preference list and delete it
    removeFromPreferenceList(participant);
    deleteParticipant(participant);
    Assert.assertTrue(_clusterVerifier.verify());
  }

  @Test(dependsOnMethods = "testProcessStateTransitionMessageFailure")
  public void testProcessStateTransitionAfterReconnect() throws Exception {
    // Remove the first participant
    HelixGatewayParticipant participant = _participants.get(0);
    deleteParticipant(participant);

    // Verify the Helix state transitions to "UNASSIGNED_STATE" for the participant
    verifyHelixPartitionStates(participant.getInstanceName(), HelixGatewayParticipant.UNASSIGNED_STATE);

    // Re-add the participant with its initial state
    addParticipant(participant.getInstanceName(), participant.getShardStateMap());
    Assert.assertTrue(_clusterVerifier.verify());

    // Verify the Helix state is "ONLINE"
    verifyHelixPartitionStates(participant.getInstanceName(), "ONLINE");
  }

  @Test(dependsOnMethods = "testProcessStateTransitionAfterReconnect")
  public void testProcessStateTransitionAfterReconnectAfterDroppingPartition() throws Exception {
    // Remove the first participant and verify state
    HelixGatewayParticipant participant = _participants.get(0);
    deleteParticipant(participant);
    verifyHelixPartitionStates(participant.getInstanceName(), HelixGatewayParticipant.UNASSIGNED_STATE);

    // Remove shard preference and re-add the participant
    removeFromPreferenceList(participant);
    HelixGatewayParticipant participantReplacement =
        addParticipant(participant.getInstanceName(), participant.getShardStateMap());
    verifyPendingMessages(List.of(participantReplacement));

    // Process the pending message successfully
    processPendingMessage(participantReplacement, true, "DROPPED");

    // Verify that the cluster converges and states are correctly updated to "ONLINE"
    Assert.assertTrue(_clusterVerifier.verify());
    verifyGatewayStateMatchesHelixState();
  }

  @Test(dependsOnMethods = "testProcessStateTransitionAfterReconnectAfterDroppingPartition")
  public void testGatewayParticipantDisconnectGracefully() {
    int gracefulDisconnectCount = MockHelixGatewayServiceChannel._gracefulDisconnectCount.get();
    // Remove the first participant
    HelixGatewayParticipant participant = _participants.get(0);
    deleteParticipant(participant);

    Assert.assertEquals(MockHelixGatewayServiceChannel._gracefulDisconnectCount.get(), gracefulDisconnectCount + 1);
  }

  @Test(dependsOnMethods = "testGatewayParticipantDisconnectGracefully")
  public void testGatewayParticipantDisconnectWithError() throws Exception {
    int errorDisconnectCount = MockHelixGatewayServiceChannel._errorDisconnectCount.get();
    int onDisconnectCallbackCount = _onDisconnectCallbackCount.get();

    // Call on disconnect with error for all participants
    for (HelixGatewayParticipant participant : _participants) {
      participant.onDisconnected(null, new Exception("Test error"));
    }

    Assert.assertEquals(MockHelixGatewayServiceChannel._errorDisconnectCount.get(),
        errorDisconnectCount + _participants.size());
    Assert.assertEquals(_onDisconnectCallbackCount.get(), onDisconnectCallbackCount + _participants.size());
  }

  public static class MockHelixGatewayServiceChannel implements HelixGatewayServiceChannel {
    private final Map<String, ShardChangeRequests> _pendingMessageMap;
    private static final AtomicInteger _gracefulDisconnectCount = new AtomicInteger();
    private static final AtomicInteger _errorDisconnectCount = new AtomicInteger();

    public MockHelixGatewayServiceChannel(Map<String, ShardChangeRequests> pendingMessageMap) {
      _pendingMessageMap = pendingMessageMap;
    }

    @Override
    public void sendStateChangeRequests(String instanceName,
        HelixGatewayServiceOuterClass.ShardChangeRequests shardChangeRequests) {
      _pendingMessageMap.put(instanceName, shardChangeRequests);
    }

    @Override
    public void start() throws IOException {

    }

    @Override
    public void stop() {

    }

    @Override
    public void closeConnectionWithError(String instanceName, String reason) {
      _errorDisconnectCount.incrementAndGet();
    }
    @Override
    public void completeConnection(String instanceName) {
      _gracefulDisconnectCount.incrementAndGet();
    }
  }
}
