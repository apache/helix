package org.apache.helix.integration.rebalancer;

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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestZeroReplicaAvoidance extends ZkTestBase
    implements ExternalViewChangeListener, IdealStateChangeListener {
  private final int NUM_NODE = 6;
  private final int START_PORT = 12918;
  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  private List<MockParticipantManager> _participants = new ArrayList<>();
  private ZkHelixClusterVerifier _clusterVerifier;
  // The flags are updated by the Helix callback threads and read by the test thread.
  private volatile boolean _testSuccess = true;
  private volatile boolean _startListen = false;
  private final List<String> _validationFailures = Collections.synchronizedList(new ArrayList<>());

  private ClusterControllerManager _controller;

  @BeforeMethod
  public void beforeMethod() {
    _testSuccess = true;
    _validationFailures.clear();
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
      participant.setTransition(new DelayedTransition());
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
  }

  @AfterMethod
  public void afterMethod() {
    _startListen = false;
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (MockParticipantManager participant : _participants) {
      if (participant != null && participant.isConnected()) {
        participant.syncStop();
      }
    }
    _participants.clear();
    deleteCluster(CLUSTER_NAME);
  }

  private String[] TestStateModels = {
      BuiltInStateModelDefinitions.MasterSlave.name(),
      BuiltInStateModelDefinitions.OnlineOffline.name(),
      BuiltInStateModelDefinitions.LeaderStandby.name()
  };

  @Test
  public void testDelayedRebalancer() throws Exception {
    System.out.println("START testDelayedRebalancer at " + new Date(System.currentTimeMillis()));
    HelixManager manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, null, InstanceType.SPECTATOR, ZK_ADDR);
    manager.connect();
    manager.addExternalViewChangeListener(this);
    manager.addIdealStateChangeListener(this);
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    // Start half number of nodes.
    int i = 0;
    for (; i < NUM_NODE / 2; i++) {
      _participants.get(i).syncStart();
    }

    int replica = 3;
    int partition = 30;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + stateModel;
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, partition, replica, replica,
          0);
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling(50000L, 100L));

    _startListen = true;
    DelayedTransition.setDelay(5);

    // add the other half of nodes.
    for (; i < NUM_NODE; i++) {
      _participants.get(i).syncStart();
    }
    Assert.assertTrue(_clusterVerifier.verify(70000L));
    Assert.assertTrue(_testSuccess, "Zero replica validation failed: " + _validationFailures);

    if (manager.isConnected()) {
      manager.disconnect();
    }
    System.out.println("END testDelayedRebalancer at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testWagedRebalancer() throws Exception {
    System.out.println("START testWagedRebalancer at " + new Date(System.currentTimeMillis()));
    HelixManager manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, null, InstanceType.SPECTATOR, ZK_ADDR);
    manager.connect();
    manager.addExternalViewChangeListener(this);
    manager.addIdealStateChangeListener(this);
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    // Start half number of nodes.
    int i = 0;
    for (; i < NUM_NODE / 2; i++) {
      _participants.get(i).syncStart();
    }

    int replica = 3;
    int partition = 30;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + stateModel;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, partition, replica, replica);
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling(50000L, 100L));

    _startListen = true;
    DelayedTransition.setDelay(5);

    // add the other half of nodes.
    for (; i < NUM_NODE; i++) {
      _participants.get(i).syncStart();
    }
    // Wait for the cluster to fully converge with a generous timeout so that slow or overloaded
    // machines have enough time to finish all the pending state transitions before the
    // listener-based validation result is checked.
    Assert.assertTrue(_clusterVerifier.verify(120000L));
    Assert.assertTrue(_testSuccess, "Zero replica validation failed: " + _validationFailures);

    if (manager.isConnected()) {
      manager.disconnect();
    }
    System.out.println("END testWagedRebalancer at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Validate instances for each partition is on different zone and with necessary tagged
   * instances.
   * Note that only the external view snapshot delivered with the callback is validated. The ideal
   * state is fetched separately and may already belong to a newer rebalance round on a slow
   * machine, so it is only used for the static configurations (replica count and state model).
   */
  private void validateNoZeroReplica(IdealState is, ExternalView ev) {
    int replica = is.getReplicaCount(NUM_NODE);
    StateModelDefinition stateModelDef =
        BuiltInStateModelDefinitions.valueOf(is.getStateModelDefRef()).getStateModelDefinition();

    for (String partition : is.getPartitionSet()) {
      Map<String, String> evStateMap = ev.getRecord().getMapField(partition);
      validateMap(is.getResourceName(), partition, replica, evStateMap, stateModelDef);
    }
  }

  /**
   * Validate the mapping of the ideal state snapshot delivered with the callback.
   */
  private void validateNoZeroReplica(IdealState is) {
    int replica = is.getReplicaCount(NUM_NODE);
    StateModelDefinition stateModelDef =
        BuiltInStateModelDefinitions.valueOf(is.getStateModelDefRef()).getStateModelDefinition();

    for (String partition : is.getPartitionSet()) {
      validateMap(is.getResourceName(), partition, replica, is.getInstanceStateMap(partition),
          stateModelDef);
    }
  }

  private void validateMap(String resource, String partition, int replica,
      Map<String, String> instanceStateMap, StateModelDefinition stateModelDef) {
    if (instanceStateMap == null || instanceStateMap.isEmpty()) {
      recordValidationFailure(
          String.format("Resource %s partition %s has no active replica!", resource, partition));
      return;
    }
    if (instanceStateMap.size() < replica) {
      recordValidationFailure(
          String.format("Resource %s partition %s has %d active replica, less than required %d!",
              resource, partition, instanceStateMap.size(), replica));
      return;
    }

    Map<String, Integer> stateCountMap = stateModelDef.getStateCountMap(NUM_NODE, replica);
    String topState = stateModelDef.getStatesPriorityList().get(0);
    if (stateCountMap.get(topState) == 1) {
      int topCount = 0;
      for (String val : instanceStateMap.values()) {
        if (topState.equals(val)) {
          topCount++;
        }
      }
      if (topCount > 1) {
        recordValidationFailure(
            String.format("Resource %s partition %s has %d replica in %s, more than 1!", resource,
                partition, topCount, topState));
      }
    }
  }

  private void recordValidationFailure(String message) {
    // Record the failure instead of calling Assert.fail(). The validation runs on the Helix
    // callback threads, where a thrown assertion error is swallowed by the callback handler and
    // would also abort the validation of the remaining resources.
    _testSuccess = false;
    _validationFailures.add(message);
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    if (!_startListen) {
      return;
    }
    for (ExternalView view : externalViewList) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME,
          view.getResourceName());
      if (is == null) {
        // The resource may have been removed while the callback was pending; skip it.
        continue;
      }
      validateNoZeroReplica(is, view);
    }
  }

  @Override
  public void onIdealStateChange(List<IdealState> idealStates, NotificationContext changeContext) {
    if (!_startListen) {
      return;
    }
    for (IdealState is : idealStates) {
      // Only validate the ideal state snapshot delivered with this callback. Fetching the
      // corresponding external view here is racy on a slow machine: the fetched external view may
      // reflect a different rebalance round than this snapshot and cause false alarms.
      validateNoZeroReplica(is);
    }
  }

  private static class DelayedTransition extends MockTransition {
    private static long _delay = 0;

    public static void setDelay(int delay) {
      _delay = delay;
    }

    @Override
    public void doTransition(Message message, NotificationContext context)
        throws InterruptedException {
      if (_delay > 0) {
        Thread.sleep(_delay);
      }
    }
  }
}
