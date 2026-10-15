package org.apache.helix.integration.messaging;

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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.integration.DelayedTransitionBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestP2PMessageSemiAuto extends ZkTestBase {
  final String CLASS_NAME = getShortClassName();
  final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  static final int PARTICIPANT_NUMBER = 3;
  static final int PARTICIPANT_START_PORT = 12918;

  static final String DB_NAME_1 = "TestDB_1";
  static final String DB_NAME_2 = "TestDB_2";

  // Keep the number of partitions small enough that all state transitions (with the delay
  // configured below) complete well within the relay message expiry
  // (Message.RELAY_MESSAGE_DEFAULT_EXPIRY, 5 seconds) even on slow machines. Otherwise relay
  // messages expire and the controller sends the transitions instead, defeating the P2P checks.
  static final int PARTITION_NUMBER = 50;
  static final int REPLICA_NUMBER = 3;

  List<MockParticipantManager> _participants = new ArrayList<>();
  List<String> _instances = new ArrayList<>();
  ClusterControllerManager _controller;

  ZkHelixClusterVerifier _clusterVerifier;
  ConfigAccessor _configAccessor;
  HelixDataAccessor _accessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    System.out
        .println("START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      String instance = PARTICIPANT_PREFIX + "_" + (PARTICIPANT_START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance);
      _instances.add(instance);
    }

    // start dummy participants
    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _instances.get(i));
      participant.setTransition(new DelayedTransitionBase(50));
      participant.syncStart();
      _participants.add(participant);
    }

    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, DB_NAME_1, _instances,
        BuiltInStateModelDefinitions.MasterSlave.name(), PARTITION_NUMBER, REPLICA_NUMBER);
    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, DB_NAME_2, _instances,
        BuiltInStateModelDefinitions.MasterSlave.name(), PARTITION_NUMBER, REPLICA_NUMBER);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    _configAccessor = new ConfigAccessor(_gZkClient);
    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testP2PStateTransitionDisabled() throws Exception {
    // disable the master instance
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, false);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    verifyP2PMessage(DB_NAME_1, _instances.get(1), MasterSlaveSMD.States.MASTER.name(),
        _controller.getInstanceName(), 1);
    verifyP2PMessage(DB_NAME_2, _instances.get(1), MasterSlaveSMD.States.MASTER.name(),
        _controller.getInstanceName(), 1);

    // re-enable the old master
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, true);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    verifyP2PMessage(DB_NAME_1, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(),
        _controller.getInstanceName(), 1);
    verifyP2PMessage(DB_NAME_2, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(),
        _controller.getInstanceName(), 1);
  }

  @Test(dependsOnMethods = {
      "testP2PStateTransitionDisabled"
  })
  public void testP2PStateTransitionEnabledInCluster() throws Exception {
    enableP2PInCluster(CLUSTER_NAME, _configAccessor, true);
    enableP2PInResource(CLUSTER_NAME, _configAccessor, DB_NAME_1, false);
    enableP2PInResource(CLUSTER_NAME, _configAccessor, DB_NAME_2, false);

    // disable the master instance
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, false);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    verifyP2PMessage(DB_NAME_1, _instances.get(1), MasterSlaveSMD.States.MASTER.name(),
        prevMasterInstance);
    verifyP2PMessage(DB_NAME_2, _instances.get(1), MasterSlaveSMD.States.MASTER.name(),
        prevMasterInstance);

    // re-enable the old master
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, true);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    verifyP2PMessage(DB_NAME_1, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(),
        _instances.get(1));
    verifyP2PMessage(DB_NAME_2, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(),
        _instances.get(1));
  }

  @Test(dependsOnMethods = {
      "testP2PStateTransitionDisabled"
  })
  public void testP2PStateTransitionEnabledInResource() throws Exception {
    enableP2PInCluster(CLUSTER_NAME, _configAccessor, false);
    enableP2PInResource(CLUSTER_NAME, _configAccessor, DB_NAME_1, true);
    enableP2PInResource(CLUSTER_NAME, _configAccessor, DB_NAME_2, false);

    // disable the master instance
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, false);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    verifyP2PMessage(DB_NAME_1, _instances.get(1), MasterSlaveSMD.States.MASTER.name(),
        prevMasterInstance);
    verifyP2PMessage(DB_NAME_2, _instances.get(1), MasterSlaveSMD.States.MASTER.name(),
        _controller.getInstanceName(), 1);

    // re-enable the old master
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, true);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    verifyP2PMessage(DB_NAME_1, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(),
        _instances.get(1));
    verifyP2PMessage(DB_NAME_2, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(),
        _controller.getInstanceName(), 1);
  }

  private void verifyP2PMessage(String dbName, String instance, String expectedState,
      String expectedTriggerHost) throws Exception {
    // Use 0.5 as the expected ratio. Relay messages may occasionally expire on slow machines
    // (the controller then triggers the transition itself), so only require that the majority
    // of the transitions were triggered by the expected host via P2P messages.
    verifyP2PMessage(dbName, instance, expectedState, expectedTriggerHost, 0.5);
  }

  private void verifyP2PMessage(String dbName, String instance, String expectedState,
      String expectedTriggerHost, double expectedRatio) throws Exception {
    final int[] expectedHostCount = new int[1];
    final int[] totalCount = new int[1];

    boolean verified = TestHelper.verify(() -> {
      ResourceControllerDataProvider dataCache = new ResourceControllerDataProvider(CLUSTER_NAME);
      dataCache.refresh(_accessor);

      Map<String, LiveInstance> liveInstanceMap = dataCache.getAssignableLiveInstances();
      LiveInstance liveInstance = liveInstanceMap.get(instance);
      if (liveInstance == null) {
        return false;
      }

      Map<String, CurrentState> currentStateMap =
          dataCache.getCurrentState(instance, liveInstance.getEphemeralOwner());
      if (currentStateMap == null) {
        return false;
      }
      CurrentState currentState = currentStateMap.get(dbName);
      if (currentState == null
          || currentState.getPartitionStateMap().size() != PARTITION_NUMBER) {
        return false;
      }

      int total = 0;
      int expectedHost = 0;
      for (String partition : currentState.getPartitionStateMap().keySet()) {
        String state = currentState.getState(partition);
        if (!expectedState.equals(state)) {
          // Not all transitions have completed yet, retry before sampling the trigger hosts.
          return false;
        }
        String triggerHost = currentState.getTriggerHost(partition);
        if (expectedTriggerHost.equals(triggerHost)) {
          expectedHost++;
        }
        total++;
      }

      expectedHostCount[0] = expectedHost;
      totalCount[0] = total;
      return ((double) expectedHost) / ((double) total) >= expectedRatio;
    }, TestHelper.WAIT_DURATION);

    // totalCount is only populated once all partitions reach the expected state; 0 means the
    // current states never converged, which is a different failure than a bad trigger ratio.
    Assert.assertTrue(verified, totalCount[0] == 0
        ? String.format(
            "Current states of %s on %s never converged to %s on all %d partitions within the"
                + " timeout, so the P2P trigger hosts could not be sampled!",
            dbName, instance, expectedState, PARTITION_NUMBER)
        : String.format(
            "Only %d out of %d transitions of %s to %s were triggered by expected host %s,"
                + " expected ratio is at least %s!",
            expectedHostCount[0], totalCount[0], dbName, expectedState, expectedTriggerHost,
            expectedRatio));
  }
}
