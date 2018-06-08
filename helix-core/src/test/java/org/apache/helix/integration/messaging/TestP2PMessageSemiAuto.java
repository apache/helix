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
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.api.config.HelixConfigProperty;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.integration.DelayedTransitionBase;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
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

  static final int PARTITION_NUMBER = 20;
  static final int REPLICA_NUMBER = 3;

  List<MockParticipantManager> _participants = new ArrayList<>();
  List<String> _instances = new ArrayList<>();
  ClusterControllerManager _controller;

  HelixClusterVerifier _clusterVerifier;
  ConfigAccessor _configAccessor;
  HelixDataAccessor _accessor;

  @BeforeClass
  public void beforeClass()
      throws InterruptedException {
    System.out.println(
        "START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

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

    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient).build();
    Assert.assertTrue(_clusterVerifier.verify());

    _configAccessor = new ConfigAccessor(_gZkClient);
    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (MockParticipantManager p : _participants) {
      if (p.isConnected()) {
        p.syncStop();
      }
    }
    _gSetupTool.deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testP2PStateTransitionDisabled() {
    // disable the master instance
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, false);

    Assert.assertTrue(_clusterVerifier.verify());
    verifyP2PMessage(DB_NAME_1,_instances.get(1), MasterSlaveSMD.States.MASTER.name(), _controller.getInstanceName());
    verifyP2PMessage(DB_NAME_2,_instances.get(1), MasterSlaveSMD.States.MASTER.name(), _controller.getInstanceName());


    //re-enable the old master
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, true);
    Assert.assertTrue(_clusterVerifier.verify());

    verifyP2PMessage(DB_NAME_1, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(), _controller.getInstanceName());
    verifyP2PMessage(DB_NAME_2, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(), _controller.getInstanceName());
  }

  @Test (dependsOnMethods = {"testP2PStateTransitionDisabled"})
  public void testP2PStateTransitionEnabledInCluster() {
    enableP2PInCluster(true);
    enableP2PInResource(DB_NAME_1,false);
    enableP2PInResource(DB_NAME_2,false);

    // disable the master instance
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, false);

    Assert.assertTrue(_clusterVerifier.verify());
    verifyP2PMessage(DB_NAME_1, _instances.get(1), MasterSlaveSMD.States.MASTER.name(), prevMasterInstance);
    verifyP2PMessage(DB_NAME_2, _instances.get(1), MasterSlaveSMD.States.MASTER.name(), prevMasterInstance);

    //re-enable the old master
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, true);
    Assert.assertTrue(_clusterVerifier.verify());

    verifyP2PMessage(DB_NAME_1, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(), _instances.get(1));
    verifyP2PMessage(DB_NAME_2, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(), _instances.get(1));
  }

  @Test (dependsOnMethods = {"testP2PStateTransitionDisabled"})
  public void testP2PStateTransitionEnabledInResource() {
    enableP2PInCluster(false);
    enableP2PInResource(DB_NAME_1,true);
    enableP2PInResource(DB_NAME_2,false);


    // disable the master instance
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, false);

    Assert.assertTrue(_clusterVerifier.verify());
    verifyP2PMessage(DB_NAME_1, _instances.get(1), MasterSlaveSMD.States.MASTER.name(), prevMasterInstance);
    verifyP2PMessage(DB_NAME_2, _instances.get(1), MasterSlaveSMD.States.MASTER.name(), _controller.getInstanceName());


    //re-enable the old master
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, true);
    Assert.assertTrue(_clusterVerifier.verify());

    verifyP2PMessage(DB_NAME_1, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(), _instances.get(1));
    verifyP2PMessage(DB_NAME_2, prevMasterInstance, MasterSlaveSMD.States.MASTER.name(), _controller.getInstanceName());
  }

  private void enableP2PInCluster(boolean enable) {
    // enable p2p message in cluster.
    if (enable) {
      ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
      clusterConfig.enableP2PMessage(true);
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    } else {
      ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
      clusterConfig.getRecord().getSimpleFields().remove(HelixConfigProperty.P2P_MESSAGE_ENABLED.name());
      _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    }
  }

  private void enableP2PInResource(String dbName, boolean enable) {
    if (enable) {
      ResourceConfig resourceConfig = new ResourceConfig.Builder(dbName).setP2PMessageEnabled(true).build();
      _configAccessor.setResourceConfig(CLUSTER_NAME, dbName, resourceConfig);
    } else {
      // remove P2P Message in resource config
      ResourceConfig resourceConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, dbName);
      if (resourceConfig != null) {
        resourceConfig.getRecord().getSimpleFields().remove(HelixConfigProperty.P2P_MESSAGE_ENABLED.name());
        _configAccessor.setResourceConfig(CLUSTER_NAME, dbName, resourceConfig);
      }
    }
  }

  private void verifyP2PMessage(String dbName, String instance, String expectedState, String expectedTriggerHost) {
    ClusterDataCache dataCache = new ClusterDataCache(CLUSTER_NAME);
    dataCache.refresh(_accessor);

    Map<String, LiveInstance> liveInstanceMap = dataCache.getLiveInstances();
    LiveInstance liveInstance = liveInstanceMap.get(instance);

    Map<String, CurrentState> currentStateMap = dataCache.getCurrentState(instance,
        liveInstance.getSessionId());
    Assert.assertNotNull(currentStateMap);
    CurrentState currentState = currentStateMap.get(dbName);
    Assert.assertNotNull(currentState);
    Assert.assertEquals(currentState.getPartitionStateMap().size(), PARTITION_NUMBER);

    int total = 0;
    int expectedHost = 0;
    for (String partition : currentState.getPartitionStateMap().keySet()) {
      String state = currentState.getState(partition);
      Assert.assertEquals(state, expectedState,
          dbName + " Partition " + partition + "'s state is different as expected!");
      String triggerHost = currentState.getTriggerHost(partition);
      if (triggerHost.equals(expectedTriggerHost)) {
        expectedHost ++;
      }
      total ++;
    }

    double ratio = ((double) expectedHost) / ((double) total);
    Assert.assertTrue(ratio >= 0.7, String
        .format("Only %d out of %d percent transitions to Master were triggered by expected host!",
            expectedHost, total));
  }
}

