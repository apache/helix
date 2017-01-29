package org.apache.helix.integration;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestSemiAutoRebalance extends ZkIntegrationTestBase {
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  protected static final int PARTICIPANT_NUMBER = 5;
  protected static final int PARTICIPANT_START_PORT = 12918;

  protected static final String DB_NAME = "TestDB";
  protected static final int PARTITION_NUMBER = 20;
  protected static final int REPLICA_NUMBER = 3;
  protected static final String STATE_MODEL = "MasterSlave";

  protected List<MockParticipantManager> _participants = new ArrayList<MockParticipantManager>();
  protected ClusterControllerManager _controller;

  protected HelixDataAccessor _accessor;
  protected PropertyKey.Builder _keyBuilder;

  @BeforeClass
  public void beforeClass()
      throws InterruptedException {
    System.out.println(
        "START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, DB_NAME, PARTITION_NUMBER, STATE_MODEL,
        IdealState.RebalanceMode.SEMI_AUTO.toString());

    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    _keyBuilder = _accessor.keyBuilder();

    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      String instance = PARTICIPANT_PREFIX + "_" + (PARTICIPANT_START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance);
      instances.add(instance);
    }

    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, DB_NAME, REPLICA_NUMBER);

    // start dummy participants
    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instances.get(i));
      participant.syncStart();
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    Thread.sleep(1000);

    // verify ideal state and external view
    IdealState idealState = _accessor.getProperty(_keyBuilder.idealStates(DB_NAME));
    Assert.assertNotNull(idealState);
    Assert.assertEquals(idealState.getNumPartitions(), PARTITION_NUMBER);
    for (String partition : idealState.getPartitionSet()) {
      List<String> preferenceList = idealState.getPreferenceList(partition);
      Assert.assertNotNull(preferenceList);
      Assert.assertEquals(preferenceList.size(), REPLICA_NUMBER);
    }

    ExternalView externalView = _accessor.getProperty(_keyBuilder.externalView(DB_NAME));
    Assert.assertNotNull(externalView);
    Assert.assertEquals(externalView.getPartitionSet().size(), PARTITION_NUMBER);
    for (String partition : externalView.getPartitionSet()) {
      Map<String, String> stateMap = externalView.getStateMap(partition);
      Assert.assertEquals(stateMap.size(), REPLICA_NUMBER);

      int masters = 0;
      for (String state : stateMap.values()) {
        if (state.equals(MasterSlaveSMD.States.MASTER.name())) {
          ++masters;
        }
      }
      Assert.assertEquals(masters, 1);
    }
  }

  @Test
  public void testAddParticipant()
      throws InterruptedException {
    String newInstance = PARTICIPANT_PREFIX + "_" + (PARTICIPANT_START_PORT + _participants.size());
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, newInstance);

    MockParticipantManager newParticipant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newInstance);
    newParticipant.syncStart();

    Thread.sleep(1000);

    List<String> instances = _accessor.getChildNames(_keyBuilder.instanceConfigs());
    Assert.assertEquals(instances.size(), _participants.size() + 1);
    Assert.assertTrue(instances.contains(newInstance));

    List<String> liveInstances = _accessor.getChildNames(_keyBuilder.liveInstances());
    Assert.assertEquals(liveInstances.size(), _participants.size() + 1);
    Assert.assertTrue(liveInstances.contains(newInstance));

    // nothing for new participant
    ExternalView externalView = _accessor.getProperty(_keyBuilder.externalView(DB_NAME));
    Assert.assertNotNull(externalView);
    for (String partition : externalView.getPartitionSet()) {
      Map<String, String> stateMap = externalView.getStateMap(partition);
      Assert.assertFalse(stateMap.containsKey(newInstance));
    }

    // clear
    newParticipant.syncStop();
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, newInstance, false);
    _gSetupTool.dropInstanceFromCluster(CLUSTER_NAME, newInstance);

    instances = _accessor.getChildNames(_keyBuilder.instanceConfigs());
    Assert.assertEquals(instances.size(), _participants.size());

    liveInstances = _accessor.getChildNames(_keyBuilder.liveInstances());
    Assert.assertEquals(liveInstances.size(), _participants.size());
  }

  @Test(dependsOnMethods = "testAddParticipant")
  public void testStopAndReStartParticipant()
      throws InterruptedException {
    MockParticipantManager participant = _participants.get(0);
    String instance = participant.getInstanceName();

    Map<String, MasterSlaveSMD.States> affectedPartitions =
        new HashMap<String, MasterSlaveSMD.States>();

    ExternalView externalView = _accessor.getProperty(_keyBuilder.externalView(DB_NAME));

    for (String partition : externalView.getPartitionSet()) {
      Map<String, String> stateMap = externalView.getStateMap(partition);
      if (stateMap.containsKey(instance)) {
        affectedPartitions.put(partition, MasterSlaveSMD.States.valueOf(stateMap.get(instance)));
      }
    }

    stopParticipant(participant, affectedPartitions);

    // create a new participant
    participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance);
    _participants.set(0, participant);
    startParticipant(participant, affectedPartitions);
  }

  private void stopParticipant(
      MockParticipantManager participant, Map<String, MasterSlaveSMD.States> affectedPartitions)
      throws InterruptedException {
    participant.syncStop();

    Thread.sleep(1000);

    ExternalView externalView = _accessor.getProperty(_keyBuilder.externalView(DB_NAME));
    // No re-assignment of partition, if a MASTER is removed, one of SLAVE would be prompted
    for (Map.Entry<String, MasterSlaveSMD.States> entry : affectedPartitions.entrySet()) {
      Map<String, String> stateMap = externalView.getStateMap(entry.getKey());
      Assert.assertEquals(stateMap.size(), REPLICA_NUMBER - 1);
      Assert.assertTrue(stateMap.values().contains(MasterSlaveSMD.States.MASTER.toString()));
    }
  }

  private void startParticipant(
      MockParticipantManager participant, Map<String, MasterSlaveSMD.States> affectedPartitions)
      throws InterruptedException {
    String instance = participant.getInstanceName();
    participant.syncStart();

    Thread.sleep(1000);

    ExternalView externalView = _accessor.getProperty(_keyBuilder.externalView(DB_NAME));
    // Everything back to the initial state
    for (Map.Entry<String, MasterSlaveSMD.States> entry : affectedPartitions.entrySet()) {
      Map<String, String> stateMap = externalView.getStateMap(entry.getKey());
      Assert.assertEquals(stateMap.size(), REPLICA_NUMBER);

      Assert.assertTrue(stateMap.containsKey(instance));
      Assert.assertEquals(stateMap.get(instance), entry.getValue().toString());
    }
  }
}
