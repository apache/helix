package org.apache.helix.integration.rebalancer.WagedRebalancer;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestWagedRebalanceFaultZone extends ZkTestBase {
  protected final int NUM_NODE = 6;
  protected static final int START_PORT = 12918;
  protected static final int PARTITIONS = 20;
  protected static final int ZONES = 3;
  protected static final int TAGS = 2;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;

  List<MockParticipantManager> _participants = new ArrayList<>();
  Map<String, String> _nodeToZoneMap = new HashMap<>();
  Map<String, String> _nodeToTagMap = new HashMap<>();
  List<String> _nodes = new ArrayList<>();
  Set<String> _allDBs = new HashSet<>();
  int _replica = 3;

  String[] _testModels = {
      BuiltInStateModelDefinitions.OnlineOffline.name(),
      BuiltInStateModelDefinitions.MasterSlave.name(),
      BuiltInStateModelDefinitions.LeaderStandby.name()
  };

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      addInstanceConfig(storageNodeName, i, ZONES, TAGS);
    }

    // start dummy participants
    for (String node : _nodes) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      participant.syncStart();
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    enableTopologyAwareRebalance(_gZkClient, CLUSTER_NAME, true);
  }

  protected void addInstanceConfig(String storageNodeName, int seqNo, int zoneCount, int tagCount) {
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    String zone = "zone-" + seqNo % zoneCount;
    String tag = "tag-" + seqNo % tagCount;
    _gSetupTool.getClusterManagementTool().setInstanceZoneId(CLUSTER_NAME, storageNodeName, zone);
    _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, storageNodeName, tag);
    _nodeToZoneMap.put(storageNodeName, zone);
    _nodeToTagMap.put(storageNodeName, tag);
    _nodes.add(storageNodeName);
  }

  @Test
  public void testZoneIsolation() throws Exception {
    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    validate(_replica);
  }

  @Test
  public void testZoneIsolationWithInstanceTag() throws Exception {
    Set<String> tags = new HashSet<String>(_nodeToTagMap.values());
    int i = 0;
    for (String tag : tags) {
      String db = "Test-DB-" + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db,
          BuiltInStateModelDefinitions.MasterSlave.name(), PARTITIONS, _replica, _replica);
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      is.setInstanceGroupTag(tag);
      _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, is);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    validate(_replica);
  }

  @Test(dependsOnMethods = { "testZoneIsolation", "testZoneIsolationWithInstanceTag" })
  public void testLackEnoughLiveRacks() throws Exception {
    // shutdown participants within one zone
    String zone = _nodeToZoneMap.values().iterator().next();
    for (int i = 0; i < _participants.size(); i++) {
      MockParticipantManager p = _participants.get(i);
      if (_nodeToZoneMap.get(p.getInstanceName()).equals(zone)) {
        p.syncStop();
      }
    }

    int j = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + j++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);
    validate(2);

    // restart the participants within the zone
    for (int i = 0; i < _participants.size(); i++) {
      MockParticipantManager p = _participants.get(i);
      if (_nodeToZoneMap.get(p.getInstanceName()).equals(zone)) {
        MockParticipantManager newNode =
            new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, p.getInstanceName());
        _participants.set(i, newNode);
        newNode.syncStart();
      }
    }

    Thread.sleep(300);
    // Verify if the partitions get assigned
    validate(_replica);
  }

  @Test(dependsOnMethods = { "testLackEnoughLiveRacks" })
  public void testLackEnoughRacks() throws Exception {
    // shutdown participants within one zone
    String zone = _nodeToZoneMap.values().iterator().next();
    for (int i = 0; i < _participants.size(); i++) {
      MockParticipantManager p = _participants.get(i);
      if (_nodeToZoneMap.get(p.getInstanceName()).equals(zone)) {
        p.syncStop();
        _gSetupTool.getClusterManagementTool()
            .enableInstance(CLUSTER_NAME, p.getInstanceName(), false);
        Thread.sleep(50);
        _gSetupTool.dropInstanceFromCluster(CLUSTER_NAME, p.getInstanceName());
      }
    }

    int j = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + j++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);
    validate(2);

    // Create new participants within the zone
    int nodeCount = _participants.size();
    for (int i = 0; i < nodeCount; i++) {
      MockParticipantManager p = _participants.get(i);
      if (_nodeToZoneMap.get(p.getInstanceName()).equals(zone)) {
        String replaceNodeName = p.getInstanceName() + "-replacement_" + START_PORT;
        addInstanceConfig(replaceNodeName, i, ZONES, TAGS);
        MockParticipantManager newNode =
            new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, replaceNodeName);
        _participants.set(i, newNode);
        newNode.syncStart();
      }
    }

    Thread.sleep(300);
    // Verify if the partitions get assigned
    validate(_replica);
  }

  @Test(dependsOnMethods = { "testZoneIsolation", "testZoneIsolationWithInstanceTag" })
  public void testAddZone() throws Exception {
    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    validate(_replica);

    // Create new participants within the a new zone
    Set<MockParticipantManager> newNodes = new HashSet<>();
    Map<String, Integer> newNodeReplicaCount = new HashMap<>();

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);

    try {
      // Configure the preference so as to allow movements.
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
      preference.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 10);
      preference.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 0);
      clusterConfig.setGlobalRebalancePreference(preference);
      configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

      int nodeCount = 2;
      for (int j = 0; j < nodeCount; j++) {
        String newNodeName = "new-zone-node-" + j + "_" + START_PORT;
        // Add all new node to the new zone
        addInstanceConfig(newNodeName, j, ZONES + 1, TAGS);
        MockParticipantManager newNode =
            new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newNodeName);
        newNode.syncStart();
        newNodes.add(newNode);
        newNodeReplicaCount.put(newNodeName, 0);
      }
      Thread.sleep(300);

      validate(_replica);

      // The new zone nodes shall have some assignments
      for (String db : _allDBs) {
        IdealState is =
            _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
        ExternalView ev =
            _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
        validateZoneAndTagIsolation(is, ev, _replica);
        for (String partition : ev.getPartitionSet()) {
          Map<String, String> stateMap = ev.getStateMap(partition);
          for (String node : stateMap.keySet()) {
            if (newNodeReplicaCount.containsKey(node)) {
              newNodeReplicaCount.computeIfPresent(node, (nodeName, replicaCount) -> replicaCount + 1);
            }
          }
        }
      }
      Assert.assertTrue(newNodeReplicaCount.values().stream().allMatch(count -> count > 0));
    } finally {
      // Revert the preference
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
      preference.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 1);
      preference.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 1);
      clusterConfig.setGlobalRebalancePreference(preference);
      configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
      // Stop the new nodes
      for (MockParticipantManager p : newNodes) {
        if (p != null && p.isConnected()) {
          p.syncStop();
        }
      }
    }
  }

  private void validate(int expectedReplica) {
    ZkHelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setDeactivatedNodeAwareness(true).setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    for (String db : _allDBs) {
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      validateZoneAndTagIsolation(is, ev, expectedReplica);
    }
  }

  /**
   * Validate instances for each partition is on different zone and with necessary tagged instances.
   */
  private void validateZoneAndTagIsolation(IdealState is, ExternalView ev, int expectedReplica) {
    String tag = is.getInstanceGroupTag();
    for (String partition : is.getPartitionSet()) {
      Set<String> assignedZones = new HashSet<String>();

      Map<String, String> assignmentMap = ev.getRecord().getMapField(partition);
      Set<String> instancesInEV = assignmentMap.keySet();
      // TODO: preference List is not persisted in IS.
      // Assert.assertEquals(instancesInEV, instancesInIs);
      for (String instance : instancesInEV) {
        assignedZones.add(_nodeToZoneMap.get(instance));
        if (tag != null) {
          InstanceConfig config =
              _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instance);
          Assert.assertTrue(config.containsTag(tag));
        }
      }
      Assert.assertEquals(assignedZones.size(), expectedReplica);
    }
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    for (String db : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    }
    _allDBs.clear();
    // waiting for all DB be dropped.
    Thread.sleep(100);
    ZkHelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setDeactivatedNodeAwareness(true).setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @AfterClass
  public void afterClass() throws Exception {
    /*
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (MockParticipantManager p : _participants) {
      if (p != null && p.isConnected()) {
        p.syncStop();
      }
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }
}
