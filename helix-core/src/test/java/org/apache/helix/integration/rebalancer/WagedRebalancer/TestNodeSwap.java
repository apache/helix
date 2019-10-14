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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestNodeSwap extends ZkTestBase {
  final int NUM_NODE = 6;
  protected static final int START_PORT = 12918;
  protected static final int _PARTITIONS = 20;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;
  protected HelixClusterVerifier _clusterVerifier;

  List<MockParticipantManager> _participants = new ArrayList<>();
  Set<String> _allDBs = new HashSet<>();
  int _replica = 3;

  String[] _testModels = { BuiltInStateModelDefinitions.OnlineOffline.name(),
      BuiltInStateModelDefinitions.MasterSlave.name(),
      BuiltInStateModelDefinitions.LeaderStandby.name()
  };

  @BeforeClass
  public void beforeClass() throws Exception {
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
    clusterConfig.setDelayRebalaceEnabled(true);
    // Set a long enough time to ensure delayed rebalance is activate
    clusterConfig.setRebalanceDelayTime(3000000);
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    Set<String> nodes = new HashSet<>();
    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      String zone = "zone-" + i % 3;
      String domain = String.format("zone=%s,instance=%s", zone, storageNodeName);

      InstanceConfig instanceConfig =
          configAccessor.getInstanceConfig(CLUSTER_NAME, storageNodeName);
      instanceConfig.setDomain(domain);
      _gSetupTool.getClusterManagementTool()
          .setInstanceConfig(CLUSTER_NAME, storageNodeName, instanceConfig);
      nodes.add(storageNodeName);
    }

    // start dummy participants
    for (String node : nodes) {
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

    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _replica - 1);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(1000);

    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setDeactivatedNodeAwareness(true).setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verify(5000));
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testNodeSwap() throws Exception {
    Map<String, ExternalView> record = new HashMap<>();
    for (String db : _allDBs) {
      record.put(db,
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db));
    }
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);

    // 1. disable an old node
    MockParticipantManager oldParticipant = _participants.get(0);
    String oldParticipantName = oldParticipant.getInstanceName();
    final InstanceConfig instanceConfig =
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, oldParticipantName);
    instanceConfig.setInstanceEnabled(false);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, oldParticipantName, instanceConfig);
    Assert.assertTrue(_clusterVerifier.verify(10000));

    // 2. then entering maintenance mode and remove it from topology
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "NodeSwap", Collections.emptyMap());
    oldParticipant.syncStop();
    _participants.remove(oldParticipant);
    Thread.sleep(2000);
    _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, instanceConfig);

    // 3. create new participant with same topology
    String newParticipantName = "RandomParticipant_" + START_PORT;
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, newParticipantName);
    InstanceConfig newConfig = configAccessor.getInstanceConfig(CLUSTER_NAME, newParticipantName);
    String zone = instanceConfig.getDomainAsMap().get("zone");
    String domain = String.format("zone=%s,instance=%s", zone, newParticipantName);
    newConfig.setDomain(domain);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, newParticipantName, newConfig);

    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newParticipantName);
    participant.syncStart();
    _participants.add(0, participant);

    // 4. exit maintenance mode and rebalance
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, "NodeSwapDone", Collections.emptyMap());

    Thread.sleep(2000);
    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setDeactivatedNodeAwareness(true).setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verify(5000));

    // Since only one node temporary down, the same partitions will be moved to the newly added node.
    for (String db : _allDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      ExternalView oldEv = record.get(db);
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        Map<String, String> oldStateMap = oldEv.getStateMap(partition);
        Assert.assertTrue(oldStateMap != null && stateMap != null);
        Assert.assertEquals(stateMap.size(), _replica);
        // Note the WAGED rebalanacer won't ensure the same state, because moving the top states
        // back to the replaced node might be unnecessary and causing overhead.
        Set<String> instanceSet = new HashSet<>(stateMap.keySet());
        if (instanceSet.remove(newParticipantName)) {
          instanceSet.add(oldParticipantName);
        }
        Assert.assertEquals(oldStateMap.keySet(), instanceSet);
      }
    }
  }

  @Test(dependsOnMethods = "testNodeSwap")
  public void testFaultZoneSwap() throws Exception {
    Map<String, ExternalView> record = new HashMap<>();
    for (String db : _allDBs) {
      record.put(db,
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db));
    }
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);

    // 1. disable a whole fault zone
    Map<String, InstanceConfig> removedInstanceConfigMap = new HashMap<>();
    for (MockParticipantManager participant : _participants) {
      String instanceName = participant.getInstanceName();
      InstanceConfig instanceConfig =
          _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceName);
      if (instanceConfig.getDomainAsMap().get("zone").equals("zone-0")) {
        instanceConfig.setInstanceEnabled(false);
        _gSetupTool.getClusterManagementTool()
            .setInstanceConfig(CLUSTER_NAME, instanceName, instanceConfig);
        removedInstanceConfigMap.put(instanceName, instanceConfig);
      }
    }
    Assert.assertTrue(_clusterVerifier.verify(10000));

    // 2. then entering maintenance mode and remove all the zone-0 nodes from topology
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "NodeSwap", Collections.emptyMap());
    Iterator<MockParticipantManager> iter = _participants.iterator();
    while(iter.hasNext()) {
      MockParticipantManager participant = iter.next();
      String instanceName = participant.getInstanceName();
      if (removedInstanceConfigMap.containsKey(instanceName)) {
        participant.syncStop();
        iter.remove();
        Thread.sleep(1000);
        _gSetupTool.getClusterManagementTool()
            .dropInstance(CLUSTER_NAME, removedInstanceConfigMap.get(instanceName));
      }
    }

    // 3. create new participants with same topology
    Set<String> newInstanceNames = new HashSet<>();
    for (int i = 0; i < removedInstanceConfigMap.size(); i++) {
      String newParticipantName = "NewParticipant_" + (START_PORT + i++);
      newInstanceNames.add(newParticipantName);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, newParticipantName);
      InstanceConfig newConfig = configAccessor.getInstanceConfig(CLUSTER_NAME, newParticipantName);
      String domain = String.format("zone=zone-0,instance=%s", newParticipantName);
      newConfig.setDomain(domain);
      _gSetupTool.getClusterManagementTool()
          .setInstanceConfig(CLUSTER_NAME, newParticipantName, newConfig);

      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newParticipantName);
      participant.syncStart();
      _participants.add(participant);
    }

    // 4. exit maintenance mode and rebalance
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, "NodeSwapDone", Collections.emptyMap());

    Thread.sleep(2000);
    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setDeactivatedNodeAwareness(true).setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verify(5000));

    for (String db : _allDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      ExternalView oldEv = record.get(db);
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        Map<String, String> oldStateMap = oldEv.getStateMap(partition);
        Assert.assertTrue(oldStateMap != null && stateMap != null);
        Assert.assertEquals(stateMap.size(), _replica);
        Set<String> instanceSet = new HashSet<>(stateMap.keySet());
        instanceSet.removeAll(oldStateMap.keySet());
        // All the different instances in the new mapping are the newly added instance
        Assert.assertTrue(newInstanceNames.containsAll(instanceSet));
        instanceSet = new HashSet<>(oldStateMap.keySet());
        instanceSet.removeAll(stateMap.keySet());
        // All the different instances in the old mapping are the removed instance
        Assert.assertTrue(removedInstanceConfigMap.keySet().containsAll(instanceSet));
      }
    }
  }
}
