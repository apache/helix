package org.apache.helix.integration.rebalancer.CrushRebalancers;

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
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.MultiRoundCrushRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestNodeSwap extends ZkTestBase {
  final int NUM_NODE = 6;
  protected static final int START_PORT = 12918;
  protected static final int _PARTITIONS = 20;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;

  List<MockParticipantManager> _participants = new ArrayList<>();
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

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
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
      _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, storageNodeName,
          instanceConfig);
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
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
  }

  @DataProvider(name = "rebalanceStrategies")
  public static Object[][] rebalanceStrategies() {
    return new String[][] {
        {
            "CrushRebalanceStrategy", CrushRebalanceStrategy.class.getName()
        }, {
            "MultiRoundCrushRebalanceStrategy", MultiRoundCrushRebalanceStrategy.class.getName()
        }, {
            "CrushEdRebalanceStrategy", CrushEdRebalanceStrategy.class.getName()
        }
    };
  }

  @Test(dataProvider = "rebalanceStrategies")
  public void testNodeSwap(String rebalanceStrategyName, String rebalanceStrategyClass)
      throws Exception {
    System.out.println("Test testNodeSwap for " + rebalanceStrategyName);

    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + rebalanceStrategyName + "-" + i++;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, _PARTITIONS, stateModel,
          IdealState.RebalanceMode.FULL_AUTO + "", rebalanceStrategyClass);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(1000);

    HelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setDeactivatedNodeAwareness(true).setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verify(5000));

    Map<String, ExternalView> record = new HashMap<>();
    for (String db : _allDBs) {
      record.put(db,
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db));
    }

    // swap a node and rebalance for new distribution
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);

    // 1. disable and remove an old node
    MockParticipantManager oldParticipant = _participants.get(0);
    String oldParticipantName = oldParticipant.getInstanceName();

    final InstanceConfig instanceConfig =
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, oldParticipantName);
    // disable the node first
    instanceConfig.setInstanceEnabled(false);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, oldParticipantName,
        instanceConfig);
    Assert.assertTrue(_clusterVerifier.verify(10000));

    // then remove it from topology
    oldParticipant.syncStop();
    Thread.sleep(2000);
    _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, instanceConfig);

    // 2. create new participant with same topology
    String newParticipantName = "RandomParticipant-" + rebalanceStrategyName + "_" + START_PORT;
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, newParticipantName);
    InstanceConfig newConfig = configAccessor.getInstanceConfig(CLUSTER_NAME, newParticipantName);
    newConfig.setDomain(instanceConfig.getDomainAsString());
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME, newParticipantName,
        newConfig);

    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newParticipantName);
    participant.syncStart();
    _participants.add(0, participant);
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
        for (String instance : stateMap.keySet()) {
          String topoName = instance;
          if (instance.equals(newParticipantName)) {
            topoName = oldParticipantName;
          }
          Assert.assertEquals(stateMap.get(instance), oldStateMap.get(topoName));
        }
      }
    }
  }
}
