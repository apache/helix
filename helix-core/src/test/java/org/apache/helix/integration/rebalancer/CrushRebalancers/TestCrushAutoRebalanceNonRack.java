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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.*;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

public class TestCrushAutoRebalanceNonRack extends ZkStandAloneCMTestBase {
  final int NUM_NODE = 6;
  protected static final int START_PORT = 12918;
  protected static final int _PARTITIONS = 20;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;

  List<MockParticipantManager> _participants = new ArrayList<MockParticipantManager>();
  Map<String, String> _nodeToTagMap = new HashMap<String, String>();
  List<String> _nodes = new ArrayList<>();
  Set<String> _allDBs = new HashSet<>();
  int _replica = 3;

  private static String[] _testModels = { BuiltInStateModelDefinitions.OnlineOffline.name(),
      BuiltInStateModelDefinitions.MasterSlave.name(),
      BuiltInStateModelDefinitions.LeaderStandby.name()
  };

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology("/instance");
    clusterConfig.setFaultZoneType("instance");
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      _nodes.add(storageNodeName);
      String tag = "tag-" + i % 2;
      _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, storageNodeName, tag);
      _nodeToTagMap.put(storageNodeName, tag);
      InstanceConfig instanceConfig =
          configAccessor.getInstanceConfig(CLUSTER_NAME, storageNodeName);
      instanceConfig.setDomain("instance=" + storageNodeName);
      configAccessor.setInstanceConfig(CLUSTER_NAME, storageNodeName, instanceConfig);
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
    //enableTopologyAwareRebalance(_gZkClient, CLUSTER_NAME, true);
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
    super.afterClass();
  }

  @DataProvider(name = "rebalanceStrategies")
  public static String[][] rebalanceStrategies() {
    return new String[][] { { "CrushRebalanceStrategy", CrushRebalanceStrategy.class.getName() },
        {"CrushEdRebalanceStrategy", CrushEdRebalanceStrategy.class.getName()}
    };
  }

  @Test(dataProvider = "rebalanceStrategies", enabled = true)
  public void test(String rebalanceStrategyName, String rebalanceStrategyClass) throws Exception {
    System.out.println("Test " + rebalanceStrategyName);
    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + rebalanceStrategyName + "-" + i++;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, _PARTITIONS, stateModel,
          RebalanceMode.FULL_AUTO + "", rebalanceStrategyClass);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    HelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verify(5000));
    for (String db : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      validateIsolation(is, ev, _replica);
    }
  }

  @Test(dataProvider = "rebalanceStrategies", enabled = true, dependsOnMethods = "test")
  public void testWithInstanceTag(String rebalanceStrategyName, String rebalanceStrategyClass)
      throws Exception {
    Set<String> tags = new HashSet<String>(_nodeToTagMap.values());
    int i = 3;
    for (String tag : tags) {
      String db = "Test-DB-" + rebalanceStrategyName + "-" + i++;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, _PARTITIONS,
          BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO + "",
          rebalanceStrategyClass);
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      is.setInstanceGroupTag(tag);
      _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, is);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    HelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verify(5000));
    for (String db : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      validateIsolation(is, ev, _replica);
    }
  }

  @Test(dataProvider = "rebalanceStrategies", enabled = true, dependsOnMethods = {
      "testWithInstanceTag"
  })
  public void testLackEnoughLiveInstances(String rebalanceStrategyName,
      String rebalanceStrategyClass) throws Exception {
    System.out.println("TestLackEnoughLiveInstances " + rebalanceStrategyName);
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    // shutdown participants, keep only two left
    for (int i = 2; i < _participants.size(); i++) {
      _participants.get(i).syncStop();
    }

    int j = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + rebalanceStrategyName + "-" + j++;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, _PARTITIONS, stateModel,
          RebalanceMode.FULL_AUTO + "", rebalanceStrategyClass);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    HelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verify(5000));
    for (String db : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      validateIsolation(is, ev, 2);
    }

    for (int i = 2; i < _participants.size(); i++) {
      _participants.get(i).syncStart();
    }
  }

  @Test(dataProvider = "rebalanceStrategies", enabled = true, dependsOnMethods = {
      "testLackEnoughLiveInstances"
  })
  public void testLackEnoughInstances(String rebalanceStrategyName, String rebalanceStrategyClass)
      throws Exception {
    System.out.println("TestLackEnoughInstances " + rebalanceStrategyName);
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    // shutdown participants, keep only two left
    for (int i = 2; i < _participants.size(); i++) {
      MockParticipantManager p = _participants.get(i);
      p.syncStop();
      _gSetupTool.getClusterManagementTool()
          .enableInstance(CLUSTER_NAME, p.getInstanceName(), false);
      Thread.sleep(200);
      _gSetupTool.dropInstanceFromCluster(CLUSTER_NAME, p.getInstanceName());
    }

    int j = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + rebalanceStrategyName + "-" + j++;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, _PARTITIONS, stateModel,
          RebalanceMode.FULL_AUTO + "", rebalanceStrategyClass);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);
    HelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verify());
    for (String db : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      validateIsolation(is, ev, 2);
    }

    // recover test environment
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    for (int i = 2; i < _participants.size(); i++) {
      String storageNodeName = _participants.get(i).getInstanceName();
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

      InstanceConfig instanceConfig =
          configAccessor.getInstanceConfig(CLUSTER_NAME, storageNodeName);
      instanceConfig.setDomain("instance=" + storageNodeName);
      configAccessor.setInstanceConfig(CLUSTER_NAME, storageNodeName, instanceConfig);

      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
      participant.syncStart();
      _participants.set(i, participant);
    }
  }

  /**
   * Validate each partition is different instances and with necessary tagged instances.
   */
  private void validateIsolation(IdealState is, ExternalView ev, int expectedReplica) {
    String tag = is.getInstanceGroupTag();
    for (String partition : is.getPartitionSet()) {
      Map<String, String> assignmentMap = ev.getRecord().getMapField(partition);
      Set<String> instancesInEV = assignmentMap.keySet();
      Assert.assertEquals(instancesInEV.size(), expectedReplica);
      for (String instance : instancesInEV) {
        if (tag != null) {
          InstanceConfig config =
              _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instance);
          Assert.assertTrue(config.containsTag(tag));
        }
      }
    }
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    for (String db : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    }
    _allDBs.clear();
    // waiting for all DB be dropped.
    Thread.sleep(200);
  }
}
