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

import com.google.common.collect.ImmutableMap;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.util.HelixUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestWagedRebalance extends ZkTestBase {
  protected final int NUM_NODE = 6;
  protected static final int START_PORT = 12918;
  protected static final int PARTITIONS = 20;
  protected static final int TAGS = 2;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;

  List<MockParticipantManager> _participants = new ArrayList<>();
  Map<String, String> _nodeToTagMap = new HashMap<>();
  List<String> _nodes = new ArrayList<>();
  private Set<String> _allDBs = new HashSet<>();
  private int _replica = 3;

  private static String[] _testModels = {
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
      addInstanceConfig(storageNodeName, i, TAGS);
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
  }

  protected void addInstanceConfig(String storageNodeName, int seqNo, int tagCount) {
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    String tag = "tag-" + seqNo % tagCount;
    _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, storageNodeName, tag);
    _nodeToTagMap.put(storageNodeName, tag);
    _nodes.add(storageNodeName);
  }

  @Test
  public void test() throws Exception {
    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica, _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    validate(_replica);

    // Adding 3 more resources
    i = 0;
    for (String stateModel : _testModels) {
      String moreDB = "More-Test-DB-" + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, moreDB, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, moreDB, _replica);
      _allDBs.add(moreDB);

      Thread.sleep(300);

      validate(_replica);
    }

    // Drop the 3 additional resources
    for (int j = 0; j < 3; j++) {
      String moreDB = "More-Test-DB-" + j++;
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, moreDB);
      _allDBs.remove(moreDB);

      Thread.sleep(300);

      validate(_replica);
    }
  }

  /**
   * Use HelixUtil.getIdealAssignmentForWagedFullAuto() to compute the cluster-wide assignment and
   * verify that it matches with the result from the original WAGED rebalancer's algorithm result.
   */
  @Test(dependsOnMethods = "test")
  public void testRebalanceTool() throws InterruptedException {
    // Create resources for testing
    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    validate(_replica);

    // Read cluster parameters from ZK
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    ClusterConfig clusterConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    List<InstanceConfig> instanceConfigs =
        dataAccessor.getChildValues(dataAccessor.keyBuilder().instanceConfigs(), true);
    List<String> liveInstances =
        dataAccessor.getChildNames(dataAccessor.keyBuilder().liveInstances());
    List<IdealState> idealStates =
        dataAccessor.getChildValues(dataAccessor.keyBuilder().idealStates(), true);
    List<ResourceConfig> resourceConfigs =
        dataAccessor.getChildValues(dataAccessor.keyBuilder().resourceConfigs(), true);

    // Verify that utilResult contains the assignment for the resources added
    Map<String, ResourceAssignment> utilResult = HelixUtil
        .getTargetAssignmentForWagedFullAuto(ZK_ADDR, clusterConfig, instanceConfigs, liveInstances,
            idealStates, resourceConfigs);
    Assert.assertNotNull(utilResult);
    Assert.assertEquals(utilResult.size(), idealStates.size());
    for (IdealState idealState : idealStates) {
      Assert.assertTrue(utilResult.containsKey(idealState.getResourceName()));
      StateModelDefinition stateModelDefinition =
          BuiltInStateModelDefinitions.valueOf(idealState.getStateModelDefRef())
              .getStateModelDefinition();
      for (String partition : idealState.getPartitionSet()) {
        Assert.assertEquals(
            utilResult.get(idealState.getResourceName()).getRecord().getMapField(partition),
            HelixUtil
                .computeIdealMapping(idealState.getPreferenceList(partition), stateModelDefinition,
                    new HashSet<>(liveInstances)));
      }
    }

    // Verify that the partition state mapping mode also works
    Map<String, ResourceAssignment> paritionMappingBasedResult = HelixUtil
        .getImmediateAssignmentForWagedFullAuto(ZK_ADDR, clusterConfig, instanceConfigs,
            liveInstances, idealStates, resourceConfigs);
    Assert.assertNotNull(paritionMappingBasedResult);
    Assert.assertEquals(paritionMappingBasedResult.size(), idealStates.size());
    for (IdealState idealState : idealStates) {
      Assert.assertTrue(paritionMappingBasedResult.containsKey(idealState.getResourceName()));
      Assert.assertEquals(
          paritionMappingBasedResult.get(idealState.getResourceName()).getRecord().getMapFields(),
          idealState.getRecord().getMapFields());
    }

    // Try to add a few extra instances
    String instance_0 = "instance_0";
    String instance_1 = "instance_1";
    Set<String> newInstances = new HashSet<>();
    newInstances.add(instance_0);
    newInstances.add(instance_1);
    liveInstances.addAll(newInstances);
    for (String instance : newInstances) {
      InstanceConfig instanceConfig = new InstanceConfig(instance);
      instanceConfigs.add(instanceConfig);
    }

    utilResult = HelixUtil
        .getTargetAssignmentForWagedFullAuto(ZK_ADDR, clusterConfig, instanceConfigs, liveInstances,
            idealStates, resourceConfigs);

    Set<String> instancesWithAssignments = new HashSet<>();
    utilResult.values().forEach(
        resourceAssignment -> resourceAssignment.getRecord().getMapFields().values()
            .forEach(entry -> instancesWithAssignments.addAll(entry.keySet())));
    // The newly added instances should contain some partitions
    Assert.assertTrue(instancesWithAssignments.contains(instance_0));
    Assert.assertTrue(instancesWithAssignments.contains(instance_1));
  }

  @Test(dependsOnMethods = "test")
  public void testWithInstanceTag() throws Exception {
    Set<String> tags = new HashSet<String>(_nodeToTagMap.values());
    int i = 3;
    for (String tag : tags) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + i++;
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

  @Test(dependsOnMethods = "test")
  public void testChangeIdealState() throws InterruptedException {
    String dbName = "Test-DB-" + TestHelper.getTestMethodName();
    createResourceWithWagedRebalance(CLUSTER_NAME, dbName,
        BuiltInStateModelDefinitions.MasterSlave.name(), PARTITIONS, _replica, _replica);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, _replica);
    _allDBs.add(dbName);
    Thread.sleep(300);

    validate(_replica);

    // Adjust the replica count
    IdealState is =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, dbName);
    int newReplicaFactor = _replica - 1;
    is.setReplicas("" + newReplicaFactor);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, dbName, is);
    Thread.sleep(300);

    validate(newReplicaFactor);

    // Adjust the partition list
    is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, dbName);
    is.setNumPartitions(PARTITIONS + 1);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, dbName, is);
    _gSetupTool.getClusterManagementTool().rebalance(CLUSTER_NAME, dbName, newReplicaFactor);
    Thread.sleep(300);

    validate(newReplicaFactor);
    ExternalView ev =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, dbName);
    Assert.assertEquals(ev.getPartitionSet().size(), PARTITIONS + 1);
  }

  @Test(dependsOnMethods = "test")
  public void testDisableInstance() throws InterruptedException {
    String dbName = "Test-DB-" + TestHelper.getTestMethodName();
    createResourceWithWagedRebalance(CLUSTER_NAME, dbName,
        BuiltInStateModelDefinitions.MasterSlave.name(), PARTITIONS, _replica, _replica);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, _replica);
    _allDBs.add(dbName);
    Thread.sleep(300);

    validate(_replica);

    // Disable participants, keep only three left
    Set<String> disableParticipants = new HashSet<>();

    try {
      for (int i = 3; i < _participants.size(); i++) {
        MockParticipantManager p = _participants.get(i);
        disableParticipants.add(p.getInstanceName());
        InstanceConfig config = _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, p.getInstanceName());
        config.setInstanceEnabled(false);
        _gSetupTool.getClusterManagementTool()
            .setInstanceConfig(CLUSTER_NAME, p.getInstanceName(), config);
      }
      Thread.sleep(300);

      validate(_replica);

      // Verify there is no assignment on the disabled participants.
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, dbName);
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> replicaStateMap = ev.getStateMap(partition);
        for (String instance : replicaStateMap.keySet()) {
          Assert.assertFalse(disableParticipants.contains(instance));
        }
      }
    } finally {
      // recover the config
      for (String instanceName : disableParticipants) {
        InstanceConfig config =
            _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceName);
        config.setInstanceEnabled(true);
        _gSetupTool.getClusterManagementTool()
            .setInstanceConfig(CLUSTER_NAME, instanceName, config);
      }
    }
  }

  @Test(dependsOnMethods = "testDisableInstance")
  public void testLackEnoughLiveInstances() throws Exception {
    // shutdown participants, keep only two left
    for (int i = 2; i < _participants.size(); i++) {
      _participants.get(i).syncStop();
    }

    int j = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + j++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }

    Thread.sleep(300);
    // Verify if the partitions get assigned
    validate(2);

    // restart the participants within the zone
    for (int i = 2; i < _participants.size(); i++) {
      MockParticipantManager p = _participants.get(i);
      MockParticipantManager newNode =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, p.getInstanceName());
      _participants.set(i, newNode);
      newNode.syncStart();
    }

    Thread.sleep(300);
    // Verify if the partitions get assigned
    validate(_replica);
  }

  @Test(dependsOnMethods = "testDisableInstance")
  public void testLackEnoughInstances() throws Exception {
    // shutdown participants, keep only two left
    for (int i = 2; i < _participants.size(); i++) {
      MockParticipantManager p = _participants.get(i);
      p.syncStop();
      _gSetupTool.getClusterManagementTool()
          .enableInstance(CLUSTER_NAME, p.getInstanceName(), false);
      _gSetupTool.dropInstanceFromCluster(CLUSTER_NAME, p.getInstanceName());

    }

    int j = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + j++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }

    Thread.sleep(300);
    // Verify if the partitions get assigned
    validate(2);

    // Create new participants within the zone
    for (int i = 2; i < _participants.size(); i++) {
      MockParticipantManager p = _participants.get(i);
      String replaceNodeName = p.getInstanceName() + "-replacement_" + START_PORT;
      addInstanceConfig(replaceNodeName, i, TAGS);
      MockParticipantManager newNode =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, replaceNodeName);
      _participants.set(i, newNode);
      newNode.syncStart();
    }

    Thread.sleep(300);
    // Verify if the partitions get assigned
    validate(_replica);
  }

  @Test(dependsOnMethods = "test")
  public void testMixedRebalancerUsage() throws InterruptedException {
    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + i++;
      if (i == 0) {
        _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, PARTITIONS, stateModel,
            IdealState.RebalanceMode.FULL_AUTO + "", CrushRebalanceStrategy.class.getName());
      } else if (i == 1) {
        _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, PARTITIONS, stateModel,
            IdealState.RebalanceMode.FULL_AUTO + "", CrushEdRebalanceStrategy.class.getName());
      } else {
        createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
            _replica);
      }
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    validate(_replica);
  }

  @Test(dependsOnMethods = "test")
  public void testMaxPartitionLimitation() throws Exception {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    // Change the cluster level config so no assignment can be done
    clusterConfig.setMaxPartitionsPerInstance(1);
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    try {
      String limitedResourceName = null;
      int i = 0;
      for (String stateModel : _testModels) {
        String db = "Test-DB-" + TestHelper.getTestMethodName() + i++;
        createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
            _replica);
        if (i == 1) {
          // The limited resource has additional limitation.
          // The other resources could have been assigned in theory if the WAGED rebalancer were
          // not used.
          // However, with the WAGED rebalancer, this restricted resource will block the other ones.
          limitedResourceName = db;
          IdealState idealState =
              _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
          idealState.setMaxPartitionsPerInstance(1);
          _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, idealState);
        }
        _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
        _allDBs.add(db);
      }
      Thread.sleep(300);

      // Since the WAGED rebalancer need to finish rebalancing every resources, the initial
      // assignment won't show.
      Assert.assertFalse(TestHelper.verify(() -> _allDBs.stream().anyMatch(db -> {
        ExternalView ev =
            _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
        return ev != null && !ev.getPartitionSet().isEmpty();
      }), 2000));

      // Remove the cluster level limitation
      clusterConfig.setMaxPartitionsPerInstance(-1);
      configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
      Thread.sleep(300);

      // Since the WAGED rebalancer need to finish rebalancing every resources, the assignment won't
      // show even removed cluster level restriction
      Assert.assertFalse(TestHelper.verify(() -> _allDBs.stream().anyMatch(db -> {
        ExternalView ev =
            _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
        return ev != null && !ev.getPartitionSet().isEmpty();
      }), 2000));

      // Remove the resource level limitation
      IdealState idealState = _gSetupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, limitedResourceName);
      idealState.setMaxPartitionsPerInstance(Integer.MAX_VALUE);
      _gSetupTool.getClusterManagementTool()
          .setResourceIdealState(CLUSTER_NAME, limitedResourceName, idealState);

      validate(_replica);
    } finally {
      // recover the config change
      clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
      clusterConfig.setMaxPartitionsPerInstance(-1);
      configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    }
  }

  @Test(dependsOnMethods = "test")
  public void testNewInstances() throws InterruptedException {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setGlobalRebalancePreference(ImmutableMap
        .of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 0,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 10));
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica, _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    Thread.sleep(300);
    validate(_replica);

    String newNodeName = "newNode-" + TestHelper.getTestMethodName() + "_" + START_PORT;
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newNodeName);
    try {
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, newNodeName);
      participant.syncStart();

      Thread.sleep(300);
      validate(_replica);

      Assert.assertFalse(_allDBs.stream().anyMatch(db -> {
        ExternalView ev =
            _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
        for (String partition : ev.getPartitionSet()) {
          if (ev.getStateMap(partition).containsKey(newNodeName)) {
            return true;
          }
        }
        return false;
      }));

      clusterConfig.setGlobalRebalancePreference(ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE);
      configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
      Thread.sleep(300);
      validate(_replica);

      Assert.assertTrue(_allDBs.stream().anyMatch(db -> {
        ExternalView ev =
            _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
        for (String partition : ev.getPartitionSet()) {
          if (ev.getStateMap(partition).containsKey(newNodeName)) {
            return true;
          }
        }
        return false;
      }));
    } finally {
      if (participant != null && participant.isConnected()) {
        participant.syncStop();
      }
    }
  }

  /**
   * The stateful WAGED rebalancer will be reset while the controller regains the leadership.
   * This test is to verify if the reset has been done and the rebalancer has forgotten any previous
   * status after leadership switched.
   */
  @Test(dependsOnMethods = "test")
  public void testRebalancerReset() throws Exception {
    // Configure the rebalance preference so as to trigger more partition movements for evenness.
    // This is to ensure the controller will try to move something if the rebalancer has been reset.
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setGlobalRebalancePreference(ImmutableMap
        .of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 10,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 0));
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-DB-" + TestHelper.getTestMethodName() + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    // TODO remove this sleep after fix https://github.com/apache/helix/issues/526
    Thread.sleep(300);
    validate(_replica);

    // Adding one more resource. Since it is added after the other resources, the assignment is
    // impacted because of the other resources' assignment.
    String moreDB = "More-Test-DB";
    createResourceWithWagedRebalance(CLUSTER_NAME, moreDB,
        BuiltInStateModelDefinitions.MasterSlave.name(), PARTITIONS, _replica, _replica);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, moreDB, _replica);
    _allDBs.add(moreDB);
    // TODO remove this sleep after fix https://github.com/apache/helix/issues/526
    Thread.sleep(300);
    validate(_replica);
    ExternalView oldEV =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, moreDB);

    _controller.handleNewSession();
    // Trigger a rebalance to test if the rebalancer calculate with empty cache states.
    RebalanceScheduler.invokeRebalance(_controller.getHelixDataAccessor(), moreDB);

    // After reset done, the rebalancer will try to rebalance all the partitions since it has
    // forgotten the previous state.
    // TODO remove this sleep after fix https://github.com/apache/helix/issues/526
    Thread.sleep(300);
    validate(_replica);
    ExternalView newEV =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, moreDB);

    // To verify that the controller has moved some partitions.
    Assert.assertFalse(newEV.equals(oldEV));
  }

  private void validate(int expectedReplica) {
    HelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setDeactivatedNodeAwareness(true).setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verify(5000));
    for (String db : _allDBs) {
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      validateIsolation(is, ev, expectedReplica);
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
    Thread.sleep(100);
    ZkHelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setDeactivatedNodeAwareness(true).setResources(_allDBs).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (MockParticipantManager p : _participants) {
      if (p != null && p.isConnected()) {
        p.syncStop();
      }
    }
    deleteCluster(CLUSTER_NAME);
  }
}
