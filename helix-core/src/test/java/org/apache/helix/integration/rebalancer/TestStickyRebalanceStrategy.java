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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.StickyRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestStickyRebalanceStrategy extends ZkTestBase {
  static final int NUM_NODE = 18;
  static final int ADDITIONAL_NUM_NODE = 2;
  protected static final int START_PORT = 12918;
  protected static final int PARTITIONS = 2;
  protected static final int REPLICAS = 3;
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  protected ClusterControllerManager _controller;
  protected List<MockParticipantManager> _participants = new ArrayList<>();
  protected List<MockParticipantManager> _additionalParticipants = new ArrayList<>();
  protected Map<String, String> _instanceNameZoneMap = new HashMap<>();
  protected int _minActiveReplica = 0;
  protected ZkHelixClusterVerifier _clusterVerifier;
  protected List<String> _testDBs = new ArrayList<>();
  protected ConfigAccessor _configAccessor;
  protected String[] TestStateModels =
      {BuiltInStateModelDefinitions.MasterSlave.name(), BuiltInStateModelDefinitions.LeaderStandby.name(), BuiltInStateModelDefinitions.OnlineOffline.name()};

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _configAccessor = new ConfigAccessor(_gZkClient);

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      _participants.addAll(addInstance("" + START_PORT + i, "zone-" + i % REPLICAS, true));
    }

    for (int i = NUM_NODE; i < NUM_NODE + ADDITIONAL_NUM_NODE; i++) {
      _additionalParticipants.addAll(
          addInstance("" + START_PORT + i, "zone-" + i % REPLICAS, false));
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_clusterVerifier != null) {
      _clusterVerifier.close();
    }
    /*
      shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @BeforeMethod
  public void beforeTest() {
    // Restart any participants that has been disconnected from last test
    for (int i = 0; i < _participants.size(); i++) {
      if (!_participants.get(i).isConnected()) {
        _participants.set(i, new MockParticipantManager(ZK_ADDR, CLUSTER_NAME,
            _participants.get(i).getInstanceName()));
        _participants.get(i).syncStart();
      }
    }

    // Stop any additional participants that has been added from last test
    for (MockParticipantManager additionalParticipant : _additionalParticipants) {
      if (additionalParticipant.isConnected()) {
        additionalParticipant.syncStop();
      }
    }
  }

  @AfterMethod
  public void afterTest() throws InterruptedException {
    // delete all DBs create in last test
    for (String db : _testDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    }
    _testDBs.clear();
    _clusterVerifier.verifyByPolling();
  }

  @Test
  public void testNoSameZoneAssignment() throws Exception {
    setTopologyAwareAndGlobalMaxPartitionAllowedPerInstanceInCluster(CLUSTER_NAME, 1);
    Map<String, ExternalView> externalViews = createTestDBs();
    for (ExternalView ev : externalViews.values()) {
      Map<String, Map<String, String>> assignments = ev.getRecord().getMapFields();
      Assert.assertNotNull(assignments);
      Assert.assertEquals(assignments.size(), PARTITIONS);
      for (Map<String, String> assignmentMap : assignments.values()) {
        Assert.assertEquals(assignmentMap.keySet().size(), REPLICAS);
        Set<String> zoneSet = new HashSet<>();
        for (String instanceName : assignmentMap.keySet()) {
          zoneSet.add(_instanceNameZoneMap.get(instanceName));
        }
        Assert.assertEquals(zoneSet.size(), REPLICAS);
      }
    }
  }
  @Test
  public void testFirstTimeAssignmentWithNoInitialLiveNodes() throws Exception {
    setTopologyAwareAndGlobalMaxPartitionAllowedPerInstanceInCluster(CLUSTER_NAME, 1);
    // Shut down all the nodes
    for (int i = 0; i < NUM_NODE; i++) {
      _participants.get(i).syncStop();
    }
    // Create resource
    Map<String, ExternalView> externalViewsBefore = createTestDBs();
    // Start all the nodes
    for (int i = 0; i < NUM_NODE; i++) {
      _participants.set(i, new MockParticipantManager(ZK_ADDR, CLUSTER_NAME,
          _participants.get(i).getInstanceName()));
      _participants.get(i).syncStart();
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> externalViewsAfter = new HashMap<>();
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViewsAfter.put(db, ev);
    }
    validateAllPartitionAssigned(externalViewsAfter);
  }

  @Test
  public void testNoPartitionMovementWithNewInstanceAdd() throws Exception {
    setTopologyAwareAndGlobalMaxPartitionAllowedPerInstanceInCluster(CLUSTER_NAME, 1);
    Map<String, ExternalView> externalViewsBefore = createTestDBs();

    // Start more new instances
    for (int i = 0; i < _additionalParticipants.size(); i++) {
      _additionalParticipants.set(i, new MockParticipantManager(ZK_ADDR, CLUSTER_NAME,
          _additionalParticipants.get(i).getInstanceName()));
      _additionalParticipants.get(i).syncStart();
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    // All partition assignment should remain the same
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev);
    }
  }

  @Test
  public void testNoPartitionMovementWithInstanceDown() throws Exception {
    setTopologyAwareAndGlobalMaxPartitionAllowedPerInstanceInCluster(CLUSTER_NAME, 1);
    Map<String, ExternalView> externalViewsBefore = createTestDBs();

    // Shut down 2 instances
    _participants.get(0).syncStop();
    _participants.get(_participants.size() - 1).syncStop();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    // No movement for previous remaining assignment
    Map<String, ExternalView> externalViewsAfter = new HashMap<>();
    Map<String, IdealState> idealStates = new HashMap<>();
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      externalViewsAfter.put(db, ev);
      idealStates.put(db, is);
    }
    validateNoPartitionMoveWithDiffCount(idealStates, externalViewsBefore, externalViewsAfter, 2);
  }

  @Test
  public void testNoPartitionMovementWithInstanceRestart() throws Exception {
    setTopologyAwareAndGlobalMaxPartitionAllowedPerInstanceInCluster(CLUSTER_NAME, 1);
    // Create resource
    Map<String, ExternalView> externalViewsBefore = createTestDBs();
    // Shut down half of the nodes
    for (int i = 0; i < _participants.size(); i++) {
      if (i % 2 == 0) {
        _participants.get(i).syncStop();
      }
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Map<String, ExternalView> externalViewsAfter = new HashMap<>();
    Map<String, IdealState> idealStates = new HashMap<>();
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      externalViewsAfter.put(db, ev);
      idealStates.put(db, is);
    }
    validateNoPartitionMoveWithDiffCount(idealStates, externalViewsBefore, externalViewsAfter,
        NUM_NODE / 2);

    // Start all the nodes
    for (int i = 0; i < _participants.size(); i++) {
      if (!_participants.get(i).isConnected()) {
        _participants.set(i, new MockParticipantManager(ZK_ADDR, CLUSTER_NAME,
            _participants.get(i).getInstanceName()));
        _participants.get(i).syncStart();
      }
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev);
    }
  }

  @Test
  public void testFirstTimeAssignmentWithStackingPlacement() throws Exception {
    setTopologyAwareAndGlobalMaxPartitionAllowedPerInstanceInCluster(CLUSTER_NAME, 2);
    Map<String, ExternalView> externalViewsBefore = createTestDBs();
    validateAllPartitionAssigned(externalViewsBefore);
  }

  @Test
  public void testNoPartitionMovementWithNewInstanceAddWithStackingPlacement() throws Exception {
    setTopologyAwareAndGlobalMaxPartitionAllowedPerInstanceInCluster(CLUSTER_NAME, 2);
    Map<String, ExternalView> externalViewsBefore = createTestDBs();

    // Start more new instances
    for (int i = 0; i < _additionalParticipants.size(); i++) {
      _additionalParticipants.set(i, new MockParticipantManager(ZK_ADDR, CLUSTER_NAME,
          _additionalParticipants.get(i).getInstanceName()));
      _additionalParticipants.get(i).syncStart();
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    // All partition assignment should remain the same
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev);
    }
  }

  @Test
  public void testNoPartitionMovementWithInstanceDownWithStackingPlacement() throws Exception {
    setTopologyAwareAndGlobalMaxPartitionAllowedPerInstanceInCluster(CLUSTER_NAME, 2);
    // Shut down half of the nodes given we allow stacking placement
    for (int i = 0; i < NUM_NODE / 2; i++) {
      _participants.get(i).syncStop();
    }
    Map<String, ExternalView> externalViewsBefore = createTestDBs();

    // Shut down 2 instances
    _participants.get(_participants.size() - 1).syncStop();
    _participants.get(_participants.size() - 2).syncStop();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    // No movement for previous remaining assignment
    Map<String, ExternalView> externalViewsAfter = new HashMap<>();
    Map<String, IdealState> idealStates = new HashMap<>();
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      externalViewsAfter.put(db, ev);
      idealStates.put(db, is);
    }
    validateNoPartitionMoveWithDiffCount(idealStates, externalViewsBefore, externalViewsAfter, 4);
  }

  // create test DBs, wait it converged and return externalviews
  protected Map<String, ExternalView> createTestDBs() throws InterruptedException {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + i++;
      createResourceWithConditionBasedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, REPLICAS,
          _minActiveReplica, StickyRebalanceStrategy.class.getName());
      _testDBs.add(db);
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  protected void validateNoPartitionMove(IdealState is, ExternalView evBefore,
      ExternalView evAfter) {
    for (String partition : is.getPartitionSet()) {
      Map<String, String> assignmentsBefore = evBefore.getRecord().getMapField(partition);
      Map<String, String> assignmentsAfter = evAfter.getRecord().getMapField(partition);

      Set<String> instancesBefore = new HashSet<>(assignmentsBefore.keySet());
      Set<String> instancesAfter = new HashSet<>(assignmentsAfter.keySet());

      Assert.assertEquals(instancesBefore, instancesAfter,
          String.format("%s has been moved to new instances, before: %s, after: %s", partition,
              assignmentsBefore, assignmentsAfter));
    }
  }

  protected void validateNoPartitionMoveWithDiffCount(Map<String, IdealState> idealStates,
      Map<String, ExternalView> externalViewsBefore, Map<String, ExternalView> externalViewsAfter,
      int diffCount) {
    for (Map.Entry<String, IdealState> entry : idealStates.entrySet()) {
      String resourceName = entry.getKey();
      IdealState is = entry.getValue();
      for (String partition : is.getPartitionSet()) {
        Map<String, String> assignmentsBefore =
            externalViewsBefore.get(resourceName).getRecord().getMapField(partition);
        Map<String, String> assignmentsAfter =
            externalViewsAfter.get(resourceName).getRecord().getMapField(partition);

        Set<String> instancesBefore = new HashSet<>(assignmentsBefore.keySet());
        Set<String> instancesAfter = new HashSet<>(assignmentsAfter.keySet());

        if (instancesBefore.size() == instancesAfter.size()) {
          Assert.assertEquals(instancesBefore, instancesAfter,
              String.format("%s has been moved to new instances, before: %s, after: %s", partition,
                  assignmentsBefore, assignmentsAfter));
        } else {
          Assert.assertTrue(instancesBefore.containsAll(instancesAfter),
              String.format("%s has been moved to new instances, before: %s, after: %s", partition,
                  assignmentsBefore, assignmentsAfter));
          diffCount = diffCount - (instancesBefore.size() - instancesAfter.size());
        }
      }
    }
    Assert.assertEquals(diffCount, 0,
        String.format("Partition movement detected, before: %s, after: %s", externalViewsBefore,
            externalViewsAfter));
  }

  private void validateAllPartitionAssigned(Map<String, ExternalView> externalViewsBefore) {
    for (ExternalView ev : externalViewsBefore.values()) {
      Map<String, Map<String, String>> assignments = ev.getRecord().getMapFields();
      Assert.assertNotNull(assignments);
      Assert.assertEquals(assignments.size(), PARTITIONS);
      for (Map<String, String> assignmentMap : assignments.values()) {
        Assert.assertEquals(assignmentMap.keySet().size(), REPLICAS);
      }
    }
  }

  private void setTopologyAwareAndGlobalMaxPartitionAllowedPerInstanceInCluster(String clusterName,
      int maxPartitionAllowed) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterName);
    clusterConfig.setTopology("/zone/host/logicalId");
    clusterConfig.setFaultZoneType("zone");
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setGlobalMaxPartitionAllowedPerInstance(maxPartitionAllowed);
    _configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  private List<MockParticipantManager> addInstance(String instanceNameSuffix, String zone,
      boolean enabled) {
    List<MockParticipantManager> participants = new ArrayList<>();
    String storageNodeName = PARTICIPANT_PREFIX + "_" + instanceNameSuffix;
    _instanceNameZoneMap.put(storageNodeName, zone);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

    String domain =
        String.format("zone=%s,host=%s,logicalId=%s", zone, storageNodeName, instanceNameSuffix);
    InstanceConfig instanceConfig =
        _configAccessor.getInstanceConfig(CLUSTER_NAME, storageNodeName);
    instanceConfig.setDomain(domain);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(CLUSTER_NAME, storageNodeName, instanceConfig);
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
    if (enabled) {
      // start dummy participant
      participant.syncStart();
    }
    participants.add(participant);

    return participants;
  }
}
