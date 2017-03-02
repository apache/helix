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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestDelayedAutoRebalance extends ZkIntegrationTestBase {
  final int NUM_NODE = 5;
  protected static final int START_PORT = 12918;
  protected static final int _PARTITIONS = 5;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;

  protected ClusterSetup _setupTool = null;
  List<MockParticipantManager> _participants = new ArrayList<MockParticipantManager>();
  int _replica = 3;
  int _minActiveReplica = _replica - 1;
  HelixClusterVerifier _clusterVerifier;
  List<String> _testDBs = new ArrayList<String>();

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(_gZkClient);
    _setupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

      // start dummy participants
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
      participant.syncStart();
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
  }

  protected String[] TestStateModels = {
      BuiltInStateModelDefinitions.MasterSlave.name(),
      BuiltInStateModelDefinitions.OnlineOffline.name(),
      BuiltInStateModelDefinitions.LeaderStandby.name()
  };

  /**
   * The partition movement should be delayed (not happen immediately) after one single node goes offline.
   * Delay is enabled by default, delay time is set in IdealState.
   * @throws Exception
   */
  @Test
  public void testDelayedPartitionMovement() throws Exception {
    Map<String, ExternalView> externalViewsBefore = createTestDBs(1000000);

    // bring down one node, no partition should be moved.
    _participants.get(0).syncStop();
    Thread.sleep(500);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName());
    }
  }

  @Test(dependsOnMethods = {"testDelayedPartitionMovement"})
  public void testDelayedPartitionMovementWithClusterConfigedDelay() throws Exception {
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, 1000000);

    Map<String, ExternalView> externalViewsBefore = createTestDBs(-1);

    // bring down one node, no partition should be moved.
    _participants.get(0).syncStop();
    Thread.sleep(1000);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName());
    }

    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, -1);
  }

  /**
   * Test when two nodes go offline,  the minimal active replica should be maintained.
   * @throws Exception
   */
  @Test(dependsOnMethods = {"testDelayedPartitionMovement"})
  public void testMinimalActiveReplicaMaintain() throws Exception {
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, 1000000);
    Map<String, ExternalView> externalViewsBefore = createTestDBs(-1);

    // bring down one node, no partition should be moved.
    _participants.get(0).syncStop();
    Thread.sleep(500);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName());
    }

    // bring down another node, the minimal active replica for each partition should be maintained.
    _participants.get(3).syncStop();
    Thread.sleep(500);
    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
    }
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, -1);
  }

  /**
   * The partititon should be moved to other nodes after the delay time
   */
  @Test (dependsOnMethods = {"testMinimalActiveReplicaMaintain"})
  public void testPartitionMovementAfterDelayTime() throws Exception {
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    long delay = 4000;
    Map<String, ExternalView> externalViewsBefore = createTestDBs(delay);

    // bring down one node, no partition should be moved.
    _participants.get(0).syncStop();
    Thread.sleep(200);
    Assert.assertTrue(_clusterVerifier.verify());
    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName());
    }

    Thread.sleep(delay + 200);
    Assert.assertTrue(_clusterVerifier.verify());
    // after delay time, it should maintain required number of replicas.
    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _replica);
    }
  }

  @Test (dependsOnMethods = {"testMinimalActiveReplicaMaintain"})
  public void testDisableDelayRebalanceInResource() throws Exception {
    Map<String, ExternalView> externalViewsBefore = createTestDBs(1000000);

    // bring down one node, no partition should be moved.
    _participants.get(0).syncStop();
    Thread.sleep(500);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName());
    }

    // disable delay rebalance for one db, partition should be moved immediately
    String testDb = _testDBs.get(0);
    IdealState idealState = _setupTool.getClusterManagementTool().getResourceIdealState(
        CLUSTER_NAME, testDb);
    idealState.setDelayRebalanceEnabled(false);
    _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, testDb, idealState);
    Thread.sleep(1000);


    // once delay rebalance is disabled, it should maintain required number of replicas for that db.
    // replica for other dbs should not be moved.
    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is =
          _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);

      if (db.equals(testDb)) {
        validateMinActiveAndTopStateReplica(idealState, ev, _replica);
      } else {
        validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
        validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
            _participants.get(0).getInstanceName());
      }
    }
  }

  @Test (dependsOnMethods = {"testDisableDelayRebalanceInResource"})
  public void testDisableDelayRebalanceInCluster() throws Exception {
    enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, true);

    Map<String, ExternalView> externalViewsBefore = createTestDBs(1000000);

    // bring down one node, no partition should be moved.
    _participants.get(0).syncStop();
    Thread.sleep(500);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName());
    }

    // disable delay rebalance for the entire cluster.
    enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, false);
    // TODO: remove this once controller is listening on cluster config change.
    RebalanceScheduler.invokeRebalance(_controller.getHelixDataAccessor(), _testDBs.get(0));
    Thread.sleep(500);
    Assert.assertTrue(_clusterVerifier.verify());
    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(
          CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _replica);
    }

    enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, true);
  }

  @AfterMethod
  public void afterTest() throws InterruptedException {
    // delete all DBs create in last test
    for (String db : _testDBs) {
      _setupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    }
    _testDBs.clear();
    Thread.sleep(50);
  }

  @BeforeMethod
  public void beforeTest() {
    // restart any participant that has been disconnected from last test.
    for (int i = 0; i < _participants.size(); i++) {
      if (!_participants.get(i).isConnected()) {
        _participants.set(i, new MockParticipantManager(ZK_ADDR, CLUSTER_NAME,
            _participants.get(i).getInstanceName()));
        _participants.get(i).syncStart();
      }
    }
  }

  // create test DBs, wait it converged and return externalviews
  private Map<String, ExternalView> createTestDBs(long delayTime) throws InterruptedException {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + i++;
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _minActiveReplica, delayTime);
      _testDBs.add(db);
    }
    Thread.sleep(800);
    Assert.assertTrue(_clusterVerifier.verify());
    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  protected void validateNoPartitionMove(IdealState is, ExternalView evBefore, ExternalView evAfter,
      String offlineInstance) {
    for (String partition : is.getPartitionSet()) {
      Map<String, String> assignmentsBefore = evBefore.getRecord().getMapField(partition);
      Map<String, String> assignmentsAfter = evAfter.getRecord().getMapField(partition);

      Set<String> instancesBefore = new HashSet<String>(assignmentsBefore.keySet());
      Set<String> instancesAfter = new HashSet<String>(assignmentsAfter.keySet());
      instancesBefore.remove(offlineInstance);

      Assert.assertEquals(instancesBefore, instancesAfter, String
          .format("%s has been moved to new instances, before: %s, after: %s, offline instance:",
              partition, assignmentsBefore.toString(), assignmentsAfter.toString(),
              offlineInstance));
    }
  }

  /**
   * Validate there should be always minimal active replica and top state replica for each partition.
   * Also make sure there is always some partitions with only active replica count.
   */
  protected void validateMinActiveAndTopStateReplica(IdealState is, ExternalView ev,
      int minActiveReplica) {
    StateModelDefinition stateModelDef =
        BuiltInStateModelDefinitions.valueOf(is.getStateModelDefRef()).getStateModelDefinition();
    String topState = stateModelDef.getStatesPriorityList().get(0);
    int replica = Integer.valueOf(is.getReplicas());

    Map<String, Integer> stateCount =
        stateModelDef.getStateCountMap(NUM_NODE, replica);
    Set<String> activeStates = stateCount.keySet();

    for (String partition : is.getPartitionSet()) {
      Map<String, String> assignmentMap = ev.getRecord().getMapField(partition);
      Assert.assertNotNull(assignmentMap,
          is.getResourceName() + "'s best possible assignment is null for partition " + partition);
      Assert.assertTrue(!assignmentMap.isEmpty(),
          is.getResourceName() + "'s partition " + partition + " has no best possible map in IS.");

      boolean hasTopState = false;
      int activeReplica = 0;
      for (String state : assignmentMap.values()) {
        if (topState.equalsIgnoreCase(state)) {
          hasTopState = true;
        }
        if (activeStates.contains(state)) {
          activeReplica++;
        }
      }

      Assert.assertTrue(hasTopState, String.format("%s missing %s replica", partition, topState));
      Assert.assertTrue(activeReplica >= minActiveReplica, String
          .format("%s has less active replica %d then required %d", partition, activeReplica,
              minActiveReplica));
    }
  }

  @AfterClass
  public void afterClass() throws Exception {
    /**
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    _setupTool.deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }
}
