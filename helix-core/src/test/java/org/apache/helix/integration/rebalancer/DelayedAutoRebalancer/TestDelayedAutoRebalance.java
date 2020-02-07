package org.apache.helix.integration.rebalancer.DelayedAutoRebalancer;

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

import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestDelayedAutoRebalance extends ZkTestBase {
  final int NUM_NODE = 5;
  protected static final int START_PORT = 12918;
  protected static final int _PARTITIONS = 5;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;

  List<MockParticipantManager> _participants = new ArrayList<>();
  int _replica = 3;
  int _minActiveReplica = _replica - 1;
  ZkHelixClusterVerifier _clusterVerifier;
  List<String> _testDBs = new ArrayList<String>();

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

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

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
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
    validateDelayedMovements(externalViewsBefore);
  }

  @Test(dependsOnMethods = {"testDelayedPartitionMovement"})
  public void testDelayedPartitionMovementWithClusterConfigedDelay() throws Exception {
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, 1000000);
    Map<String, ExternalView> externalViewsBefore = createTestDBs(-1);
    validateDelayedMovements(externalViewsBefore);
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
    validateDelayedMovements(externalViewsBefore);

    // bring down another node, the minimal active replica for each partition should be maintained.
    _participants.get(3).syncStop();
    Thread.sleep(500);
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica, NUM_NODE);
    }
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, -1);
  }

  /**
   * The partition should be moved to other nodes after the delay time
   */
  @Test (dependsOnMethods = {"testMinimalActiveReplicaMaintain"})
  public void testPartitionMovementAfterDelayTime() throws Exception {
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    long delay = 4000;
    Map<String, ExternalView> externalViewsBefore = createTestDBs(delay);
    validateDelayedMovements(externalViewsBefore);

    Thread.sleep(delay + 200);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    // after delay time, it should maintain required number of replicas.
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _replica, NUM_NODE);
    }
  }

  @Test (dependsOnMethods = {"testMinimalActiveReplicaMaintain"})
  public void testDisableDelayRebalanceInResource() throws Exception {
    Map<String, ExternalView> externalViewsBefore = createTestDBs(1000000);
    validateDelayedMovements(externalViewsBefore);

    // disable delay rebalance for one db, partition should be moved immediately
    String testDb = _testDBs.get(0);
    IdealState idealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(
        CLUSTER_NAME, testDb);
    idealState.setDelayRebalanceEnabled(false);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, testDb, idealState);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // once delay rebalance is disabled, it should maintain required number of replicas for that db.
    // replica for other dbs should not be moved.
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);

      if (db.equals(testDb)) {
        validateMinActiveAndTopStateReplica(idealState, ev, _replica, NUM_NODE);
      } else {
        validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica, NUM_NODE);
        validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
            _participants.get(0).getInstanceName(), false);
      }
    }
  }

  @Test (dependsOnMethods = {"testDisableDelayRebalanceInResource"})
  public void testDisableDelayRebalanceInCluster() throws Exception {
    enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, true);

    Map<String, ExternalView> externalViewsBefore = createTestDBs(1000000);
    validateDelayedMovements(externalViewsBefore);

    // disable delay rebalance for the entire cluster.
    enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, false);
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _replica, NUM_NODE);
    }

    enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, true);
  }

  @Test (dependsOnMethods = {"testDisableDelayRebalanceInCluster"})
  public void testDisableDelayRebalanceInInstance() throws Exception {
    Map<String, ExternalView> externalViewsBefore = createTestDBs(1000000);
    validateDelayedMovements(externalViewsBefore);

    String disabledInstanceName = _participants.get(0).getInstanceName();
    enableDelayRebalanceInInstance(_gZkClient, CLUSTER_NAME, disabledInstanceName, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    for (String db : _testDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      Map<String, List<String>> preferenceLists = is.getPreferenceLists();
      for (List<String> instances : preferenceLists.values()) {
        Assert.assertFalse(instances.contains(disabledInstanceName));
      }
    }
    enableDelayRebalanceInInstance(_gZkClient, CLUSTER_NAME, disabledInstanceName, true);
  }

  @AfterMethod
  public void afterTest() throws InterruptedException {
    // delete all DBs create in last test
    for (String db : _testDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
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
  protected Map<String, ExternalView> createTestDBs(long delayTime) throws InterruptedException {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + i++;
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _minActiveReplica, delayTime, CrushRebalanceStrategy.class.getName());
      _testDBs.add(db);
    }
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  protected void validateNoPartitionMove(IdealState is, ExternalView evBefore, ExternalView evAfter,
      String offlineInstance, boolean disabled) {

    for (String partition : is.getPartitionSet()) {
      Map<String, String> assignmentsBefore = evBefore.getRecord().getMapField(partition);
      Map<String, String> assignmentsAfter = evAfter.getRecord().getMapField(partition);

      Set<String> instancesBefore = new HashSet<String>(assignmentsBefore.keySet());
      Set<String> instancesAfter = new HashSet<String>(assignmentsAfter.keySet());

      if (disabled) {
        // the offline node is disabled
        Assert.assertEquals(instancesBefore, instancesAfter, String.format(
            "%s has been moved to new instances, before: %s, after: %s, disabled instance: %s",
            partition, assignmentsBefore.toString(), assignmentsAfter.toString(), offlineInstance));

        if (instancesAfter.contains(offlineInstance)) {
          Assert.assertEquals(assignmentsAfter.get(offlineInstance), "OFFLINE");
        }
      } else {
        // the offline node actually went offline.
        instancesBefore.remove(offlineInstance);
        Assert.assertEquals(instancesBefore, instancesAfter, String.format(
            "%s has been moved to new instances, before: %s, after: %s, offline instance: %s",
            partition, assignmentsBefore.toString(), assignmentsAfter.toString(), offlineInstance));
      }
    }

  }

  private void validateDelayedMovements(Map<String, ExternalView> externalViewsBefore)
      throws InterruptedException {
    _participants.get(0).syncStop();
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    for (String db : _testDBs) {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica, NUM_NODE);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev, _participants.get(0).getInstanceName(), false);
    }
  }

  @AfterClass
  public void afterClass() throws Exception {
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
}
