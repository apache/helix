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

import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestDelayedAutoRebalanceWithDisabledInstance extends TestDelayedAutoRebalance {
  private ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _configAccessor = new ConfigAccessor(_gZkClient);
  }


  /**
   * The partition movement should be delayed (not happen immediately) after one single node is disabled.
   * Delay is enabled by default, delay time is set in IdealState.
   * @throws Exception
   */
  @Test
  public void testDelayedPartitionMovement() throws Exception {
    Map<String, ExternalView> externalViewsBefore = createTestDBs(1000000);

    // Disable one node, no partition should be moved.
    String instance = _participants.get(0).getInstanceName();
    enableInstance(instance, false);

    Thread.sleep(300);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev, instance, true);
    }
  }

  @Test(dependsOnMethods = {"testDelayedPartitionMovement"})
  public void testDelayedPartitionMovementWithClusterConfigedDelay() throws Exception {
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, 1000000);

    Map<String, ExternalView> externalViewsBefore = createTestDBs(-1);

    // Disable one node, no partition should be moved.
    String instance = _participants.get(0).getInstanceName();
    enableInstance(instance, false);

    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName(), true);
    }

    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, -1);
  }

  /**
   * Test when two nodes were disabled,  the minimal active replica should be maintained.
   * @throws Exception
   */
  @Test(dependsOnMethods = {"testDelayedPartitionMovement"})
  public void testMinimalActiveReplicaMaintain() throws Exception {
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, 1000000);
    Map<String, ExternalView> externalViewsBefore = createTestDBs(-1);

    // disable one node, no partition should be moved.
    enableInstance(_participants.get(0).getInstanceName(), false);
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName(), true);
    }

    // disable another node, the minimal active replica for each partition should be maintained.
    enableInstance(_participants.get(3).getInstanceName(), false);
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
    }
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, -1);
  }

  /**
   * Test when one node is disable while another node is offline, the minimal active replica should be maintained.
   * @throws Exception
   */
  @Test(dependsOnMethods = {"testDelayedPartitionMovement"})
  public void testMinimalActiveReplicaMaintainWithOneOffline() throws Exception {
    setDelayTimeInCluster(_gZkClient, CLUSTER_NAME, 1000000);
    Map<String, ExternalView> externalViewsBefore = createTestDBs(-1);

    // disable one node, no partition should be moved.
    enableInstance(_participants.get(0).getInstanceName(), false);
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName(), true);
    }

    // bring down another node, the minimal active replica for each partition should be maintained.
    _participants.get(3).syncStop();
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verify());

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

    // disable one node, no partition should be moved.
    enableInstance(_participants.get(0).getInstanceName(), false);
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verify());
    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName(), true);
    }

    Thread.sleep(delay + 200);
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

    // disable one node, no partition should be moved.
    enableInstance(_participants.get(0).getInstanceName(), false);
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName(), true);
    }

    // disable delay rebalance for one db, partition should be moved immediately
    String testDb = _testDBs.get(0);
    IdealState idealState = _setupTool.getClusterManagementTool().getResourceIdealState(
        CLUSTER_NAME, testDb);
    idealState.setDelayRebalanceEnabled(false);
    _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, testDb, idealState);
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verify());

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
            _participants.get(0).getInstanceName(), true);
      }
    }
  }

  @Test (dependsOnMethods = {"testDisableDelayRebalanceInResource"})
  public void testDisableDelayRebalanceInCluster() throws Exception {
    enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, true);

    Map<String, ExternalView> externalViewsBefore = createTestDBs(1000000);

    // disable one node, no partition should be moved.
    enableInstance(_participants.get(0).getInstanceName(), false);
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verify());

    for (String db : _testDBs) {
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      validateMinActiveAndTopStateReplica(is, ev, _minActiveReplica);
      validateNoPartitionMove(is, externalViewsBefore.get(db), ev,
          _participants.get(0).getInstanceName(), true);
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

  @BeforeMethod
  public void beforeTest() {
    // restart any participant that has been disconnected from last test.
    for (int i = 0; i < _participants.size(); i++) {
      if (!_participants.get(i).isConnected()) {
        _participants.set(i, new MockParticipantManager(ZK_ADDR, CLUSTER_NAME,
            _participants.get(i).getInstanceName()));
        _participants.get(i).syncStart();
      }
      enableInstance(_participants.get(i).getInstanceName(), true);
    }
  }

  private void enableInstance(String instance, boolean enabled) {
    // Disable one node, no partition should be moved.
    long currentTime = System.currentTimeMillis();
    _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instance, enabled);
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
    Assert.assertEquals(instanceConfig.getInstanceEnabled(), enabled);
    Assert.assertTrue(instanceConfig.getInstanceEnabledTime() >= currentTime);
    Assert.assertTrue(instanceConfig.getInstanceEnabledTime() <= currentTime + 100);
  }
}
