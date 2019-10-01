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

import java.util.Date;

import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestDelayedAutoRebalanceWithRackaware extends TestDelayedAutoRebalance {
  static final int NUM_NODE = 9;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      String zone = "zone-" + i % 3;
      _gSetupTool.getClusterManagementTool().setInstanceZoneId(CLUSTER_NAME, storageNodeName, zone);

      // start dummy participants
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
      participant.syncStart();
      _participants.add(participant);
    }

    enableTopologyAwareRebalance(_gZkClient, CLUSTER_NAME, true);
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _clusterVerifier = getClusterVerifier();
  }

  @Override
  protected IdealState createResourceWithDelayedRebalance(String clusterName, String db,
      String stateModel, int numPartition, int replica, int minActiveReplica, long delay) {
    return createResourceWithDelayedRebalance(clusterName, db, stateModel, numPartition, replica,
        minActiveReplica, delay, CrushRebalanceStrategy.class.getName());
  }

  @Test
  public void testDelayedPartitionMovement() throws Exception {
    super.testDelayedPartitionMovement();
  }

  @Test(dependsOnMethods = {"testDelayedPartitionMovement"})
  public void testDelayedPartitionMovementWithClusterConfigedDelay() throws Exception {
    super.testDelayedPartitionMovementWithClusterConfigedDelay();
  }

  /**
   * Test when two nodes go offline,  the minimal active replica should be maintained.
   * @throws Exception
   */
  @Test(dependsOnMethods = {"testDelayedPartitionMovement"})
  public void testMinimalActiveReplicaMaintain() throws Exception {
    super.testMinimalActiveReplicaMaintain();
  }

  /**
   * The partititon should be moved to other nodes after the delay time
   */
  @Test (dependsOnMethods = {"testMinimalActiveReplicaMaintain"})
  public void testPartitionMovementAfterDelayTime() throws Exception {
    super.testPartitionMovementAfterDelayTime();
  }

  @Test (dependsOnMethods = {"testMinimalActiveReplicaMaintain"})
  public void testDisableDelayRebalanceInResource() throws Exception {
    super.testDisableDelayRebalanceInResource();
  }

  @Test (dependsOnMethods = {"testDisableDelayRebalanceInResource"})
  public void testDisableDelayRebalanceInCluster() throws Exception {
    super.testDisableDelayRebalanceInCluster();
  }

  @Test (dependsOnMethods = {"testDisableDelayRebalanceInCluster"})
  public void testDisableDelayRebalanceInInstance() throws Exception {
    super.testDisableDelayRebalanceInInstance();
  }
}
