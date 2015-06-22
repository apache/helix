package org.apache.helix.integration;

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
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixController;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixParticipant;
import org.apache.helix.TestHelper;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.manager.zk.ZkHelixLeaderElection;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Ensure that the external view is able to update properly when participants share a connection.
 */
public class TestSharedConnection extends ZkTestBase {
  /**
   * Ensure that the external view is able to update properly when participants share a connection.
   */
  @Test
  public void testSharedParticipantConnection() throws Exception {
    final int NUM_PARTICIPANTS = 2;
    final int NUM_PARTITIONS = 4;
    final int NUM_REPLICAS = 2;
    final String RESOURCE_NAME = "TestDB0";

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "OnlineOffline", RebalanceMode.CUSTOMIZED, true); // do rebalance

    // Connect
    HelixConnection connection = new ZkHelixConnection(_zkaddr);
    connection.connect();

    // Start some participants
    HelixParticipant[] participants = new HelixParticipant[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      participants[i] =
          connection.createParticipant(ClusterId.from(clusterName),
              ParticipantId.from("localhost_" + (12918 + i)));
      participants[i].getStateMachineEngine().registerStateModelFactory(
          StateModelDefId.from("OnlineOffline"), new TestHelixConnection.MockStateModelFactory());
      participants[i].start();
    }

    // Start the controller
    HelixController controller =
        connection.createController(ClusterId.from(clusterName), ControllerId.from("controller"));
    controller.start();
    Thread.sleep(500);

    // Verify balanced cluster
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // Drop a partition from the first participant
    HelixAdmin admin = connection.createClusterManagementTool();
    IdealState idealState = admin.getResourceIdealState(clusterName, RESOURCE_NAME);
    Map<ParticipantId, State> participantStateMap =
        idealState.getParticipantStateMap(PartitionId.from(RESOURCE_NAME + "_0"));
    participantStateMap.remove(ParticipantId.from("localhost_12918"));
    idealState.setParticipantStateMap(PartitionId.from(RESOURCE_NAME + "_0"), participantStateMap);
    admin.setResourceIdealState(clusterName, RESOURCE_NAME, idealState);
    Thread.sleep(1000);

    // Verify balanced cluster
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // Drop a partition from the second participant
    participantStateMap = idealState.getParticipantStateMap(PartitionId.from(RESOURCE_NAME + "_1"));
    participantStateMap.remove(ParticipantId.from("localhost_12919"));
    idealState.setParticipantStateMap(PartitionId.from(RESOURCE_NAME + "_1"), participantStateMap);
    admin.setResourceIdealState(clusterName, RESOURCE_NAME, idealState);
    Thread.sleep(1000);

    // Verify balanced cluster
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // Clean up
    controller.stop();
    for (HelixParticipant participant : participants) {
      participant.stop();
    }
    admin.dropCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Ensure that only one controller with a shared connection thinks it's leader
   */
  @Test
  public void testSharedControllerConnection() throws Exception {
    final int NUM_PARTICIPANTS = 2;
    final int NUM_PARTITIONS = 4;
    final int NUM_REPLICAS = 2;
    final int NUM_CONTROLLERS = 2;

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "OnlineOffline", RebalanceMode.CUSTOMIZED, true); // do rebalance

    // Connect
    HelixConnection connection = new ZkHelixConnection(_zkaddr);
    connection.connect();

    // Create a couple controllers
    HelixController[] controllers = new HelixController[NUM_CONTROLLERS];
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      controllers[i] =
          connection.createController(ClusterId.from(clusterName),
              ControllerId.from("controller_" + i));
      controllers[i].start();
    }
    Thread.sleep(1000);

    // Now verify that exactly one is leader
    int leaderCount = 0;
    for (HelixController controller : controllers) {
      HelixManager adaptor = new ZKHelixManager(controller);
      boolean result = ZkHelixLeaderElection.tryUpdateController(adaptor);
      if (result) {
        leaderCount++;
      }
    }
    Assert.assertEquals(leaderCount, 1);

    // Clean up
    for (HelixController controller : controllers) {
      controller.stop();
    }
    HelixAdmin admin = connection.createClusterManagementTool();
    admin.dropCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
