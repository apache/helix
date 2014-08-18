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

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The controller pipeline will only update ideal states, live instances, and instance configs
 * when the change. However, if a controller loses leadership and subsequently regains it, we need
 * to ensure that the controller can verify its cache. That's what this test is for.
 */
public class TestReelectedPipelineCorrectness extends ZkTestBase {
  private static final int CHECK_INTERVAL = 50;
  private static final int CHECK_TIMEOUT = 10000;

  @Test
  public void testReelection() throws Exception {
    final int NUM_CONTROLLERS = 2;
    final int NUM_PARTICIPANTS = 4;
    final int NUM_PARTITIONS = 8;
    final int NUM_REPLICAS = 2;

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
        "MasterSlave", RebalanceMode.FULL_AUTO, true); // do rebalance

    // configure multi-cluster controllers
    String controllerCluster = clusterName + "_controllers";
    _setupTool.addCluster(controllerCluster, true);
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      _setupTool.addInstanceToCluster(controllerCluster, "controller_" + i);
    }
    _setupTool.activateCluster(clusterName, controllerCluster, true);

    // start participants
    MockParticipant[] participants = new MockParticipant[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      final String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    // start controllers
    MockMultiClusterController[] controllers = new MockMultiClusterController[NUM_CONTROLLERS];
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      controllers[i] =
          new MockMultiClusterController(_zkaddr, controllerCluster, "controller_" + i);
      controllers[i].syncStart();
    }
    Thread.sleep(1000);

    // Ensure a balanced cluster
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // Disable the leader, resulting in a leader election
    HelixDataAccessor accessor = participants[0].getHelixDataAccessor();
    LiveInstance leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    int totalWait = 0;
    while (leader == null && totalWait < CHECK_TIMEOUT) {
      Thread.sleep(CHECK_INTERVAL);
      totalWait += CHECK_INTERVAL;
      leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    }
    if (totalWait >= CHECK_TIMEOUT) {
      Assert.fail("No leader was ever elected!");
    }
    String leaderId = leader.getId();
    String standbyId = (leaderId.equals("controller_0")) ? "controller_1" : "controller_0";
    HelixAdmin admin = _setupTool.getClusterManagementTool();
    admin.enableInstance(controllerCluster, leaderId, false);

    // Stop a participant to make sure that the leader election worked
    Thread.sleep(500);
    participants[0].syncStop();
    Thread.sleep(500);
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // Disable the original standby (leaving 0 active controllers) and kill another participant
    admin.enableInstance(controllerCluster, standbyId, false);
    Thread.sleep(500);
    participants[1].syncStop();

    // Also change the ideal state
    IdealState idealState = admin.getResourceIdealState(clusterName, "TestDB0");
    idealState.setMaxPartitionsPerInstance(1);
    admin.setResourceIdealState(clusterName, "TestDB0", idealState);
    Thread.sleep(500);

    // Also disable an instance in the main cluster
    admin.enableInstance(clusterName, "localhost_12920", false);

    // Re-enable the original leader
    admin.enableInstance(controllerCluster, leaderId, true);

    // Now check that both the ideal state and the live instances are adhered to by the rebalance
    Thread.sleep(500);
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // cleanup
    for (int i = 0; i < NUM_CONTROLLERS; i++) {
      controllers[i].syncStop();
    }
    for (int i = 2; i < NUM_PARTICIPANTS; i++) {
      participants[i].syncStop();
    }

    System.out.println("STOP " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
