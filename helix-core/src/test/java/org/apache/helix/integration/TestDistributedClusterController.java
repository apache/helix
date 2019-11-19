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
import java.util.HashSet;
import java.util.Set;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDistributedClusterController extends ZkTestBase {

  @Test
  public void testDistributedClusterController() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterNamePrefix = className + "_" + methodName;
    final int n = 5;
    final int clusterNb = 10;

    Set<MockParticipantManager> participantRefs = new HashSet<>();

    System.out
        .println("START " + clusterNamePrefix + " at " + new Date(System.currentTimeMillis()));

    // setup 10 clusters
    for (int i = 0; i < clusterNb; i++) {
      String clusterName = clusterNamePrefix + "0_" + i;
      String participantName = "localhost" + i;
      String resourceName = "TestDB" + i;
      TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
          participantName, // participant name prefix
          resourceName, // resource name prefix
          1, // resources
          8, // partitions per resource
          n, // number of nodes
          3, // replicas
          "MasterSlave", true); // do rebalance
    }

    // setup controller cluster
    final String controllerClusterName = "CONTROLLER_" + clusterNamePrefix;
    TestHelper.setupCluster("CONTROLLER_" + clusterNamePrefix, ZK_ADDR, 0, // controller
                                                                           // port
        "controller", // participant name prefix
        clusterNamePrefix, // resource name prefix
        1, // resources
        clusterNb, // partitions per resource
        n, // number of nodes
        3, // replicas
        "LeaderStandby", true); // do rebalance

    // start distributed cluster controllers
    ClusterDistributedController[] controllers = new ClusterDistributedController[n];
    for (int i = 0; i < n; i++) {
      controllers[i] =
          new ClusterDistributedController(ZK_ADDR, controllerClusterName, "controller_" + i);
      controllers[i].syncStart();
    }

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, controllerClusterName),
        30000);
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // start first cluster
    MockParticipantManager[] participants = new MockParticipantManager[n];
    final String firstClusterName = clusterNamePrefix + "0_0";
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost0_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, firstClusterName, instanceName);
      participants[i].syncStart();
      participantRefs.add(participants[i]);
    }

    result = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");

    // stop current leader in controller cluster
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(controllerClusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    String leaderName = leader.getId();
    int j = Integer.parseInt(leaderName.substring(leaderName.lastIndexOf('_') + 1));
    controllers[j].syncStop();

    // setup the second cluster
    MockParticipantManager[] participants2 = new MockParticipantManager[n];
    final String secondClusterName = clusterNamePrefix + "0_1";
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost1_" + (12918 + i);
      participants2[i] = new MockParticipantManager(ZK_ADDR, secondClusterName, instanceName);
      participants2[i].syncStart();
      participantRefs.add(participants2[i]);
    }

    result = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, secondClusterName));
    Assert.assertTrue(result, "second cluster NOT in ideal state");

    // clean up
    // wait for all zk callbacks done
    System.out.println("Cleaning up...");
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(ClusterStateVerifier
          .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, controllerClusterName)));
      controllers[i].syncStop();
    }

    for (MockParticipantManager participant : participantRefs) {
      participant.syncStop();
    }

    // delete 10 clusters
    for (int i = 0; i < clusterNb; i++) {
      deleteCluster(clusterNamePrefix + "0_" + i);
    }
    deleteCluster(controllerClusterName);
  }
}
