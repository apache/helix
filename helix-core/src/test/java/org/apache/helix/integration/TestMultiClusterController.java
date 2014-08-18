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

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMultiClusterController extends ZkTestBase {

  @Test
  public void testBasic() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterNamePrefix = className + "_" + methodName;
    final int n = 5;
    final int clusterNb = 10;

    System.out
        .println("START " + clusterNamePrefix + " at " + new Date(System.currentTimeMillis()));

    // setup 10 clusters
    for (int i = 0; i < clusterNb; i++) {
      String clusterName = clusterNamePrefix + "0_" + i;
      String participantName = "localhost" + i;
      String resourceName = "TestDB" + i;
      TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
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
    TestHelper.setupCluster("CONTROLLER_" + clusterNamePrefix, _zkaddr, 0, // controller
                                                                           // port
        "controller", // participant name prefix
        clusterNamePrefix, // resource name prefix
        1, // resources
        clusterNb, // partitions per resource
        n, // number of nodes
        3, // replicas
        "LeaderStandby", true); // do rebalance

    // start multi-cluster controllers
    MockMultiClusterController[] controllers = new MockMultiClusterController[n];
    for (int i = 0; i < n; i++) {
      controllers[i] =
          new MockMultiClusterController(_zkaddr, controllerClusterName, "controller_" + i);
      controllers[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(
            new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr, controllerClusterName),
            30000);
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // start first cluster
    MockParticipant[] participants = new MockParticipant[n];
    final String firstClusterName = clusterNamePrefix + "0_0";
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost0_" + (12918 + i);
      participants[i] = new MockParticipant(_zkaddr, firstClusterName, instanceName);
      participants[i].syncStart();
    }

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");

    // stop current leader in controller cluster
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(controllerClusterName, _baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    String leaderName = leader.getId();
    int j = Integer.parseInt(leaderName.substring(leaderName.lastIndexOf('_') + 1));
    controllers[j].syncStop();

    // setup the second cluster
    MockParticipant[] participants2 = new MockParticipant[n];
    final String secondClusterName = clusterNamePrefix + "0_1";
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost1_" + (12918 + i);
      participants2[i] = new MockParticipant(_zkaddr, secondClusterName, instanceName);
      participants2[i].syncStart();
    }

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            secondClusterName));
    Assert.assertTrue(result, "second cluster NOT in ideal state");

    // clean up
    // wait for all zk callbacks done
    System.out.println("Cleaning up...");
    for (int i = 0; i < 5; i++) {
      result =
          ClusterStateVerifier
              .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                  controllerClusterName));
      controllers[i].syncStop();
    }

    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterNamePrefix + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testAddAndStop() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterNamePrefix = className + "_" + methodName;
    final int n = 5;
    final int clusterNb = 10;

    System.out
        .println("START " + clusterNamePrefix + " at " + new Date(System.currentTimeMillis()));

    // setup 10 clusters
    for (int i = 0; i < clusterNb; i++) {
      String clusterName = clusterNamePrefix + "0_" + i;
      String participantName = "localhost" + i;
      String resourceName = "TestDB" + i;
      TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
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
    TestHelper.setupCluster("CONTROLLER_" + clusterNamePrefix, _zkaddr, 0, // controller
                                                                           // port
        "controller", // participant name prefix
        clusterNamePrefix, // resource name prefix
        1, // resources
        clusterNb, // partitions per resource
        n, // number of nodes
        3, // replicas
        "LeaderStandby", true); // do rebalance

    // start multi-cluster cluster controllers
    MockMultiClusterController[] controllers = new MockMultiClusterController[n + n];
    for (int i = 0; i < n; i++) {
      controllers[i] =
          new MockMultiClusterController(_zkaddr, controllerClusterName, "controller_" + i);
      controllers[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(
            new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr, controllerClusterName),
            30000);
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // start first cluster
    MockParticipant[] participants = new MockParticipant[n];
    final String firstClusterName = clusterNamePrefix + "0_0";
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost0_" + (12918 + i);
      participants[i] = new MockParticipant(_zkaddr, firstClusterName, instanceName);
      participants[i].syncStart();
    }

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");

    // add more controllers to controller cluster
    for (int i = 0; i < n; i++) {
      String controller = "controller_" + (n + i);
      _setupTool.addInstanceToCluster(controllerClusterName, controller);
    }
    _setupTool.rebalanceStorageCluster(controllerClusterName, clusterNamePrefix + "0", 6);
    for (int i = n; i < 2 * n; i++) {
      controllers[i] =
          new MockMultiClusterController(_zkaddr, controllerClusterName, "controller_" + i);
      controllers[i].syncStart();
    }

    // verify controller cluster
    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                controllerClusterName));
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // verify first cluster
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");

    // stop controller_0-5
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(controllerClusterName, _baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    for (int i = 0; i < n; i++) {
      LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
      String leaderName = leader.getId();
      int j = Integer.parseInt(leaderName.substring(leaderName.lastIndexOf('_') + 1));
      controllers[j].syncStop();

      result =
          ClusterStateVerifier
              .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                  controllerClusterName));
      Assert.assertTrue(result, "Controller cluster NOT in ideal state");

      result =
          ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
              firstClusterName));
      Assert.assertTrue(result, "first cluster NOT in ideal state");
    }

    // clean up
    // wait for all zk callbacks done
    System.out.println("Cleaning up...");
    for (int i = 0; i < 5; i++) {
      result =
          ClusterStateVerifier
              .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                  controllerClusterName));
      controllers[i].syncStop();
    }

    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterNamePrefix + " at " + new Date(System.currentTimeMillis()));

  }
}
