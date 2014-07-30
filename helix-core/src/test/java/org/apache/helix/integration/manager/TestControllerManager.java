package org.apache.helix.integration.manager;

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
import java.util.concurrent.TimeUnit;

import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestControllerManager extends ZkTestBase {

  @Test
  public void testMultipleControllersOfSameName() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        16, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start multiple controllers of same name
    int m = 3;
    MockController[] controllers = new MockController[m];

    for (int i = 0; i < m; i++) {
      controllers[i] = new MockController(_zkaddr, clusterName, "controller");
      controllers[i].syncStart();
    }

    // assert that only one is leader
    int leaderCnt = 0;
    for (int i = 0; i < m; i++) {
      if (controllers[i].isLeader()) {
        leaderCnt++;
      }
    }
    Assert.assertEquals(leaderCnt, 1, "Should have only 1 leader but was " + leaderCnt);

    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);

      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // cleanup
    for (int i = 0; i < m; i++) {
      controllers[i].syncStop();
    }

    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void simpleSessionExpiryTest() throws Exception {
    // Logger.getRootLogger().setLevel(Level.WARN);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;
    int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        16, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);
    String oldSessionId = controller.getSessionId();

    // expire zk-connection on localhost_12918
    ZkTestHelper.expireSession(controller.getZkClient());

    // wait until session expiry callback happens
    TimeUnit.MILLISECONDS.sleep(100);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);
    String newSessionId = controller.getSessionId();
    Assert.assertNotSame(newSessionId, oldSessionId);

    // cleanup
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
