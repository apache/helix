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
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkCallbackHandler;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.HelixTestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConsecutiveZkSessionExpiry extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestConsecutiveZkSessionExpiry.class);

  /**
   * make use of PreConnectCallback to insert session expiry during HelixManager#handleNewSession()
   */
  class PreConnectTestCallback implements PreConnectCallback {
    final String instanceName;
    final CountDownLatch startCountDown;
    final CountDownLatch endCountDown;
    int count = 0;

    public PreConnectTestCallback(String instanceName, CountDownLatch startCountdown,
        CountDownLatch endCountdown) {
      this.instanceName = instanceName;
      this.startCountDown = startCountdown;
      this.endCountDown = endCountdown;
    }

    @Override
    public void onPreConnect() {
      // TODO Auto-generated method stub
      LOG.info("handleNewSession for instance: " + instanceName + ", count: " + count);
      if (count++ == 1) {
        startCountDown.countDown();
        LOG.info("wait session expiry to happen");
        try {
          endCountDown.await();
        } catch (Exception e) {
          LOG.error("interrupted in waiting", e);
        }
      }
    }

  }

  @Test
  public void testParticipant() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    final MockController controller = new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    CountDownLatch startCountdown = new CountDownLatch(1);
    CountDownLatch endCountdown = new CountDownLatch(1);

    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);

      if (i == 0) {
        participants[i].addPreConnectCallback(new PreConnectTestCallback(instanceName,
            startCountdown, endCountdown));
      }
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // expire the session of participant
    LOG.info("1st Expiring participant session...");
    String oldSessionId = participants[0].getSessionId();

    ZkTestHelper.asyncExpireSession(participants[0].getZkClient());
    String newSessionId = participants[0].getSessionId();
    LOG.info("Expried participant session. oldSessionId: " + oldSessionId + ", newSessionId: "
        + newSessionId);

    // expire zk session again during HelixManager#handleNewSession()
    startCountdown.await();
    LOG.info("2nd Expiring participant session...");
    oldSessionId = participants[0].getSessionId();

    ZkTestHelper.asyncExpireSession(participants[0].getZkClient());
    newSessionId = participants[0].getSessionId();
    LOG.info("Expried participant session. oldSessionId: " + oldSessionId + ", newSessionId: "
        + newSessionId);

    endCountdown.countDown();

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMultiClusterController() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    MockMultiClusterController[] multiClusterControllers = new MockMultiClusterController[n];
    CountDownLatch startCountdown = new CountDownLatch(1);
    CountDownLatch endCountdown = new CountDownLatch(1);

    for (int i = 0; i < n; i++) {
      String contrllerName = "localhost_" + (12918 + i);
      multiClusterControllers[i] = new MockMultiClusterController(_zkaddr, clusterName, contrllerName);
      multiClusterControllers[i].getStateMachineEngine().registerStateModelFactory(StateModelDefId.MasterSlave,
          new MockMSModelFactory());
      if (i == 0) {
        multiClusterControllers[i].addPreConnectCallback(new PreConnectTestCallback(contrllerName,
            startCountdown, endCountdown));
      }
      multiClusterControllers[i].connect();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // expire the session of multiClusterController
    LOG.info("1st Expiring multiClusterController session...");
    String oldSessionId = multiClusterControllers[0].getSessionId();

    ZkTestHelper.asyncExpireSession(multiClusterControllers[0].getZkClient());
    String newSessionId = multiClusterControllers[0].getSessionId();
    LOG.info("Expried multiClusterController session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    // expire zk session again during HelixManager#handleNewSession()
    startCountdown.await();
    LOG.info("2nd Expiring multiClusterController session...");
    oldSessionId = multiClusterControllers[0].getSessionId();

    ZkTestHelper.asyncExpireSession(multiClusterControllers[0].getZkClient());
    newSessionId = multiClusterControllers[0].getSessionId();
    LOG.info("Expried multiClusterController session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    endCountdown.countDown();

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    // verify leader changes to localhost_12919
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNotNull(HelixTestUtil.pollForProperty(LiveInstance.class, accessor,
        keyBuilder.liveInstance("localhost_12918"), true));
    LiveInstance leader =
        HelixTestUtil.pollForProperty(LiveInstance.class, accessor, keyBuilder.controllerLeader(),
            true);
    Assert.assertNotNull(leader);
    Assert.assertEquals(leader.getId(), "localhost_12919");

    // check localhost_12918 has 2 handlers: message and leader-election
    TestHelper.printHandlers(multiClusterControllers[0], multiClusterControllers[0].getHandlers());
    List<ZkCallbackHandler> handlers = multiClusterControllers[0].getHandlers();
    Assert
        .assertEquals(
            handlers.size(),
            2,
            "MultiCluster controller should have 2 handler (message and leader election) after lose leadership, but was "
                + handlers.size());

    // clean up
    multiClusterControllers[0].disconnect();
    multiClusterControllers[1].disconnect();
    Assert.assertNull(HelixTestUtil.pollForProperty(LiveInstance.class, accessor,
        keyBuilder.liveInstance("localhost_12918"), false));
    Assert.assertNull(HelixTestUtil.pollForProperty(LiveInstance.class, accessor,
        keyBuilder.liveInstance("localhost_12919"), false));
    Assert.assertNull(HelixTestUtil.pollForProperty(LiveInstance.class, accessor,
        keyBuilder.controllerLeader(), false));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

}
