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
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConsecutiveZkSessionExpiry extends ZkUnitTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestConsecutiveZkSessionExpiry.class);

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
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    final ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start participants
    CountDownLatch startCountdown = new CountDownLatch(1);
    CountDownLatch endCountdown = new CountDownLatch(1);

    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);

      if (i == 0) {
        participants[i].addPreConnectCallback(new PreConnectTestCallback(instanceName,
            startCountdown, endCountdown));
      }
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(
                new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
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
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDistributedController() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ClusterDistributedController[] distributedControllers = new ClusterDistributedController[n];
    CountDownLatch startCountdown = new CountDownLatch(1);
    CountDownLatch endCountdown = new CountDownLatch(1);

    for (int i = 0; i < n; i++) {
      String contrllerName = "localhost_" + (12918 + i);
      distributedControllers[i] =
          new ClusterDistributedController(ZK_ADDR, clusterName, contrllerName);
      distributedControllers[i].getStateMachineEngine().registerStateModelFactory("MasterSlave",
          new MockMSModelFactory());
      if (i == 0) {
        distributedControllers[i].addPreConnectCallback(new PreConnectTestCallback(contrllerName,
            startCountdown, endCountdown));
      }
      distributedControllers[i].connect();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // expire the session of distributedController
    LOG.info("1st Expiring distributedController session...");
    String oldSessionId = distributedControllers[0].getSessionId();

    ZkTestHelper.asyncExpireSession(distributedControllers[0].getZkClient());
    String newSessionId = distributedControllers[0].getSessionId();
    LOG.info("Expried distributedController session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    // expire zk session again during HelixManager#handleNewSession()
    startCountdown.await();
    LOG.info("2nd Expiring distributedController session...");
    oldSessionId = distributedControllers[0].getSessionId();

    ZkTestHelper.asyncExpireSession(distributedControllers[0].getZkClient());
    newSessionId = distributedControllers[0].getSessionId();
    LOG.info("Expried distributedController session. oldSessionId: " + oldSessionId
        + ", newSessionId: " + newSessionId);

    endCountdown.countDown();

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // verify leader changes to localhost_12919
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNotNull(pollForProperty(LiveInstance.class, accessor,
        keyBuilder.liveInstance("localhost_12918"), true));
    LiveInstance leader =
        pollForProperty(LiveInstance.class, accessor, keyBuilder.controllerLeader(), true);
    Assert.assertNotNull(leader);
    Assert.assertEquals(leader.getId(), "localhost_12919");

    // check localhost_12918 has 2 handlers: message and data-accessor
    LOG.debug("handlers: " + TestHelper.printHandlers(distributedControllers[0]));
    List<CallbackHandler> handlers = distributedControllers[0].getHandlers();
    Assert
        .assertEquals(
            handlers.size(),
            1,
            "Distributed controller should have 1 handler (message) after lose leadership, but was "
                + handlers.size());

    // clean up
    distributedControllers[0].disconnect();
    distributedControllers[1].disconnect();
    Assert.assertNull(pollForProperty(LiveInstance.class, accessor,
        keyBuilder.liveInstance("localhost_12918"), false));
    Assert.assertNull(pollForProperty(LiveInstance.class, accessor,
        keyBuilder.liveInstance("localhost_12919"), false));
    Assert.assertNull(pollForProperty(LiveInstance.class, accessor, keyBuilder.controllerLeader(),
        false));

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

}
