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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkReconnect {
  private static final Logger LOG = Logger.getLogger(TestZkReconnect.class);

  @Test
  public void testZKReconnect() throws Exception {
    final AtomicReference<ZkServer> zkServerRef = new AtomicReference<ZkServer>();
    final int zkPort = TestHelper.getRandomPort();
    final String zkAddr = String.format("localhost:%d", zkPort);
    ZkServer zkServer = TestHelper.startZkServer(zkAddr);
    zkServerRef.set(zkServer);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    // Setup cluster
    LOG.info("Setup clusters");
    ClusterSetup clusterSetup = new ClusterSetup(zkAddr);
    clusterSetup.addCluster(clusterName, true);

    // Registers and starts controller
    LOG.info("Starts controller");
    HelixManager controller =
        HelixManagerFactory.getZKHelixManager(clusterName, null, InstanceType.CONTROLLER, zkAddr);
    controller.connect();

    // Registers and starts participant
    LOG.info("Starts participant");
    String hostname = "localhost";
    String instanceId = String.format("%s_%d", hostname, 1);
    clusterSetup.addInstanceToCluster(clusterName, instanceId);
    HelixManager participant =
        HelixManagerFactory.getZKHelixManager(clusterName, instanceId, InstanceType.PARTICIPANT,
            zkAddr);
    participant.connect();

    LOG.info("Register state machine");
    final CountDownLatch latch = new CountDownLatch(1);
    participant.getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new StateModelFactory<StateModel>() {
          @Override
          public StateModel createNewStateModel(String resource, String stateUnitKey) {
            return new SimpleStateModel(latch);
          }
        }, "test");

    String resourceName = "test-resource";
    LOG.info("Ideal state assignment");
    HelixAdmin helixAdmin = participant.getClusterManagmentTool();
    helixAdmin.addResource(clusterName, resourceName, 1, "OnlineOffline",
        IdealState.RebalanceMode.CUSTOMIZED.toString());

    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resourceName);
    idealState.setReplicas("1");
    idealState.setStateModelFactoryName("test");
    idealState.setPartitionState(resourceName + "_0", instanceId, "ONLINE");

    LOG.info("Shutdown ZK server");
    TestHelper.stopZkServer(zkServerRef.get());
    Executors.newSingleThreadScheduledExecutor().schedule(new Runnable() {

      @Override
      public void run() {
        try {
          LOG.info("Restart ZK server");
          // zkServer.set(TestUtils.startZookeeper(zkDir, zkPort));
          zkServerRef.set(TestHelper.startZkServer(zkAddr, null, false));
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }, 2L, TimeUnit.SECONDS);

    // future.get();

    LOG.info("Before update ideal state");
    helixAdmin.setResourceIdealState(clusterName, resourceName, idealState);
    LOG.info("After update ideal state");

    LOG.info("Wait for OFFLINE->ONLINE state transition");
    try {
      Assert.assertTrue(latch.await(15, TimeUnit.SECONDS));

      // wait until stable state
      boolean result =
          ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(zkAddr,
              clusterName));
      Assert.assertTrue(result);

    } finally {
      participant.disconnect();
      zkServerRef.get().shutdown();
    }
  }

  @Test
  public void testZKDisconnectCallback() throws Exception {
    final int zkPort = TestHelper.getRandomPort();
    final String zkAddr = String.format("localhost:%d", zkPort);
    final ZkServer zkServer = TestHelper.startZkServer(zkAddr);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    // Init flag to check if callback is triggered
    final AtomicReference<Boolean> flag = new AtomicReference<Boolean>(false);

    // Setup cluster
    LOG.info("Setup clusters");
    ClusterSetup clusterSetup = new ClusterSetup(zkAddr);
    clusterSetup.addCluster(clusterName, true);
    // For fast test, set short timeout
    System.setProperty("zk.connection.timeout", "2000");
    System.setProperty("zk.connectionReEstablishment.timeout", "1000");

    // Registers and starts controller, register listener for disconnect handling
    LOG.info("Starts controller");
    final ZKHelixManager controller =
        (ZKHelixManager) HelixManagerFactory.getZKHelixManager(clusterName, null, InstanceType.CONTROLLER, zkAddr,
            new HelixManagerStateListener() {
              @Override
              public void onConnected(HelixManager helixManager) throws Exception {
                return;
              }

              @Override
              public void onDisconnected(HelixManager helixManager, Throwable error) throws Exception {
                Assert.assertEquals(helixManager.getClusterName(), clusterName);
                flag.getAndSet(true);
              }
            });

    try {
      controller.connect();
      ZkHelixPropertyStore propertyStore = controller.getHelixPropertyStore();

      // 1. shutdown zkServer and check if handler trigger callback
      zkServer.shutdown();
      // Retry will fail, and flag should be set within onDisconnected handler
      controller.handleSessionEstablishmentError(new Exception("For testing"));
      Assert.assertTrue(flag.get());

      try {
        propertyStore.get("/", null, 0);
        Assert.fail("propertyStore should be disconnected.");
      } catch (IllegalStateException e) {
        // Expected exception
        System.out.println(e.getMessage());
      }

      // 2. restart zkServer and check if handler will recover connection
      flag.getAndSet(false);
      zkServer.start();
      // Retry will succeed, and flag should not be set
      controller.handleSessionEstablishmentError(new Exception("For testing"));
      Assert.assertFalse(flag.get());
      // New propertyStore should be in good state
      propertyStore = controller.getHelixPropertyStore();
      propertyStore.get("/", null, 0);
    } finally {
      controller.disconnect();
      zkServer.shutdown();
      System.clearProperty("zk.connection.timeout");
      System.clearProperty("zk.connectionReEstablishment.timeout");
    }
  }

  public static final class SimpleStateModel extends StateModel {

    private final CountDownLatch latch;

    public SimpleStateModel(CountDownLatch latch) {
      this.latch = latch;
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      // LOG.info(HelixUtils.toString(message));
      LOG.info("message: " + message);
      latch.countDown();
    }
  }
}
