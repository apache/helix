package org.apache.helix.manager.zk;

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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.tools.ClusterSetup;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkReconnect {
  private static final Logger LOG = LoggerFactory.getLogger(TestZkReconnect.class);
  ExecutorService _executor = Executors.newSingleThreadExecutor();

  @Test
  public void testHelixManagerStateListenerCallback() throws Exception {
    final int zkPort = TestHelper.getRandomPort();
    final String zkAddr = String.format("localhost:%d", zkPort);
    final ZkServer zkServer = TestHelper.startZkServer(zkAddr);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    // Init onDisconnectedFlag to check if callback is triggered
    final AtomicReference<Boolean> onDisconnectedFlag = new AtomicReference<>(false);
    final AtomicReference<Boolean> onConnectedFlag = new AtomicReference<>(false);

    // Setup cluster
    LOG.info("Setup clusters");
    ClusterSetup clusterSetup = new ClusterSetup(zkAddr);
    clusterSetup.addCluster(clusterName, true);
    // For fast test, set short timeout
    System.setProperty(SystemPropertyKeys.ZK_CONNECTION_TIMEOUT, "2000");

    // Registers and starts controller, register listener for disconnect handling
    LOG.info("Starts controller");
    final ZKHelixManager controller =
        (ZKHelixManager) HelixManagerFactory.getZKHelixManager(clusterName, null, InstanceType.CONTROLLER, zkAddr,
            new HelixManagerStateListener() {
              @Override
              public void onConnected(HelixManager helixManager) throws Exception {
                Assert.assertEquals(helixManager.getClusterName(), clusterName);
                onConnectedFlag.getAndSet(true);
              }

              @Override
              public void onDisconnected(HelixManager helixManager, Throwable error) throws Exception {
                Assert.assertEquals(helixManager.getClusterName(), clusterName);
                onDisconnectedFlag.getAndSet(true);
              }
            });

    try {
      controller.connect();
      // check onConnected() is triggered
      Assert.assertTrue(onConnectedFlag.getAndSet(false));

      // 1. shutdown zkServer and check if handler trigger callback
      zkServer.shutdown();
      // Simulate a retry in ZkClient that will not succeed
      injectExpire(controller._zkclient);
      Assert.assertFalse(controller._zkclient.waitUntilConnected(5000, TimeUnit.MILLISECONDS));
      // While retrying, onDisconnectedFlag = false
      Assert.assertFalse(onDisconnectedFlag.get());

      // 2. restart zkServer and check if handler will recover connection
      zkServer.start();
      Assert.assertTrue(controller._zkclient
          .waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));
      Assert.assertTrue(controller.isConnected());

      // New propertyStore should be in good state
      ZkHelixPropertyStore propertyStore = controller.getHelixPropertyStore();
      propertyStore.get("/", null, 0);

      onConnectedFlag.set(false);

      // Inject expire to test handler
      // onDisconnectedFlag should be set within onDisconnected handler
      controller.handleSessionEstablishmentError(new Exception("For testing"));
      Assert.assertTrue(onDisconnectedFlag.get());
      Assert.assertFalse(onConnectedFlag.get());
      Assert.assertFalse(controller.isConnected());

      // Verify ZK is down
      try {
        controller.getHelixPropertyStore();
      } catch (HelixException e) {
        // Expected exception
        System.out.println(e.getMessage());
      }
    } finally {
      controller.disconnect();
      zkServer.shutdown();
      System.clearProperty(SystemPropertyKeys.ZK_CONNECTION_TIMEOUT);
    }
  }

  private void injectExpire(final ZkClient zkClient)
      throws ExecutionException, InterruptedException {
    Future future = _executor.submit(new Runnable() {
      @Override
      public void run() {
        WatchedEvent event =
            new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null);
        zkClient.process(event);
      }
    });
    future.get();
  }
}
