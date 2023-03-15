package org.apache.helix.metaclient.impl.zk;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.helix.metaclient.api.ConnectStateChangeListener;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.policy.ExponentialBackoffReconnectPolicy;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.apache.helix.metaclient.constants.MetaClientConstants.DEFAULT_INIT_EXP_BACKOFF_RETRY_INTERVAL_MS;
import static org.apache.helix.metaclient.constants.MetaClientConstants.DEFAULT_MAX_EXP_BACKOFF_RETRY_INTERVAL_MS;


public class TestConnectStateChangeListenerAndRetry  {
  protected static final String ZK_ADDR = "localhost:2181";
  protected static ZkServer _zkServer;

  private static final long AUTO_RECONNECT_TIMEOUT_MS_FOR_TEST = 3 * 1000;
  private static final long AUTO_RECONNECT_WAIT_TIME_WITHIN = 1 * 1000;
  private static final long AUTO_RECONNECT_WAIT_TIME_EXD = 5 * 1000;

  /**
   * Simulate a zk state change by calling {@link ZkClient#process(WatchedEvent)} directly
   */
  private static void simulateZkStateReconnected(ZkClient zkClient) throws InterruptedException {
      WatchedEvent event =
          new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected,
              null);
      zkClient.process(event);

      Thread.sleep(AUTO_RECONNECT_WAIT_TIME_WITHIN);

      event = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected,
          null);
      zkClient.process(event);
  }

  @BeforeSuite
  public void prepare() {
    // start local zookeeper server
    _zkServer = ZkMetaClientTestBase.startZkServer(ZK_ADDR);
  }

  @AfterSuite
  public void cleanUp() {

  }

  @Test
  public void testConnectState() {
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClientReconnectTest()) {
      zkMetaClient.connect();
      zkMetaClient.connect();
      Assert.fail("The second connect should throw IllegalStateException");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IllegalStateException);
    }
  }

  // test mock zkclient event
  @Test(dependsOnMethods = "testConnectState")
  public void testReConnectSucceed() throws InterruptedException {
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClientReconnectTest()) {
      zkMetaClient.connect();
      simulateZkStateReconnected(zkMetaClient.getZkClient());
      Thread.sleep(AUTO_RECONNECT_WAIT_TIME_EXD);
      // When ZK reconnect happens within timeout window, zkMetaClient should ba able to perform CRUD.
      Assert.assertTrue(zkMetaClient.getZkClient().getConnection().getZookeeperState().isConnected());
    }
  }

  @Test(dependsOnMethods = "testReConnectSucceed")
  public void testConnectStateChangeListener() throws Exception {
    final String basePath = "/TestConnectStateChangeListenerAndRetry_testListener";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClientReconnectTest()) {
      CountDownLatch countDownLatch = new CountDownLatch(1);
      final MetaClientInterface.ConnectState[] connectState =
          new MetaClientInterface.ConnectState[2];
      ConnectStateChangeListener listener = new ConnectStateChangeListener() {
        @Override
        public void handleConnectStateChanged(MetaClientInterface.ConnectState prevState,
            MetaClientInterface.ConnectState currentState) throws Exception {
          connectState[0] = prevState;
          connectState[1] = currentState;
          countDownLatch.countDown();
        }

        @Override
        public void handleConnectionEstablishmentError(Throwable error) throws Exception {

        }
      };
      Assert.assertTrue(zkMetaClient.subscribeStateChanges(listener));
      zkMetaClient.connect();
      countDownLatch.await(5000, TimeUnit.SECONDS);
      Assert.assertEquals(connectState[0], MetaClientInterface.ConnectState.NOT_CONNECTED);
      Assert.assertEquals(connectState[1], MetaClientInterface.ConnectState.CONNECTED);

      _zkServer.shutdown();
      Thread.sleep(AUTO_RECONNECT_WAIT_TIME_EXD);
      Assert.assertEquals(connectState[0], MetaClientInterface.ConnectState.CONNECTED);
      Assert.assertEquals(connectState[1], MetaClientInterface.ConnectState.DISCONNECTED);

      try {
        zkMetaClient.create("/key", "value");
        Assert.fail("Create call after close should throw IllegalStateException");
      } catch (Exception ex) {
        Assert.assertTrue(ex.getCause() instanceof IllegalStateException);
      }
    }
  }

  static ZkMetaClient<String> createZkMetaClientReconnectTest() {
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR)
            .setMetaClientReconnectPolicy(
                new ExponentialBackoffReconnectPolicy(
                     AUTO_RECONNECT_TIMEOUT_MS_FOR_TEST))
            .build();
    return new ZkMetaClient<>(config);
  }
}
