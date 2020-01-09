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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.zookeeper.IZkStateListener;
import org.apache.helix.manager.zk.zookeeper.ZkConnection;
import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.ZkClientMonitor;
import org.apache.helix.monitoring.mbeans.ZkClientPathMonitor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRawZkClient extends ZkUnitTestBase {
  private final String TEST_TAG = "test_monitor";
  private final String TEST_ROOT = "/my_cluster/IDEALSTATES";

  private ZkClient _zkClient;

  @BeforeClass
  public void beforeClass() {
    _zkClient = new ZkClient(ZK_ADDR);
  }

  @AfterClass
  public void afterClass() {
    _zkClient.delete(TEST_ROOT);
    _zkClient.deleteRecursively("/tmp");
    _zkClient.deleteRecursively("/my_cluster");
    _zkClient.close();
  }

  @Test()
  void testGetStat() {
    String path = "/tmp/getStatTest";
    _zkClient.deleteRecursively(path);

    Stat stat, newStat;
    stat = _zkClient.getStat(path);
    AssertJUnit.assertNull(stat);
    _zkClient.createPersistent(path, true);

    stat = _zkClient.getStat(path);
    AssertJUnit.assertNotNull(stat);

    newStat = _zkClient.getStat(path);
    AssertJUnit.assertEquals(stat, newStat);

    _zkClient.writeData(path, "Test");
    newStat = _zkClient.getStat(path);
    AssertJUnit.assertNotSame(stat, newStat);
  }

  /*
   * Tests subscribing state changes for helix's IZkStateListener.
   */
  @Test
  public void testSubscribeStateChanges() {
    int numListeners = _zkClient.numberOfListeners();
    List<IZkStateListener> listeners = new ArrayList<>();

    // Subscribe multiple listeners to test that listener's hashcode works as expected.
    // Each listener is subscribed and unsubscribed successfully.
    for (int i = 0; i < 3; i++) {
      IZkStateListener listener = new IZkStateListener() {
        @Override
        public void handleStateChanged(KeeperState state) {
          System.out.println("Handle new state: " + state);
        }

        @Override
        public void handleNewSession(final String sessionId) {
          System.out.println("Handle new session: " + sessionId);
        }

        @Override
        public void handleSessionEstablishmentError(Throwable error) {
          System.out.println("Handle session establishment error: " + error);
        }
      };

      _zkClient.subscribeStateChanges(listener);
      Assert.assertEquals(_zkClient.numberOfListeners(), ++numListeners);
      listeners.add(listener);
    }

    for (IZkStateListener listener : listeners) {
      _zkClient.unsubscribeStateChanges(listener);
      Assert.assertEquals(_zkClient.numberOfListeners(), --numListeners);
    }
  }

  /*
   * Tests session expiry for the helix's IZkStateListener.
   */
  @Test
  void testSessionExpiry() throws Exception {
    long lastSessionId = _zkClient.getSessionId();

    // Test multiple times to make sure each time the new session id is increasing.
    for (int i = 0; i < 3; i++) {
      ZkTestHelper.expireSession(_zkClient);
      long newSessionId = _zkClient.getSessionId();
      Assert.assertTrue(newSessionId != lastSessionId,
          "New session id should not equal to expired session id.");
      lastSessionId = newSessionId;
    }
  }

  /*
   * Tests state changes subscription for I0Itec's IZkStateListener.
   * This is a test for backward compatibility.
   *
   * TODO: remove this test when getting rid of I0Itec.
   */
  @Test
  public void testSubscribeStateChangesForI0ItecIZkStateListener() {
    int numListeners = _zkClient.numberOfListeners();
    List<org.I0Itec.zkclient.IZkStateListener> listeners = new ArrayList<>();

    // Subscribe multiple listeners to test that listener's hashcode works as expected.
    // Each listener is subscribed and unsubscribed successfully.
    for (int i = 0; i < 3; i++) {
      org.I0Itec.zkclient.IZkStateListener listener = new org.I0Itec.zkclient.IZkStateListener() {
        @Override
        public void handleStateChanged(KeeperState state) {
          System.out.println("Handle new state: " + state);
        }

        @Override
        public void handleNewSession() {
          System.out.println("Handle new session: ");
        }

        @Override
        public void handleSessionEstablishmentError(Throwable error) {
          System.out.println("Handle session establishment error: " + error);
        }
      };

      _zkClient.subscribeStateChanges(listener);
      Assert.assertEquals(_zkClient.numberOfListeners(), ++numListeners);

      // Try to subscribe the listener again but number of listeners should not change because the
      // listener already exists.
      _zkClient.subscribeStateChanges(listener);
      Assert.assertEquals(_zkClient.numberOfListeners(), numListeners);

      listeners.add(listener);
    }

    for (org.I0Itec.zkclient.IZkStateListener listener : listeners) {
      _zkClient.unsubscribeStateChanges(listener);
      Assert.assertEquals(_zkClient.numberOfListeners(), --numListeners);
    }
  }

  /*
   * Tests session expiry for I0Itec's IZkStateListener.
   * This is a test for backward compatibility.
   *
   * TODO: remove this test when getting rid of I0Itec.
   */
  @Test
  public void testSessionExpiryForI0IItecZkStateListener() throws Exception {
    org.I0Itec.zkclient.IZkStateListener listener =
        new org.I0Itec.zkclient.IZkStateListener() {

          @Override
          public void handleStateChanged(KeeperState state) {
            System.out.println("In Old connection New state " + state);
          }

          @Override
          public void handleNewSession() {
            System.out.println("In Old connection New session");
          }

          @Override
          public void handleSessionEstablishmentError(Throwable var1) {
          }
        };

    _zkClient.subscribeStateChanges(listener);
    ZkConnection connection = ((ZkConnection) _zkClient.getConnection());
    ZooKeeper zookeeper = connection.getZookeeper();
    long oldSessionId = zookeeper.getSessionId();
    System.out.println("old sessionId= " + oldSessionId);
    Watcher watcher = event -> System.out.println("In New connection In process event:" + event);
    ZooKeeper newZookeeper = new ZooKeeper(connection.getServers(), zookeeper.getSessionTimeout(),
        watcher, zookeeper.getSessionId(), zookeeper.getSessionPasswd());
    Thread.sleep(3000);
    System.out.println("New sessionId= " + newZookeeper.getSessionId());
    Thread.sleep(3000);
    newZookeeper.close();
    Thread.sleep(10000);
    connection = ((ZkConnection) _zkClient.getConnection());
    zookeeper = connection.getZookeeper();
    long newSessionId = zookeeper.getSessionId();
    System.out.println("After session expiry sessionId= " + newSessionId);
    _zkClient.unsubscribeStateChanges(listener);
  }

  @Test
  public void testZkClientMonitor() throws Exception {
    final String TEST_KEY = "testZkClientMonitor";
    ZkClient.Builder builder = new ZkClient.Builder();
    builder.setZkServer(ZK_ADDR).setMonitorKey(TEST_KEY).setMonitorType(TEST_TAG)
        .setMonitorRootPathOnly(false);
    ZkClient zkClient = builder.build();

    String TEST_DATA = "testData";
    String TEST_NODE = "/test_zkclient_monitor";
    String TEST_PATH = TEST_ROOT + TEST_NODE;
    final long TEST_DATA_SIZE = zkClient.serialize(TEST_DATA, TEST_PATH).length;

    if (_zkClient.exists(TEST_PATH)) {
      _zkClient.delete(TEST_PATH);
    }
    if (!_zkClient.exists(TEST_ROOT)) {
      _zkClient.createPersistent(TEST_ROOT, true);
    }

    MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

    ObjectName name = MBeanRegistrar.buildObjectName(MonitorDomainNames.HelixZkClient.name(),
        ZkClientMonitor.MONITOR_TYPE, TEST_TAG, ZkClientMonitor.MONITOR_KEY, TEST_KEY);
    ObjectName rootname = MBeanRegistrar.buildObjectName(MonitorDomainNames.HelixZkClient.name(),
        ZkClientMonitor.MONITOR_TYPE, TEST_TAG, ZkClientMonitor.MONITOR_KEY, TEST_KEY,
        ZkClientPathMonitor.MONITOR_PATH, "Root");
    ObjectName idealStatename = MBeanRegistrar.buildObjectName(
        MonitorDomainNames.HelixZkClient.name(), ZkClientMonitor.MONITOR_TYPE, TEST_TAG,
        ZkClientMonitor.MONITOR_KEY, TEST_KEY, ZkClientPathMonitor.MONITOR_PATH, "IdealStates");
    Assert.assertTrue(beanServer.isRegistered(rootname));
    Assert.assertTrue(beanServer.isRegistered(idealStatename));

    Assert.assertEquals((long) beanServer.getAttribute(name, "DataChangeEventCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(name, "StateChangeEventCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(name, "OutstandingRequestGauge"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(name, "TotalCallbackCounter"), 0);

    // Test exists
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadTotalLatencyCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadLatencyGauge.Max"), 0);
    zkClient.exists(TEST_ROOT);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 1);
    Assert.assertTrue((long) beanServer.getAttribute(rootname, "ReadTotalLatencyCounter") >= 0);
    Assert.assertTrue((long) beanServer.getAttribute(rootname, "ReadLatencyGauge.Max") >= 0);

    // Test create
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteBytesCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteBytesCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteTotalLatencyCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteLatencyGauge.Max"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteTotalLatencyCounter"),
        0);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteLatencyGauge.Max"), 0);
    zkClient.create(TEST_PATH, TEST_DATA, CreateMode.PERSISTENT);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteCounter"), 1);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteBytesCounter"),
        TEST_DATA_SIZE);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteCounter"), 1);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteBytesCounter"),
        TEST_DATA_SIZE);
    long origWriteTotalLatencyCounter =
        (long) beanServer.getAttribute(rootname, "WriteTotalLatencyCounter");
    Assert.assertTrue(origWriteTotalLatencyCounter >= 0);
    Assert.assertTrue((long) beanServer.getAttribute(rootname, "WriteLatencyGauge.Max") >= 0);
    long origIdealStatesWriteTotalLatencyCounter =
        (long) beanServer.getAttribute(idealStatename, "WriteTotalLatencyCounter");
    Assert.assertTrue(origIdealStatesWriteTotalLatencyCounter >= 0);
    Assert.assertTrue((long) beanServer.getAttribute(idealStatename, "WriteLatencyGauge.Max") >= 0);

    // Test read
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 1);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadBytesCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadBytesCounter"), 0);
    long origReadTotalLatencyCounter =
        (long) beanServer.getAttribute(rootname, "ReadTotalLatencyCounter");
    long origIdealStatesReadTotalLatencyCounter =
        (long) beanServer.getAttribute(idealStatename, "ReadTotalLatencyCounter");
    Assert.assertEquals(origIdealStatesReadTotalLatencyCounter, 0);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadLatencyGauge.Max"), 0);
    zkClient.readData(TEST_PATH, new Stat());
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 2);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadBytesCounter"),
        TEST_DATA_SIZE);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadCounter"), 1);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadBytesCounter"),
        TEST_DATA_SIZE);
    Assert.assertTrue((long) beanServer.getAttribute(rootname,
        "ReadTotalLatencyCounter") >= origReadTotalLatencyCounter);
    Assert.assertTrue((long) beanServer.getAttribute(idealStatename,
        "ReadTotalLatencyCounter") >= origIdealStatesReadTotalLatencyCounter);
    Assert.assertTrue((long) beanServer.getAttribute(idealStatename, "ReadLatencyGauge.Max") >= 0);
    zkClient.getChildren(TEST_PATH);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 3);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadBytesCounter"),
        TEST_DATA_SIZE);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadCounter"), 2);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadBytesCounter"),
        TEST_DATA_SIZE);
    zkClient.getStat(TEST_PATH);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 4);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadBytesCounter"),
        TEST_DATA_SIZE);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadCounter"), 3);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadBytesCounter"),
        TEST_DATA_SIZE);
    zkClient.readDataAndStat(TEST_PATH, new Stat(), true);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 5);

    ZkAsyncCallbacks.ExistsCallbackHandler callbackHandler =
        new ZkAsyncCallbacks.ExistsCallbackHandler();
    zkClient.asyncExists(TEST_PATH, callbackHandler);
    callbackHandler.waitForSuccess();
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 6);

    // Test write
    zkClient.writeData(TEST_PATH, TEST_DATA);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteCounter"), 2);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteBytesCounter"),
        TEST_DATA_SIZE * 2);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteCounter"), 2);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteBytesCounter"),
        TEST_DATA_SIZE * 2);
    Assert.assertTrue((long) beanServer.getAttribute(rootname,
        "WriteTotalLatencyCounter") >= origWriteTotalLatencyCounter);
    Assert.assertTrue((long) beanServer.getAttribute(idealStatename,
        "WriteTotalLatencyCounter") >= origIdealStatesWriteTotalLatencyCounter);

    // Test data change count
    final Lock lock = new ReentrantLock();
    final Condition callbackFinish = lock.newCondition();
    zkClient.subscribeDataChanges(TEST_PATH, new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data) {
        callbackLock();
      }

      @Override
      public void handleDataDeleted(String dataPath) {
        callbackLock();
      }

      private void callbackLock() {
        lock.lock();
        try {
          callbackFinish.signal();
        } finally {
          lock.unlock();
        }
      }
    });
    lock.lock();
    _zkClient.writeData(TEST_PATH, "Test");
    Assert.assertTrue(callbackFinish.await(10, TimeUnit.SECONDS));
    Assert.assertEquals((long) beanServer.getAttribute(name, "DataChangeEventCounter"), 1);
    Assert.assertEquals((long) beanServer.getAttribute(name, "OutstandingRequestGauge"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(name, "TotalCallbackCounter"), 1);
    Assert.assertEquals((long) beanServer.getAttribute(name, "TotalCallbackHandledCounter"), 1);
    Assert.assertEquals((long) beanServer.getAttribute(name, "PendingCallbackGauge"), 0);

    // Simulate a delayed callback
    int waitTime = 10;
    Thread.sleep(waitTime);
    lock.lock();
    zkClient.process(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, null, TEST_PATH));
    Assert.assertTrue(callbackFinish.await(10, TimeUnit.SECONDS));
    Assert.assertTrue(
        (long) beanServer.getAttribute(rootname, "DataPropagationLatencyGauge.Max") >= waitTime);

    _zkClient.delete(TEST_PATH);
  }

  @Test(dependsOnMethods = "testZkClientMonitor")
  void testPendingRequestGauge() throws Exception {
    final String TEST_KEY = "testPendingRequestGauge";

    final MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName name = MBeanRegistrar.buildObjectName(MonitorDomainNames.HelixZkClient.name(),
        ZkClientMonitor.MONITOR_TYPE, TEST_TAG, ZkClientMonitor.MONITOR_KEY, TEST_KEY);

    final int zkPort = TestHelper.getRandomPort();
    final String zkAddr = String.format("localhost:%d", zkPort);
    final ZkServer zkServer = TestHelper.startZkServer(zkAddr);

    try {
      ZkClient.Builder builder = new ZkClient.Builder();
      builder.setZkServer(zkAddr).setMonitorKey(TEST_KEY).setMonitorType(TEST_TAG)
          .setMonitorRootPathOnly(true);
      final ZkClient zkClient = builder.build();

      zkServer.shutdown();
      zkClient.waitForKeeperState(KeeperState.Disconnected, 5000, TimeUnit.MILLISECONDS);
      Assert.assertFalse(zkClient.waitUntilConnected(0, TimeUnit.MILLISECONDS));

      Assert.assertEquals((long) beanServer.getAttribute(name, "OutstandingRequestGauge"), 0);

      // Request a read in a separate thread. This will be a pending request
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      executorService.submit(() -> {
        zkClient.exists(TEST_ROOT);
      });
      Assert.assertTrue(TestHelper.verify(
          () -> (long) beanServer.getAttribute(name, "OutstandingRequestGauge") == 1, 1000));

      zkServer.start();
      Assert.assertTrue(zkClient.waitUntilConnected(5000, TimeUnit.MILLISECONDS));
      Assert.assertTrue(TestHelper.verify(
          () -> (long) beanServer.getAttribute(name, "OutstandingRequestGauge") == 0, 2000));
      zkClient.close();
    } finally {
      zkServer.shutdown();
    }
  }

  /*
   * This test checks that a valid session can successfully create an ephemeral node.
   */
  @Test
  public void testCreateEphemeralWithValidSession() throws Exception {
    final String className = TestHelper.getTestClassName();
    final String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    TestHelper.setupCluster(clusterName, ZK_ADDR,
        12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave",
        true); // do rebalance

    final String originalSessionId = ZKUtil.toHexSessionId(_zkClient.getSessionId());
    final String path = "/" + methodName;
    final String data = "Hello Helix";

    // Verify the node is not existed yet.
    Assert.assertFalse(_zkClient.exists(path));

    // Wait until the ZkClient has got a new session.
    Assert.assertTrue(TestHelper
        .verify(() -> _zkClient.getConnection().getZookeeperState().isConnected(), 1000L));

    try {
      // Create ephemeral node.
      _zkClient.createEphemeral(path, data, originalSessionId);
    } catch (Exception ex) {
      Assert.fail("Failed to create ephemeral node.", ex);
    }

    // Verify the node is created and its data is correct.
    Stat stat = new Stat();
    String nodeData = _zkClient.readData(path, stat, true);

    Assert.assertNotNull(nodeData, "Failed to create ephemeral node: " + path);
    Assert.assertEquals(nodeData, data, "Data is not correct.");
    Assert.assertTrue(stat.getEphemeralOwner() != 0L,
        "Ephemeral owner should NOT be zero because the node is an ephemeral node.");
    Assert.assertEquals(ZKUtil.toHexSessionId(stat.getEphemeralOwner()), originalSessionId,
        "Ephemeral node is created by an unexpected session");

    // Delete the node to clean up, otherwise, the ephemeral node would be existed
    // until the session of its owner expires.
    _zkClient.delete(path);
  }

  /*
   * This test checks that ephemeral creation fails because the expected zk session does not match
   * the actual zk session.
   * How this test does is:
   * 1. Creates a zk client and gets its original session id
   * 2. Expires the original session, and a new session will be created
   * 3. Tries to create ephemeral node with the original session
   * 4. ZkSessionMismatchedException is expected for the creation and ephemeral node is not created
   */
  @Test
  public void testCreateEphemeralWithMismatchedSession() throws Exception {
    final String className = TestHelper.getTestClassName();
    final String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    TestHelper.setupCluster(clusterName, ZK_ADDR,
        12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave",
        true); // do rebalance

    final long originalSessionId = _zkClient.getSessionId();
    final String originalHexSessionId = ZKUtil.toHexSessionId(originalSessionId);
    final String path = "/" + methodName;

    // Verify the node is not existed.
    Assert.assertFalse(_zkClient.exists(path));

    // Expire the original session.
    ZkTestHelper.expireSession(_zkClient);

    // Wait until the ZkClient has got a new session.
    Assert.assertTrue(TestHelper.verify(() -> {
      try {
        // New session id should not equal to expired session id.
        return _zkClient.getSessionId() != originalSessionId;
      } catch (HelixException ex) {
        return false;
      }
    }, 1000L));

    try {
      // Try to create ephemeral node with the original session.
      // This creation should NOT be successful because the original session is already expired.
      _zkClient.createEphemeral(path, "Hello Helix", originalHexSessionId);
      Assert.fail("Ephemeral node should not be created by the expired session.");
    } catch (ZkSessionMismatchedException expected) {
      // Expected because there is a session mismatch.
    } catch (Exception unexpected) {
      Assert.fail("Should not have thrown exception: " + unexpected);
    }

    // Verify the node is not created.
    Assert.assertFalse(_zkClient.exists(path));
  }

  /*
   * Tests that when trying to create an ephemeral node, if connection to zk service is lost,
   * ConnectionLossException will be thrown in retryUntilConnected and ephemeral creation will fail.
   * Because ConnectionLossException is not thrown in createEphemeral(), operation retry timeout is
   * set to 3 seconds and then ZkTimeoutException is thrown. And retry cause message is checked to
   * see if ConnectionLossException was thrown before retry.
   */
  @Test(timeOut = 5 * 60 * 1000L)
  public void testConnectionLossWhileCreateEphemeral() throws Exception {
    final String methodName = TestHelper.getTestMethodName();

    final ZkClient zkClient = new ZkClient.Builder()
        .setZkServer(ZK_ADDR)
        .setOperationRetryTimeout(3000L) // 3 seconds
        .build();

    final String expectedSessionId = ZKUtil.toHexSessionId(zkClient.getSessionId());
    final String path = "/" + methodName;
    final String data = "data";

    Assert.assertFalse(zkClient.exists(path));

    // Shutdown zk server so zk operations will fail due to disconnection.
    TestHelper.stopZkServer(_zkServer);

    try {
      final CountDownLatch countDownLatch = new CountDownLatch(1);
      final AtomicBoolean running = new AtomicBoolean(true);

      final Thread creationThread = new Thread(() -> {
        while (running.get()) {
          try {
            // Create ephemeral node in the expected session id.
            zkClient.createEphemeral(path, data, expectedSessionId);
          } catch (ZkTimeoutException e) {
            // Verify ConnectionLossException was thrown before retry and timeout.
            if (e.getMessage().endsWith("Retry was caused by "
                + KeeperException.ConnectionLossException.Code.CONNECTIONLOSS)) {
              running.set(false);
            }
          }
        }
        countDownLatch.countDown();
      });

      creationThread.start();

      final boolean creationThreadTerminated = countDownLatch.await(10L, TimeUnit.SECONDS);
      if (!creationThreadTerminated) {
        running.set(false);
        creationThread.join(5000L);
        Assert.fail("Failed to receive a ConnectionLossException after zookeeper has shutdown.");
      }
    } finally {
      zkClient.close();
      // Recover zk server.
      _zkServer.start();
    }
  }

  /*
   * Tests that when trying to create an ephemeral node, if connection to zk service is lost,
   * the creation operation will keep retrying until connected and finally successfully create the
   * node.
   * How this test simulates the steps is:
   * 1. Shuts down zk server
   * 2. Starts a creation thread to create an ephemeral node in the original zk session
   * 3. Creation operation loses connection and keeps retrying
   * 4. Restarts zk server and Zk session is recovered
   * 5. zk client reconnects successfully and creates an ephemeral node
   */
  @Test(timeOut = 5 * 60 * 1000L)
  public void testRetryUntilConnectedAfterConnectionLoss() throws Exception {
    final String methodName = TestHelper.getTestMethodName();

    final String expectedSessionId = ZKUtil.toHexSessionId(_zkClient.getSessionId());
    final String path = "/" + methodName;
    final String data = "data";

    Assert.assertFalse(_zkClient.exists(path));

    // Shutdown zk server so zk operations will fail due to disconnection.
    TestHelper.stopZkServer(_zkServer);

    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final AtomicBoolean running = new AtomicBoolean(true);

    final Thread creationThread = new Thread(() -> {
      while (running.get()) {
        // Create ephemeral node in the expected session id.
        System.out.println("Trying to create ephemeral node...");
        _zkClient.createEphemeral(path, data, expectedSessionId);
        System.out.println("Ephemeral node created.");
        running.set(false);
      }
      countDownLatch.countDown();
    });

    creationThread.start();
    // Keep creation thread retrying to connect for 10 seconds.
    System.out.println("Keep creation thread retrying to connect for 10 seconds...");
    TimeUnit.SECONDS.sleep(10);

    System.out.println("Restarting zk server...");
    _zkServer.start();

    // Wait for creating ephemeral node successfully.
    final boolean creationThreadTerminated = countDownLatch.await(10, TimeUnit.SECONDS);
    if (!creationThreadTerminated) {
      running.set(false);
      creationThread.join(5000L);
      Assert.fail("Failed to reconnect to zk server and create ephemeral node"
          + " after zk server is recovered.");
    }

    Stat stat = new Stat();
    String nodeData = _zkClient.readData(path, stat, true);

    Assert.assertNotNull(nodeData, "Failed to create ephemeral node: " + path);
    Assert.assertEquals(nodeData, data, "Data is not correct.");
    Assert.assertTrue(stat.getEphemeralOwner() != 0L,
        "Ephemeral owner should NOT be zero because the node is an ephemeral node.");
    Assert.assertEquals(ZKUtil.toHexSessionId(stat.getEphemeralOwner()), expectedSessionId,
        "Ephemeral node is created by an unexpected session");

    // Delete the node to clean up, otherwise, the ephemeral node would be existed until the session
    // of its owner expires.
    _zkClient.delete(path);
  }

  @Test
  public void testWaitForEstablishedSession() {
    ZkClient zkClient = new ZkClient(ZK_ADDR);
    Assert.assertTrue(zkClient.waitForEstablishedSession(1, TimeUnit.SECONDS) != 0L);
    TestHelper.stopZkServer(_zkServer);
    Assert.assertTrue(zkClient.waitForKeeperState(KeeperState.Disconnected, 1, TimeUnit.SECONDS));

    try {
      zkClient.waitForEstablishedSession(3, TimeUnit.SECONDS);
      Assert.fail("Connecting to zk server should time out and ZkTimeoutException is expected.");
    } catch (ZkTimeoutException expected) {
      // Because zk server is shutdown, zkClient should not connect to zk server and a
      // ZkTimeoutException should be thrown.
    }

    zkClient.close();
    // Recover zk server for later tests.
    _zkServer.start();
  }
}
