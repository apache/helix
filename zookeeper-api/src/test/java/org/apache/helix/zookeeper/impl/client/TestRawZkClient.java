package org.apache.helix.zookeeper.impl.client;

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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
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

import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.datamodel.SessionAwareZNRecord;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.impl.TestHelper;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.impl.ZkTestHelper;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.exception.ZkSessionMismatchedException;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.helix.zookeeper.zkclient.metric.ZkClientMonitor;
import org.apache.helix.zookeeper.zkclient.metric.ZkClientPathMonitor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ContainerManager;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRawZkClient extends ZkTestBase {
  private final String TEST_TAG = "test_monitor";
  private final String TEST_ROOT = "/my_cluster/IDEALSTATES";

  private ZkClient _zkClient;

  @BeforeClass
  public void beforeClass() {
    _zkClient = new ZkClient(ZkTestBase.ZK_ADDR);
  }

  @AfterClass
  public void afterClass() {
    _zkClient.deleteRecursively(TEST_ROOT);
    _zkClient.deleteRecursively("/tmp");
    _zkClient.deleteRecursively("/my_cluster");
    _zkClient.close();
  }

  @Test
  void testUnimplementedTypes() {
    // Make sure extended types are disabled
    System.clearProperty("zookeeper.extendedTypesEnabled");

    // Make sure the test path is clear
    String parentPath = "/tmp";
    String path = "/tmp/unimplemented";
    _zkClient.deleteRecursively(parentPath);

    try {
      long ttl = 1L;
      _zkClient.createPersistentWithTTL(path, true, ttl);
    } catch (ZkException e) {
      AssertJUnit.assertTrue(e.getCause() instanceof KeeperException.UnimplementedException);
      return;
    }

    // Clean up
    _zkClient.deleteRecursively(parentPath);
    AssertJUnit.fail();
  }

  @Test
  void testCreatePersistentWithTTL() throws InterruptedException {
    // Enable extended types and create a ZkClient
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
    ZkClient zkClient = new ZkClient(ZkTestBase.ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    // Make sure the test path is clear
    String parentPath = "/tmp";
    String path = "/tmp/createTTL";
    zkClient.deleteRecursively(parentPath);
    AssertJUnit.assertFalse(zkClient.exists(parentPath));
    AssertJUnit.assertFalse(zkClient.exists(path));

    long ttl = 1L;
    ZNRecord record = new ZNRecord("record");
    String key = "key";
    String value = "value";
    record.setSimpleField(key, value);

    // Create a ZNode with the above ZNRecord and read back its data
    zkClient.createPersistentWithTTL(parentPath, record, ttl);
    AssertJUnit.assertTrue(zkClient.exists(parentPath));
    ZNRecord retrievedRecord = zkClient.readData(parentPath);
    AssertJUnit.assertEquals(value, retrievedRecord.getSimpleField(key));

    // Clear the path and test with createParents = true
    AssertJUnit.assertTrue(zkClient.delete(parentPath));
    zkClient.createPersistentWithTTL(path, true, ttl);
    AssertJUnit.assertTrue(zkClient.exists(path));

    // Check if the TTL znode expires or not.
    advanceFakeElapsedTime(2000);
    ContainerManager containerManager = _zkServerContainerManagerMap.get(_zkServerMap.get(ZkTestBase.ZK_ADDR));
    containerManager.checkContainers();

    // Clean up
    zkClient.deleteRecursively(parentPath);
    zkClient.close();
    System.clearProperty("zookeeper.extendedTypesEnabled");
  }

  @Test
  void testCreatePersistentSequentialWithTTL() throws InterruptedException {
    // Enable extended types and create a ZkClient
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
    ZkClient zkClient = new ZkClient(ZkTestBase.ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    // Make sure the test path is clear
    String parentPath = "/tmp";
    String path = "/tmp/createSequentialTTL";
    zkClient.deleteRecursively(parentPath);
    AssertJUnit.assertFalse(zkClient.exists(parentPath));
    AssertJUnit.assertFalse(zkClient.exists(path + "0000000000"));

    long ttl = 1L;
    ZNRecord record = new ZNRecord("record");
    String key = "key";
    String value = "value";
    record.setSimpleField(key, value);

    // Create a ZNode with the above ZNRecord and read back its data
    zkClient.createPersistent(parentPath);
    zkClient.createPersistentSequentialWithTTL(path, record, ttl);
    AssertJUnit.assertTrue(zkClient.exists(path + "0000000000"));
    ZNRecord retrievedRecord = zkClient.readData(path + "0000000000");
    AssertJUnit.assertEquals(value, retrievedRecord.getSimpleField(key));

    // Check if the TTL znode expires or not.
    advanceFakeElapsedTime(2000);
    ContainerManager containerManager = _zkServerContainerManagerMap.get(_zkServerMap.get(ZkTestBase.ZK_ADDR));
    containerManager.checkContainers();

    // Clean up
    zkClient.deleteRecursively(parentPath);
    zkClient.close();
    System.clearProperty("zookeeper.extendedTypesEnabled");
  }

  @Test
  void testCreateContainer() {
    // Enable extended types and create a ZkClient
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
    ZkClient zkClient = new ZkClient(ZkTestBase.ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    // Make sure the test path is clear
    String parentPath = "/tmp";
    String path = "/tmp/createContainer";
    zkClient.deleteRecursively(parentPath);
    AssertJUnit.assertFalse(zkClient.exists(parentPath));
    AssertJUnit.assertFalse(zkClient.exists(path));

    ZNRecord record = new ZNRecord("record");
    String key = "key";
    String value = "value";
    record.setSimpleField(key, value);

    // Create a ZNode with the above ZNRecord and read back its data
    zkClient.createContainer(parentPath, record);
    AssertJUnit.assertTrue(zkClient.exists(parentPath));
    ZNRecord retrievedRecord = zkClient.readData(parentPath);
    AssertJUnit.assertEquals(value, retrievedRecord.getSimpleField(key));

    // Clear the path and test with createParents = true
    AssertJUnit.assertTrue(zkClient.delete(parentPath));
    zkClient.createContainer(path, true);
    AssertJUnit.assertTrue(zkClient.exists(path));

    // Clean up
    zkClient.deleteRecursively(parentPath);
    zkClient.close();
    System.clearProperty("zookeeper.extendedTypesEnabled");
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
   * Tests state changes subscription for I0Itec's IZkStateListener.
   * This is a test for backward compatibility.
   *
   * TODO: remove this test when getting rid of I0Itec.
   */
  @Test
  public void testSubscribeStateChangesForI0ItecIZkStateListener() {
    int numListeners = _zkClient.numberOfListeners();
    List<org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener> listeners =
        new ArrayList<>();

    // Subscribe multiple listeners to test that listener's hashcode works as expected.
    // Each listener is subscribed and unsubscribed successfully.
    for (int i = 0; i < 3; i++) {
      org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener listener =
          new org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener() {
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

    for (org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener listener : listeners) {
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
  public void testSessionExpiryForI0IItecZkStateListener()
      throws Exception {
    org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener listener =
        new org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener() {

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
    ZooKeeper newZookeeper =
        new ZooKeeper(connection.getServers(), zookeeper.getSessionTimeout(), watcher,
            zookeeper.getSessionId(), zookeeper.getSessionPasswd());
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
  public void testZkClientMonitor()
      throws Exception {
    final String TEST_KEY = "testZkClientMonitor";
    ZkClient.Builder builder = new ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorKey(TEST_KEY).setMonitorType(TEST_TAG)
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

    ObjectName name = MBeanRegistrar
        .buildObjectName(MonitorDomainNames.HelixZkClient.name(), ZkClientMonitor.MONITOR_TYPE,
            TEST_TAG, ZkClientMonitor.MONITOR_KEY, TEST_KEY);
    ObjectName rootname = MBeanRegistrar
        .buildObjectName(MonitorDomainNames.HelixZkClient.name(), ZkClientMonitor.MONITOR_TYPE,
            TEST_TAG, ZkClientMonitor.MONITOR_KEY, TEST_KEY, ZkClientPathMonitor.MONITOR_PATH,
            "Root");
    ObjectName idealStatename = MBeanRegistrar
        .buildObjectName(MonitorDomainNames.HelixZkClient.name(), ZkClientMonitor.MONITOR_TYPE,
            TEST_TAG, ZkClientMonitor.MONITOR_KEY, TEST_KEY, ZkClientPathMonitor.MONITOR_PATH,
            "IdealStates");
    Assert.assertTrue(beanServer.isRegistered(rootname));
    Assert.assertTrue(beanServer.isRegistered(idealStatename));

    Assert.assertEquals((long) beanServer.getAttribute(name, "DataChangeEventCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(name, "ExpiredSessionCounter"), 0);
    Assert.assertEquals((long) beanServer.getAttribute(name, "OutstandingRequestGauge"), 0);

    boolean verifyResult = TestHelper.verify(()->{
      return (long) beanServer.getAttribute(rootname, "ReadAsyncCounter") == 1;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(verifyResult, " did not see first sync() read");

    Assert.assertEquals((long) beanServer.getAttribute(name, "StateChangeEventCounter"), 1);

    long firstLatencyCounter =  (long) beanServer.getAttribute(rootname, "ReadTotalLatencyCounter");
    long firstReadLatencyGauge = (long) beanServer.getAttribute(rootname, "ReadLatencyGauge.Max");
    Assert.assertTrue(firstLatencyCounter >= 0);
    Assert.assertTrue(firstReadLatencyGauge >= 0);
    zkClient.exists(TEST_ROOT);

    Assert.assertTrue((long) beanServer.getAttribute(rootname, "ReadCounter") == 1);

    Assert.assertTrue((long) beanServer.getAttribute(rootname, "ReadTotalLatencyCounter") >= firstLatencyCounter);
    Assert.assertTrue((long) beanServer.getAttribute(rootname, "ReadLatencyGauge.Max") >= firstReadLatencyGauge);

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
    Assert
        .assertEquals((long) beanServer.getAttribute(rootname, "ReadBytesCounter"), TEST_DATA_SIZE);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadCounter"), 1);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadBytesCounter"),
        TEST_DATA_SIZE);
    Assert.assertTrue((long) beanServer.getAttribute(rootname, "ReadTotalLatencyCounter")
        >= origReadTotalLatencyCounter);
    Assert.assertTrue((long) beanServer.getAttribute(idealStatename, "ReadTotalLatencyCounter")
        >= origIdealStatesReadTotalLatencyCounter);
    Assert.assertTrue((long) beanServer.getAttribute(idealStatename, "ReadLatencyGauge.Max") >= 0);
    zkClient.getChildren(TEST_PATH);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 3);
    Assert
        .assertEquals((long) beanServer.getAttribute(rootname, "ReadBytesCounter"), TEST_DATA_SIZE);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadCounter"), 2);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadBytesCounter"),
        TEST_DATA_SIZE);
    zkClient.getStat(TEST_PATH);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 4);
    Assert
        .assertEquals((long) beanServer.getAttribute(rootname, "ReadBytesCounter"), TEST_DATA_SIZE);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadCounter"), 3);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "ReadBytesCounter"),
        TEST_DATA_SIZE);
    zkClient.readDataAndStat(TEST_PATH, new Stat(), true);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 5);

    ZkAsyncCallbacks.ExistsCallbackHandler callbackHandler =
        new ZkAsyncCallbacks.ExistsCallbackHandler();
    zkClient.asyncExists(TEST_PATH, callbackHandler);
    callbackHandler.waitForSuccess();
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadAsyncCounter"), 2);

    // Test write
    zkClient.writeData(TEST_PATH, TEST_DATA);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteCounter"), 2);
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "WriteBytesCounter"),
        TEST_DATA_SIZE * 2);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteCounter"), 2);
    Assert.assertEquals((long) beanServer.getAttribute(idealStatename, "WriteBytesCounter"),
        TEST_DATA_SIZE * 2);
    Assert.assertTrue((long) beanServer.getAttribute(rootname, "WriteTotalLatencyCounter")
        >= origWriteTotalLatencyCounter);
    Assert.assertTrue((long) beanServer.getAttribute(idealStatename, "WriteTotalLatencyCounter")
        >= origIdealStatesWriteTotalLatencyCounter);

    // Verify the callback baseline count because of the data sync call.
    Assert.assertEquals((long) beanServer.getAttribute(name, "TotalCallbackCounter"), 1);
    Assert.assertEquals((long) beanServer.getAttribute(name, "TotalCallbackHandledCounter"), 1);

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
    Assert.assertEquals((long) beanServer.getAttribute(name, "TotalCallbackCounter"), 2);
    // Processing of the event might be slightly delayed.
    Assert.assertTrue(TestHelper
        .verify(() -> (long) beanServer.getAttribute(name, "TotalCallbackHandledCounter") == 2,
            500));
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
  void testPendingRequestGauge()
      throws Exception {
    final String TEST_KEY = "testPendingRequestGauge";

    final MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName name = MBeanRegistrar
        .buildObjectName(MonitorDomainNames.HelixZkClient.name(), ZkClientMonitor.MONITOR_TYPE,
            TEST_TAG, ZkClientMonitor.MONITOR_KEY, TEST_KEY);

    final int zkPort = TestHelper.getRandomPort();
    final String zkAddr = String.format("localhost:%d", zkPort);
    final ZkServer zkServer = startZkServer(zkAddr);

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
      Assert.assertTrue(TestHelper
          .verify(() -> (long) beanServer.getAttribute(name, "OutstandingRequestGauge") == 1,
              1000));

      zkServer.start();
      Assert.assertTrue(zkClient.waitUntilConnected(5000, TimeUnit.MILLISECONDS));
      Assert.assertTrue(TestHelper
          .verify(() -> (long) beanServer.getAttribute(name, "OutstandingRequestGauge") == 0,
              2000));
      zkClient.close();
    } finally {
      zkServer.shutdown();
    }
  }

  /*
   * Tests session expiry and session expire counter for the helix's IZkStateListener.
   */
  @Test(dependsOnMethods = "testZkClientMonitor")
  void testSessionExpireCount() throws Exception {
    final String TEST_KEY = "testSessionExpireCount";

    final MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName name = MBeanRegistrar
        .buildObjectName(MonitorDomainNames.HelixZkClient.name(), ZkClientMonitor.MONITOR_TYPE,
            TEST_TAG, ZkClientMonitor.MONITOR_KEY, TEST_KEY);

    final int zkPort = TestHelper.getRandomPort();
    final String zkAddr = String.format("localhost:%d", zkPort);
    final ZkServer zkServer = startZkServer(zkAddr);

    try {
      ZkClient.Builder builder = new ZkClient.Builder();
      builder.setZkServer(zkAddr).setMonitorKey(TEST_KEY).setMonitorType(TEST_TAG)
          .setMonitorRootPathOnly(true);
      final ZkClient zkClient = builder.build();
      long lastSessionId = zkClient.getSessionId();
      long previousSessionExpiredCount =
          (long) beanServer.getAttribute(name, "ExpiredSessionCounter");

      for (int i = 0; i < 3; i++) {
        ZkTestHelper.expireSession(zkClient);
        long newSessionId = zkClient.getSessionId();
        Assert.assertTrue(newSessionId != lastSessionId,
            "New session id should not equal to expired session id.");
        lastSessionId = newSessionId;
      }
      Assert.assertEquals((long) beanServer.getAttribute(name, "ExpiredSessionCounter"),
          previousSessionExpiredCount + 3);

      zkClient.close();
    } finally {
      zkServer.shutdown();
    }
  }

  /*
   * This test checks that a valid session can successfully create an ephemeral node.
   */
  @Test
  public void testCreateEphemeralWithValidSession()
      throws Exception {

    final String originalSessionId = Long.toHexString(_zkClient.getSessionId());
    final String path = "/" + TestHelper.getTestMethodName();
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
    Assert.assertEquals(Long.toHexString(stat.getEphemeralOwner()), originalSessionId,
        "Ephemeral node is created by an unexpected session");

    // Delete the node to clean up, otherwise, the ephemeral node would be existed
    // until the session of its owner expires.
    _zkClient.delete(path);
  }

  /*
   * This test validates that when ZK_AUTOSYNC_ENABLED_DEFAULT is enabled, sync() would be issued
   * before handleNewSession. ZKclient would not see stale data.
   */
  @Test
  public void testAutoSyncWithNewSessionEstablishment() throws Exception {
    final String path = "/" + TestHelper.getTestMethodName();
    final String data = "Hello Helix 2";

    // Wait until the ZkClient has got a new session.
    Assert.assertTrue(_zkClient.waitUntilConnected(1, TimeUnit.SECONDS));

    try {
      // Create node.
      _zkClient.create(path, data, CreateMode.PERSISTENT);
    } catch (Exception ex) {
      Assert.fail("Failed to create ephemeral node.", ex);
    }

    // Expire the original session.
    ZkTestHelper.expireSession(_zkClient);

    // Verify the node is created and its data is correct.
    Stat stat = new Stat();
    String nodeData = null;
    try {
       nodeData = _zkClient.readData(path, stat, true);
    } catch (ZkException e) {
      Assert.fail("fail to read data");
    }
    Assert.assertEquals(nodeData, data, "Data is not correct.");
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
  public void testCreateEphemeralWithMismatchedSession()
      throws Exception {
    final String methodName = TestHelper.getTestMethodName();

    final long originalSessionId = _zkClient.getSessionId();
    final String originalHexSessionId = Long.toHexString(originalSessionId);
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
      } catch (ZkClientException ex) {
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
  public void testConnectionLossWhileCreateEphemeral()
      throws Exception {
    final String methodName = TestHelper.getTestMethodName();

    final ZkClient zkClient =
        new ZkClient.Builder().setZkServer(ZkTestBase.ZK_ADDR).setOperationRetryTimeout(3000L) // 3 seconds
            .build();

    final String expectedSessionId = Long.toHexString(zkClient.getSessionId());
    final String path = "/" + methodName;
    final String data = "data";

    Assert.assertFalse(zkClient.exists(path));

    // Shutdown zk server so zk operations will fail due to disconnection.
    TestHelper.stopZkServer(_zkServerMap.get(ZK_ADDR));

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
        creationThread.join();
        Assert.fail("Failed to receive a ConnectionLossException after zookeeper has shutdown.");
      }
    } finally {
      zkClient.close();
      // Recover zk server.
      _zkServerMap.get(ZK_ADDR).start();
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
  public void testRetryUntilConnectedAfterConnectionLoss()
      throws Exception {
    final String methodName = TestHelper.getTestMethodName();

    final String expectedSessionId = Long.toHexString(_zkClient.getSessionId());
    final String path = "/" + methodName;
    final String data = "data";

    Assert.assertFalse(_zkClient.exists(path));

    // Shutdown zk server so zk operations will fail due to disconnection.
    TestHelper.stopZkServer(_zkServerMap.get(ZK_ADDR));

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
    _zkServerMap.get(ZK_ADDR).start();

    // Wait for creating ephemeral node successfully.
    final boolean creationThreadTerminated = countDownLatch.await(10, TimeUnit.SECONDS);
    if (!creationThreadTerminated) {
      running.set(false);
      creationThread.join();
      Assert.fail("Failed to reconnect to zk server and create ephemeral node"
          + " after zk server is recovered.");
    }

    Stat stat = new Stat();
    String nodeData = _zkClient.readData(path, stat, true);

    Assert.assertNotNull(nodeData, "Failed to create ephemeral node: " + path);
    Assert.assertEquals(nodeData, data, "Data is not correct.");
    Assert.assertTrue(stat.getEphemeralOwner() != 0L,
        "Ephemeral owner should NOT be zero because the node is an ephemeral node.");
    Assert.assertEquals(Long.toHexString(stat.getEphemeralOwner()), expectedSessionId,
        "Ephemeral node is created by an unexpected session");

    // Delete the node to clean up, otherwise, the ephemeral node would be existed until the session
    // of its owner expires.
    _zkClient.delete(path);
  }

  /*
   * Test the scenario where we have actually lost connection to ZK but
   * the internal client connection state assumes that we are still connected.
   * Once we re-start the ZK, the creation operation will finish.
   * How this test simulates the steps is:
   * 1. Shuts down zk server
   * 2. Starts a creation thread to create an ephemeral node in the original zk session
   * 3. Creation operation loses connection and keeps retrying
   * 4. Restarts zk server and Zk session is recovered
   * 5. zk client reconnects successfully and creates an ephemeral node
   */
  @Test(timeOut = 5 * 60 * 1000L, dependsOnMethods = "testRetryUntilConnectedAfterConnectionLoss")
  public void testRetryUntilConnectedWithZkCleanupStuck() throws Exception {
    final String methodName = TestHelper.getTestMethodName();

    final String expectedSessionId = Long.toHexString(_zkClient.getSessionId());
    final String path = "/" + methodName;
    final String data = "data";

    Assert.assertFalse(_zkClient.exists(path));

    // Shutdown zk server so zk operations will fail due to disconnection.
    TestHelper.stopZkServer(_zkServerMap.get(ZK_ADDR));

    // Make sure we mark our internal state as "Connected"
    _zkClient.setCurrentState(KeeperState.SyncConnected);

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
    _zkServerMap.get(ZK_ADDR).start();

    // Wait for creating ephemeral node successfully.
    final boolean creationThreadTerminated = countDownLatch.await(10, TimeUnit.SECONDS);
    if (!creationThreadTerminated) {
      running.set(false);
      creationThread.join();
      Assert.fail("Failed to reconnect to zk server and create ephemeral node"
          + " after zk server is recovered.");
    }

    Stat stat = new Stat();
    String nodeData = _zkClient.readData(path, stat, true);

    Assert.assertNotNull(nodeData, "Failed to create ephemeral node: " + path);
    Assert.assertEquals(nodeData, data, "Data is not correct.");
    Assert.assertTrue(stat.getEphemeralOwner() != 0L,
        "Ephemeral owner should NOT be zero because the node is an ephemeral node.");
    Assert.assertEquals(Long.toHexString(stat.getEphemeralOwner()), expectedSessionId,
        "Ephemeral node is created by an unexpected session");

    // Delete the node to clean up, otherwise, the ephemeral node would be existed until the session
    // of its owner expires.
    _zkClient.delete(path);
  }

  @Test
  public void testWaitForEstablishedSession() {
    ZkClient zkClient = new ZkClient(ZkTestBase.ZK_ADDR);
    Assert.assertTrue(zkClient.waitForEstablishedSession(1, TimeUnit.SECONDS) != 0L);
    TestHelper.stopZkServer(_zkServerMap.get(ZK_ADDR));
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
    _zkServerMap.get(ZK_ADDR).start();
  }

  @Test
  public void testAsyncWriteOperations() {
    ZkClient zkClient = new ZkClient(ZkTestBase.ZK_ADDR);
    String originSizeLimit =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
    System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES, "2000");
    try {
      zkClient.setZkSerializer(new ZNRecordSerializer());

      ZNRecord oversizeZNRecord = new ZNRecord("Oversize");
      char[] buff = new char[1024];
      Random ran = new Random();
      for (int i = 0; i < 1024; i++) {
        buff[i] = (char) (ran.nextInt(26) + 'a');
      }
      String buffString = new String(buff);
      for (int i = 0; i < 1024; i++) {
        oversizeZNRecord.setSimpleField(Integer.toString(i), buffString);
      }

      // ensure /tmp exists for the test
      if (!zkClient.exists("/tmp")) {
        zkClient.create("/tmp", null, CreateMode.PERSISTENT);
      }

      org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks.CreateCallbackHandler
          createCallback =
          new org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks.CreateCallbackHandler();

      zkClient.asyncCreate("/tmp/async", null, CreateMode.PERSISTENT, createCallback);
      createCallback.waitForSuccess();
      Assert.assertEquals(createCallback.getRc(), 0);
      Assert.assertTrue(zkClient.exists("/tmp/async"));

      Assert.assertFalse(zkClient.exists("/tmp/asyncOversize"));
      // try to create oversize node, should fail
      zkClient.asyncCreate("/tmp/asyncOversize", oversizeZNRecord, CreateMode.PERSISTENT,
          createCallback);
      createCallback.waitForSuccess();
      Assert.assertEquals(createCallback.getRc(), KeeperException.Code.MARSHALLINGERROR.intValue());
      Assert.assertFalse(zkClient.exists("/tmp/asyncOversize"));

      ZNRecord normalZNRecord = new ZNRecord("normal");
      normalZNRecord.setSimpleField("key", buffString);

      org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks.SetDataCallbackHandler
          setDataCallbackHandler =
          new org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks.SetDataCallbackHandler();

      zkClient.asyncSetData("/tmp/async", normalZNRecord, -1, setDataCallbackHandler);
      setDataCallbackHandler.waitForSuccess();
      Assert.assertEquals(setDataCallbackHandler.getRc(), 0);

      zkClient.asyncSetData("/tmp/async", oversizeZNRecord, -1, setDataCallbackHandler);
      setDataCallbackHandler.waitForSuccess();
      Assert.assertEquals(setDataCallbackHandler.getRc(),
          KeeperException.Code.MARSHALLINGERROR.intValue());
      Assert.assertEquals(zkClient.readData("/tmp/async"), normalZNRecord);
    } finally {
      TestHelper
          .resetSystemProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
              originSizeLimit);
      zkClient.delete("/tmp/async");
      zkClient.delete("/tmp/asyncOversize");
      zkClient.close();
    }
  }

  @Test
  public void testAsyncWriteByExpectedSession() throws Exception {
    ZkClient zkClient = new ZkClient(ZkTestBase.ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    String sessionId = Long.toHexString(zkClient.getSessionId());
    String path = "/" + TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName();
    SessionAwareZNRecord record = new SessionAwareZNRecord("test");

    // Set a dummy session id string to be mismatched with the real session id in ZkClient.
    record.setExpectedSessionId("ExpectedSession");
    ZkAsyncCallbacks.CreateCallbackHandler createCallback =
        new ZkAsyncCallbacks.CreateCallbackHandler();

    try {
      zkClient.asyncCreate(path, record, CreateMode.PERSISTENT, createCallback);
      Assert.fail("Invalid session should not create znode");
    } catch (ZkSessionMismatchedException expected) {
      Assert.assertEquals(expected.getMessage(),
          "Failed to get expected zookeeper instance! There is a session id mismatch. Expected: "
              + "ExpectedSession. Actual: " + sessionId);

      // Ensure the async callback is cancelled because of the exception
      Assert.assertTrue(createCallback.waitForSuccess(), "Callback operation should be done");
      Assert.assertEquals(createCallback.getRc(), KeeperException.Code.APIERROR.intValue());
    }

    Assert.assertFalse(zkClient.exists(path));

    // A valid session should be able to create the znode.
    record.setExpectedSessionId(sessionId);
    createCallback = new ZkAsyncCallbacks.CreateCallbackHandler();
    zkClient.asyncCreate(path, record, CreateMode.PERSISTENT, createCallback);

    Assert.assertTrue(createCallback.waitForSuccess(), "Callback operation should be done");
    Assert.assertEquals(createCallback.getRc(), 0);
    Assert.assertTrue(zkClient.exists(path));

    // Test asyncSetData() failed by mismatched session
    record.setExpectedSessionId("ExpectedSession");
    ZkAsyncCallbacks.SetDataCallbackHandler setDataCallback =
        new ZkAsyncCallbacks.SetDataCallbackHandler();
    try {
      zkClient.asyncSetData(path, record, 0, setDataCallback);
      Assert.fail("Invalid session should not change znode data");
    } catch (ZkSessionMismatchedException expected) {
      Assert.assertEquals(expected.getMessage(),
          "Failed to get expected zookeeper instance! There is a session id mismatch. Expected: "
              + "ExpectedSession. Actual: " + sessionId);

      // Ensure the async callback is cancelled because of the exception
      Assert.assertTrue(setDataCallback.waitForSuccess(), "Callback operation should be done");
      Assert.assertEquals(setDataCallback.getRc(), KeeperException.Code.APIERROR.intValue());
    }

    TestHelper.verify(() -> zkClient.delete(path), TestHelper.WAIT_DURATION);
    zkClient.close();
  }

  /*
   * Tests getChildren() when there are an excessive number of children and connection loss happens,
   * the operation should terminate and exit retry loop.
   */
  @Test
  public void testGetChildrenOnLargeNumChildren() throws Exception {
    final String methodName = TestHelper.getTestMethodName();
    System.out.println("Start test: " + methodName);
    // Create 110K children to make packet length of children exceed 4 MB
    // and cause connection loss for getChildren() operation
    String path = "/" + methodName;
    int numOps = 110;
    int numOpInOps = 1000;
    // All the paths that are going to be created as children nodes, plus one parent node
    // Record paths so can be deleted at the end of the test
    String[] nodePaths = new String[numOps * numOpInOps + 1];
    nodePaths[numOps * numOpInOps] = path;

    _zkClient.createPersistent(path);

    for (int i = 0; i < numOps; i++) {
      List<Op> ops = new ArrayList<>(numOpInOps);
      for (int j = 0; j < numOpInOps; j++) {
        String childPath = path + "/" + UUID.randomUUID().toString();
        nodePaths[numOpInOps * i + j] = childPath;
        ops.add(
            Op.create(childPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
      }
      // Reduce total creation time by batch creating znodes
      _zkClient.multi(ops);
    }

    try {
      _zkClient.getChildren(path);
      Assert.fail("Should not successfully get children because of connection loss.");
    } catch (ZkException expected) {
      Assert.assertEquals(expected.getMessage(),
          "org.apache.zookeeper.KeeperException$MarshallingErrorException: "
              + "KeeperErrorCode = MarshallingError");
    } finally {
      // Delete children ephemeral znodes
      _zkClient.close();
      _zkClient = new ZkClient(ZkTestBase.ZK_ADDR);

      Assert.assertTrue(TestHelper.verify(() -> {
        for (String toDelete: nodePaths) {
          try {
            _zkClient.delete(toDelete);
          } catch (ZkException e) {
            return false;
          }
        }
        return true;
      }, TestHelper.WAIT_DURATION));
    }
    System.out.println("End test: " + methodName);
  }

  @Test(expectedExceptions = ZkClientException.class,
      expectedExceptionsMessageRegExp = "Data size of path .* is greater than write size limit 1024000 bytes")
  public void testDataSizeGreaterThanLimit() {
    // Creating should fail because size is greater than limit.
    _zkClient.createPersistent("/" + TestHelper.getTestMethodName(), new byte[1001 * 1024]);
  }

  @Test
  public void testDataSizeLessThanLimit() throws Exception {
    String path = "/" + TestHelper.getTestMethodName();
    Assert.assertFalse(_zkClient.exists(path));
    // Creating znode is successful.
    _zkClient.createPersistent(path, new byte[1024]);

    Assert.assertTrue(_zkClient.exists(path));

    TestHelper.verify(() -> _zkClient.delete(path), TestHelper.WAIT_DURATION);
  }

  // Tests znrecord serializer write size limit is invalid: greater than size limit in ZkClient
  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
      "ZNRecord serializer write size limit .* is greater than ZkClient size limit .*")
  public void testInvalidWriteSizeLimitConfig() {
    String originSerializerLimit =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
    System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
        String.valueOf(ZNRecord.SIZE_LIMIT + 1024));

    ZkClient zkClient = null;
    try {
      // Constructing ZkClient should throw exception because of invalid write size limit config
      zkClient = new ZkClient(ZkTestBase.ZK_ADDR);
    } finally {
      TestHelper
          .resetSystemProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
              originSerializerLimit);
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  @Test
  void testDeleteRecursivelyAtomic() {
    System.out.println("Start test: " + TestHelper.getTestMethodName());
    String grandParent = "/testDeleteRecursively";
    String parent = grandParent + "/parent";
    String child1 = parent + "/child1";
    String child2 = parent + "/child2";
    _zkClient.createPersistent(grandParent);
    _zkClient.createPersistent(parent);
    _zkClient.createPersistent(child1);
    _zkClient.createPersistent(child2);
    Assert.assertTrue(_zkClient.exists(grandParent));
    Assert.assertFalse(_zkClient.getChildren(parent).isEmpty());

    // Test calling delete on same path twice
    try {
      _zkClient.deleteRecursivelyAtomic(Arrays.asList(grandParent, grandParent));
      Assert.fail("Operation should not succeed when attempting to delete same path twice");
    } catch (ZkClientException expected) {
      // Caught expected exception
    }

    Assert.assertTrue(_zkClient.exists(grandParent));
    Assert.assertFalse(_zkClient.getChildren(parent).isEmpty());

    // Test calling delete on path that is child of another path in the list
    try {
      _zkClient.deleteRecursivelyAtomic(Arrays.asList(grandParent, parent));
      Assert.fail("Operation should not succeed when attempting to delete same path twice");
    } catch (ZkClientException expected) {
      // Caught expected exception
    }

    // Test calling delete on single node
    Assert.assertTrue(_zkClient.exists(child2));
    _zkClient.deleteRecursivelyAtomic(child2);
    Assert.assertFalse(_zkClient.exists(child2));

    Assert.assertTrue(_zkClient.exists(grandParent));
    Assert.assertFalse(_zkClient.getChildren(parent).isEmpty());

    // Test successfully delete multiple paths. Also that operation succeeds when attempting to delete non-existent path
    String newNode = "/newNode";
    _zkClient.createPersistent(newNode);
    Assert.assertTrue(_zkClient.exists(newNode));

    String nonexistentPath = grandParent + "/nonexistent";
    Assert.assertFalse(_zkClient.exists(nonexistentPath));

    _zkClient.deleteRecursivelyAtomic(Arrays.asList(grandParent, newNode, nonexistentPath));
    Assert.assertFalse(_zkClient.exists(grandParent));
    Assert.assertFalse(_zkClient.exists(newNode));
  }
}
