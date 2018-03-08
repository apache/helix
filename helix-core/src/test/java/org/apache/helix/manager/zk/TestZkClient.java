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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.ZkClientMonitor;
import org.apache.helix.monitoring.mbeans.ZkClientPathMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
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

public class TestZkClient extends ZkUnitTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestZkClient.class);

  ZkClient _zkClient;

  @BeforeClass
  public void beforeClass() {
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
  }

  @AfterClass
  public void afterClass() {
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

    _zkClient.writeData(path, new ZNRecord("Test"));
    newStat = _zkClient.getStat(path);
    AssertJUnit.assertNotSame(stat, newStat);
  }

  @Test()
  void testSessionExpire() throws Exception {
    IZkStateListener listener = new IZkStateListener() {

      @Override
      public void handleStateChanged(KeeperState state) throws Exception {
        System.out.println("In Old connection New state " + state);
      }

      @Override
      public void handleNewSession() throws Exception {
        System.out.println("In Old connection New session");
      }

      @Override
      public void handleSessionEstablishmentError(Throwable var1) throws Exception {
      }
    };

    _zkClient.subscribeStateChanges(listener);
    ZkConnection connection = ((ZkConnection) _zkClient.getConnection());
    ZooKeeper zookeeper = connection.getZookeeper();
    System.out.println("old sessionId= " + zookeeper.getSessionId());
    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        System.out.println("In New connection In process event:" + event);
      }
    };
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
    System.out.println("After session expiry sessionId= " + zookeeper.getSessionId());
  }

  @Test(expectedExceptions = HelixException.class, expectedExceptionsMessageRegExp = "Data size larger than 1M.*")
  void testDataSizeLimit() {
    ZNRecord data = new ZNRecord(new String(new char[1024 * 1024]));
    _zkClient.writeData("/test", data, -1);
  }

  @Test
  public void testZkClientMonitor() throws Exception {
    final String TEST_TAG = "test_monitor";
    final String TEST_KEY = "test_key";
    final String TEST_DATA = "testData";
    final String TEST_ROOT = "/my_cluster/IDEALSTATES";
    final String TEST_NODE = "/test_zkclient_monitor";
    final String TEST_PATH = TEST_ROOT + TEST_NODE;

    ZkClient.Builder builder = new ZkClient.Builder();
    builder.setZkServer(ZK_ADDR).setMonitorKey(TEST_KEY).setMonitorType(TEST_TAG)
        .setMonitorRootPathOnly(false);
    ZkClient zkClient = builder.build();

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
    Assert.assertEquals((long) beanServer.getAttribute(rootname, "ReadCounter"), 6);

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

    // Test data change count
    final Lock lock = new ReentrantLock();
    final Condition callbackFinish = lock.newCondition();
    zkClient.subscribeDataChanges(TEST_PATH, new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception {
      }

      @Override
      public void handleDataDeleted(String dataPath) throws Exception {
        lock.lock();
        try {
          callbackFinish.signal();
        } finally {
          lock.unlock();
        }
      }
    });
    lock.lock();
    _zkClient.delete(TEST_PATH);
    Assert.assertTrue(callbackFinish.await(10, TimeUnit.SECONDS));
    Assert.assertEquals((long) beanServer.getAttribute(name, "DataChangeEventCounter"), 1);
  }
}
