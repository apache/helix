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

import org.apache.helix.metaclient.api.DataUpdater;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.api.DirectChildChangeListener;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.helix.metaclient.api.ConnectStateChangeListener;
import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.metaclient.api.Op;
import org.apache.helix.metaclient.api.OpResult;
import org.apache.helix.metaclient.exception.MetaClientNoNodeException;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.helix.metaclient.api.DataChangeListener.ChangeType.ENTRY_UPDATE;
import static org.apache.helix.metaclient.api.MetaClientInterface.EntryMode.CONTAINER;
import static org.apache.helix.metaclient.api.MetaClientInterface.EntryMode.PERSISTENT;


public class TestZkMetaClient extends ZkMetaClientTestBase{

  private static final String TRANSACTION_TEST_PARENT_PATH = "/transactionOpTestPath";
  private static final String TEST_INVALID_PATH = "/_invalid/a/b/c";

  private final Object _syncObject = new Object();

  @Test
  public void testCreate() {
    final String key = "/TestZkMetaClient_testCreate";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(key, ENTRY_STRING_VALUE);
      Assert.assertNotNull(zkMetaClient.exists(key));

      try {
        zkMetaClient.create("a/b/c", "invalid_path");
        Assert.fail("Should have failed with incorrect path.");
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  public void testCreateContainer() {
    final String key = "/TestZkMetaClient_testCreateContainer";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(key, ENTRY_STRING_VALUE, CONTAINER);
      Assert.assertNotNull(zkMetaClient.exists(key));
    }
  }

  @Test
  public void testCreateTTL() {
    final String key = "/TestZkMetaClient_testTTL";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.createWithTTL(key, ENTRY_STRING_VALUE, 1000);
      Assert.assertNotNull(zkMetaClient.exists(key));
    }
  }

  @Test
  public void testRenewTTL() {
    final String key = "/TestZkMetaClient_testRenewTTL_1";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.createWithTTL(key, ENTRY_STRING_VALUE, 10000);
      Assert.assertNotNull(zkMetaClient.exists(key));
      MetaClientInterface.Stat stat = zkMetaClient.exists(key);
      zkMetaClient.renewTTLNode(key);
      // Renewing a ttl node changes the nodes modified_time. Should be different
      // from the time the node was created.
      Assert.assertNotSame(stat.getCreationTime(), stat.getModifiedTime());
      try {
        zkMetaClient.renewTTLNode(TEST_INVALID_PATH);
      } catch (MetaClientNoNodeException ignored) {
      }
    }
  }

  @Test
  public void testGet() {
    final String key = "/TestZkMetaClient_testGet";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      String value;
      zkMetaClient.create(key, ENTRY_STRING_VALUE);
      String dataValue = zkMetaClient.get(key);
      Assert.assertEquals(dataValue, ENTRY_STRING_VALUE);

      value = zkMetaClient.get(key + "/a/b/c");
      Assert.assertNull(value);

      zkMetaClient.delete(key);

      value = zkMetaClient.get(key);
      Assert.assertNull(value);
    }
  }

  @Test
  public void testSet() {
    final String key = "/TestZkMetaClient_testSet";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(key, ENTRY_STRING_VALUE);
      String testValueV1 = ENTRY_STRING_VALUE + "-v1";
      String testValueV2 = ENTRY_STRING_VALUE + "-v2";

      // test set() with no expected version and validate result.
      zkMetaClient.set(key, testValueV1, -1);
      Assert.assertEquals(zkMetaClient.get(key), testValueV1);
      MetaClientInterface.Stat entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(entryStat.getVersion(), 1);
      Assert.assertEquals(entryStat.getEntryType().name(), PERSISTENT.name());

      // test set() with expected version and validate result and new version number
      zkMetaClient.set(key, testValueV2, 1);
      entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(zkMetaClient.get(key), testValueV2);
      Assert.assertEquals(entryStat.getVersion(), 2);

      // test set() with a wrong version
      try {
        zkMetaClient.set(key, "test-node-changed", 10);
        Assert.fail("No reach.");
      } catch (MetaClientException ex) {
        Assert.assertEquals(ex.getClass().getName(),
            "org.apache.helix.metaclient.exception.MetaClientBadVersionException");
      }
      zkMetaClient.delete(key);
    }
  }

  @Test
  public void testUpdate() {
    final String key = "/TestZkMetaClient_testUpdate";
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR).build();
    try (ZkMetaClient<Integer> zkMetaClient = new ZkMetaClient<>(config)) {
      zkMetaClient.connect();
      int initValue = 3;
      zkMetaClient.create(key, initValue);
      MetaClientInterface.Stat entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(entryStat.getVersion(), 0);

      // test update() and validate entry value and version
      Integer newData = zkMetaClient.update(key, new DataUpdater<Integer>() {
        @Override
        public Integer update(Integer currentData) {
          return currentData + 1;
        }
      });
      Assert.assertEquals((int) newData, (int) initValue + 1);

      entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(entryStat.getVersion(), 1);

      newData = zkMetaClient.update(key, new DataUpdater<Integer>() {

        @Override
        public Integer update(Integer currentData) {
          return currentData + 1;
        }
      });

      entryStat = zkMetaClient.exists(key);
      Assert.assertEquals(entryStat.getVersion(), 2);
      Assert.assertEquals((int) newData, (int) initValue + 2);
      zkMetaClient.delete(key);
    }
  }

  @Test
  public void testGetAndCountChildrenAndRecursiveDelete() {
    final String key = "/TestZkMetaClient_testGetAndCountChildren";
    List<String> childrenNames = Arrays.asList("/c1", "/c2", "/c3");

    // create child nodes and validate retrieved children count and names
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(key, ENTRY_STRING_VALUE);
      Assert.assertEquals(zkMetaClient.countDirectChildren(key), 0);
      for (String str : childrenNames) {
        zkMetaClient.create(key + str, ENTRY_STRING_VALUE);
      }

      List<String> retrievedChildrenNames = zkMetaClient.getDirectChildrenKeys(key);
      Assert.assertEquals(retrievedChildrenNames.size(), childrenNames.size());
      Set<String> childrenNameSet = new HashSet<>(childrenNames);
      for (String str : retrievedChildrenNames) {
        Assert.assertTrue(childrenNameSet.contains("/" + str));
      }

      // recursive delete and validate
      Assert.assertEquals(zkMetaClient.countDirectChildren(key), childrenNames.size());
      Assert.assertNotNull(zkMetaClient.exists(key));
      zkMetaClient.recursiveDelete(key);
      Assert.assertNull(zkMetaClient.exists(key));
    }
  }

  @Test
  public void testDataChangeListenerTriggerWithZkWatcher() throws Exception {
    final String path = "/TestZkMetaClient_testTriggerWithZkWatcher";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      MockDataChangeListener listener = new MockDataChangeListener();
      zkMetaClient.subscribeDataChange(path, listener, false);
      zkMetaClient.create(path, "test-node");
      int expectedCallCount = 0;
      synchronized (_syncObject) {
        while (listener.getTriggeredCount() == expectedCallCount) {
          _syncObject.wait(DEFAULT_TIMEOUT_MS);
        }
        expectedCallCount++;
        Assert.assertEquals(listener.getTriggeredCount(), expectedCallCount);
        Assert.assertEquals(listener.getLastEventType(), DataChangeListener.ChangeType.ENTRY_CREATED);
      }
      zkMetaClient.set(path, "test-node-changed", -1);
      synchronized (_syncObject) {
        while (listener.getTriggeredCount() == expectedCallCount) {
          _syncObject.wait(DEFAULT_TIMEOUT_MS);
        }
        expectedCallCount++;
        Assert.assertEquals(listener.getTriggeredCount(), expectedCallCount);
        Assert.assertEquals(listener.getLastEventType(), DataChangeListener.ChangeType.ENTRY_UPDATE);
      }
      zkMetaClient.delete(path);
      synchronized (_syncObject) {
        while (listener.getTriggeredCount() == expectedCallCount) {
          _syncObject.wait(DEFAULT_TIMEOUT_MS);
        }
        expectedCallCount++;
        Assert.assertEquals(listener.getTriggeredCount(), expectedCallCount);
        Assert.assertEquals(listener.getLastEventType(), DataChangeListener.ChangeType.ENTRY_DELETED);
      }
      // unregister listener, expect no more call
      zkMetaClient.unsubscribeDataChange(path, listener);
      zkMetaClient.create(path, "test-node");
      synchronized (_syncObject) {
        _syncObject.wait(DEFAULT_TIMEOUT_MS);
        Assert.assertEquals(listener.getTriggeredCount(), expectedCallCount);
      }
      // register a new non-persistent listener
      try {
        zkMetaClient.subscribeOneTimeDataChange(path, new MockDataChangeListener(), false);
        Assert.fail("One-time listener is not supported, NotImplementedException should be thrown.");
      } catch (NotImplementedException e) {
        // expected
      }
    }
  }

  @Test(dependsOnMethods = "testDataChangeListenerTriggerWithZkWatcher")
  public void testMultipleDataChangeListeners() throws Exception {
    final String basePath = "/TestZkMetaClient_testMultipleDataChangeListeners";
    final int count = 5;
    final String testData = "test-data";
    final AtomicBoolean dataExpected = new AtomicBoolean(true);
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      Map<String, Set<DataChangeListener>> listeners = new HashMap<>();
      CountDownLatch countDownLatch = new CountDownLatch(count);
      zkMetaClient.create(basePath + "_1", testData);
      // create paths
      for (int i = 0; i < 2; i++) {
        String path = basePath + "_" + i;
        listeners.put(path, new HashSet<>());
        // 5 listeners for each path
        for (int j = 0; j < count; j++) {
          DataChangeListener listener = new DataChangeListener() {
            @Override
            public void handleDataChange(String key, Object data, ChangeType changeType) {
              countDownLatch.countDown();
              dataExpected.set(dataExpected.get() && testData.equals(data));
            }
          };
          listeners.get(path).add(listener);
          zkMetaClient.subscribeDataChange(path, listener, false);
        }
      }
      zkMetaClient.set(basePath + "_1", testData, -1);
      Assert.assertTrue(countDownLatch.await(5000, TimeUnit.MILLISECONDS));
      Assert.assertTrue(dataExpected.get());
    }
  }

  @Test
  public void testDirectChildChangeListener() throws Exception {
    final String basePath = "/TestZkMetaClient_testDirectChildChangeListener";
    final int count = 1000;
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      CountDownLatch countDownLatch = new CountDownLatch(count);
      DirectChildChangeListener listener = new DirectChildChangeListener() {
        @Override
        public void handleDirectChildChange(String key) throws Exception {
          countDownLatch.countDown();
        }
      };
      zkMetaClient.create(basePath, "");
      Assert.assertTrue(
          zkMetaClient.subscribeDirectChildChange(basePath, listener, false)
              .isRegistered());
      for(int i=0; i<1000; ++i){
        zkMetaClient.create(basePath + "/child_" +i, "test-data");
      }
      // Verify no one time watcher is registered. Only one persist listener is registered.
      Map<String, List<String>> watchers = TestUtil.getZkWatch(zkMetaClient.getZkClient());
      Assert.assertEquals(watchers.get("persistentWatches").size(), 1);
      Assert.assertEquals(watchers.get("persistentWatches").get(0), basePath);
      Assert.assertEquals(watchers.get("childWatches").size(), 0);
      Assert.assertEquals(watchers.get("dataWatches").size(), 0);
      Assert.assertTrue(countDownLatch.await(5000, TimeUnit.MILLISECONDS));

      zkMetaClient.unsubscribeDirectChildChange(basePath, listener);
      // verify that no listener is registered on any path
      watchers = TestUtil.getZkWatch(zkMetaClient.getZkClient());
      Assert.assertEquals(watchers.get("persistentWatches").size(), 0);
      Assert.assertEquals(watchers.get("childWatches").size(), 0);
      Assert.assertEquals(watchers.get("dataWatches").size(), 0);
    }
  }

  @Test
  public void testDataChangeListener() throws Exception {
    final String basePath = "/TestZkMetaClient_testDataChangeListener";
    final int count = 200;
    final int[] get_count = {0};
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      CountDownLatch countDownLatch = new CountDownLatch(count);
      DataChangeListener listener = new DataChangeListener() {

        @Override
        public void handleDataChange(String key, Object data, ChangeType changeType)
            throws Exception {
          if(changeType == ENTRY_UPDATE) {
            get_count[0]++;
            countDownLatch.countDown();
          }
        }
      };
      zkMetaClient.create(basePath, "");
      Assert.assertTrue(
          zkMetaClient.subscribeDataChange(basePath, listener, false)
      );
      // Verify no one time watcher is registered. Only one persist listener is registered.
      Map<String, List<String>> watchers = TestUtil.getZkWatch(zkMetaClient.getZkClient());
      Assert.assertEquals(watchers.get("persistentWatches").size(), 1);
      Assert.assertEquals(watchers.get("persistentWatches").get(0), basePath);
      Assert.assertEquals(watchers.get("childWatches").size(), 0);
      Assert.assertEquals(watchers.get("dataWatches").size(), 0);

      for (int i=0; i<200; ++i) {
        zkMetaClient.set(basePath, "data7" + i, -1);
      }
      Assert.assertTrue(countDownLatch.await(5000, TimeUnit.MILLISECONDS));


      zkMetaClient.unsubscribeDataChange(basePath, listener);
      // verify that no listener is registered on any path
      watchers = TestUtil.getZkWatch(zkMetaClient.getZkClient());
      watchers = TestUtil.getZkWatch(zkMetaClient.getZkClient());
      Assert.assertEquals(watchers.get("persistentWatches").size(), 0);
      Assert.assertEquals(watchers.get("childWatches").size(), 0);
      Assert.assertEquals(watchers.get("dataWatches").size(), 0);

    }
  }

  /**
   * Transactional op calls zk.multi() with a set of ops (operations)
   * and the return values are converted into metaclient opResults.
   * This test checks whether each op was run by verifying its opResult and
   * the created/deleted/set path in zk.
   */
  @Test
  public void testTransactionOps() {
    String test_name = "/test_transaction_ops";

    try(ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();

      //Create Nodes
      List<Op> ops = Arrays.asList(
          Op.create(TRANSACTION_TEST_PARENT_PATH, new byte[0], MetaClientInterface.EntryMode.PERSISTENT),
          Op.create(TRANSACTION_TEST_PARENT_PATH + test_name, new byte[0], MetaClientInterface.EntryMode.PERSISTENT),
          Op.delete(TRANSACTION_TEST_PARENT_PATH + test_name, -1),
          Op.create(TRANSACTION_TEST_PARENT_PATH + test_name, new byte[0], MetaClientInterface.EntryMode.PERSISTENT),
          Op.set(TRANSACTION_TEST_PARENT_PATH + test_name, new byte[0], -1));

      //Execute transactional support on operations
      List<OpResult> opResults = zkMetaClient.transactionOP(ops);

      //Verify opResults types
      Assert.assertTrue(opResults.get(0) instanceof OpResult.CreateResult);
      Assert.assertTrue(opResults.get(1) instanceof OpResult.CreateResult);
      Assert.assertTrue(opResults.get(2) instanceof OpResult.DeleteResult);
      Assert.assertTrue(opResults.get(4) instanceof OpResult.SetDataResult);

      //Verify paths have been created
      MetaClientInterface.Stat entryStat = zkMetaClient.exists(TRANSACTION_TEST_PARENT_PATH + test_name);
      Assert.assertNotNull(entryStat, "Path should have been created.");

      //Cleanup
      zkMetaClient.recursiveDelete(TRANSACTION_TEST_PARENT_PATH);
      if (zkMetaClient.exists(TRANSACTION_TEST_PARENT_PATH) != null) {
        Assert.fail("Parent Path should have been removed.");
      }
    }
  }

  /**
   * This test calls transactionOp on an invalid path.
   * It checks that the invalid path has not been created to verify the
   * "all or nothing" behavior of transactionOp.
   * @throws KeeperException
   */
  @Test(dependsOnMethods = "testTransactionOps")
  public void testTransactionFail() {
    String test_name = "/test_transaction_fail";
    try(ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      //Create Nodes
      List<Op> ops = Arrays.asList(
          Op.create(TRANSACTION_TEST_PARENT_PATH, new byte[0], MetaClientInterface.EntryMode.PERSISTENT),
          Op.create(TRANSACTION_TEST_PARENT_PATH + test_name, new byte[0], MetaClientInterface.EntryMode.PERSISTENT),
          Op.create(TEST_INVALID_PATH, new byte[0], MetaClientInterface.EntryMode.PERSISTENT));

      try {
        zkMetaClient.transactionOP(ops);
        Assert.fail(
            "Should have thrown an exception. Cannot run transactional create OP on incorrect path.");
      } catch (Exception e) {
        MetaClientInterface.Stat entryStat = zkMetaClient.exists(TRANSACTION_TEST_PARENT_PATH);
        Assert.assertNull(entryStat);
      }
    }
  }

  private class MockDataChangeListener implements DataChangeListener {
    private final AtomicInteger _triggeredCount = new AtomicInteger(0);
    private volatile ChangeType _lastEventType;

    @Override
    public void handleDataChange(String key, Object data, ChangeType changeType) {
      _triggeredCount.getAndIncrement();
      _lastEventType = changeType;
      synchronized (_syncObject) {
        _syncObject.notifyAll();
      }
    }

    int getTriggeredCount() {
      return _triggeredCount.get();
    }

    ChangeType getLastEventType() {
      return _lastEventType;
    }
  }
}
