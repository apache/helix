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

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.metaclient.api.*;
import org.apache.helix.metaclient.datamodel.DataRecord;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.recipes.lock.DataRecordSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import static org.apache.helix.metaclient.api.MetaClientInterface.EntryMode.*;

public class TestStressZkClient extends ZkMetaClientTestBase {

  private ZkMetaClient<String> _zkMetaClient;
  private static final long TEST_ITERATION_COUNT = 500;

  @BeforeTest
  private void setUp() {
    this._zkMetaClient = createZkMetaClient();
    this._zkMetaClient.connect();
  }

  @AfterTest
  @Override
  public void cleanUp() {
    this._zkMetaClient.close();
  }

  @Test
  public void testCreate() {
    String zkParentKey = "/stressZk_testCreate";

    _zkMetaClient.create(zkParentKey, "parent_node");

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.create(zkParentKey + "/" + i, i);
    }

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      Assert.assertEquals(String.valueOf(_zkMetaClient.get(zkParentKey + "/" + i)), String.valueOf(i));
    }

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      try {
        _zkMetaClient.create("/a/b/c", "invalid_path");
        Assert.fail("Should have failed with incorrect path.");
      } catch (MetaClientException ignoredException) {
      }
    }

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      try {
        _zkMetaClient.create("a/b/c", "invalid_path");
        Assert.fail("Should have failed with invalid path - no leading /.");
      } catch (Exception ignoredException) {
      }

      // cleanup
      _zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testCreateContainer() {
    final String zkParentKey = "/stressZk_testCreateContainer";
    _zkMetaClient.create(zkParentKey, "parent_node");

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.create(zkParentKey + "/" + i, i, CONTAINER);
    }

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      Assert.assertEquals(String.valueOf(_zkMetaClient.get(zkParentKey + "/" + i)), String.valueOf(i));
    }

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testCreateAndRenewTTL() {
    final String zkParentKey = "/stressZk_testCreateAndRenewTTL";
    _zkMetaClient.create(zkParentKey, ENTRY_STRING_VALUE);
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.createWithTTL(zkParentKey + i, ENTRY_STRING_VALUE, 10000);
    }
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      Assert.assertNotNull(_zkMetaClient.exists(zkParentKey));
    }
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.renewTTLNode(zkParentKey + i);
    }
    MetaClientInterface.Stat stat;
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      stat = _zkMetaClient.exists(zkParentKey + i);
      Assert.assertNotSame(stat.getCreationTime(), stat.getModifiedTime());
    }
    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testGet() {
    final String zkParentKey = "/stressZk_testGet";
    _zkMetaClient.create(zkParentKey, "parent_node");

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.create(zkParentKey + "/" + i, i);
    }

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      Assert.assertEquals(_zkMetaClient.get(zkParentKey + "/" + i), i);
    }

    // Test with non-existent node path
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      Assert.assertNull(_zkMetaClient.get("/a/b/c"));
    }

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      try {
        _zkMetaClient.get("a/b/c");
        Assert.fail("Should have failed due to invalid path - no leading /.");
      } catch (Exception ignoredException) {
    }

      // cleanup
      _zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testSetSingleNode() {
    final String zkParentKey = "/stressZk_testSetSingleNode";
    _zkMetaClient.create(zkParentKey, "parent_node");

    // Set with no expected version
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.set(zkParentKey, String.valueOf(i), -1);
      MetaClientInterface.Stat entryStat = _zkMetaClient.exists(zkParentKey);

      Assert.assertEquals(_zkMetaClient.get(zkParentKey), String.valueOf(i));
      Assert.assertEquals(entryStat.getVersion(), i + 1);
      Assert.assertEquals(entryStat.getEntryType().name(), PERSISTENT.name());
    }

    _zkMetaClient.delete(zkParentKey);
    _zkMetaClient.create(zkParentKey, "parent_node");

    // Set with expected version
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.set(zkParentKey, String.valueOf(i), i);
      MetaClientInterface.Stat entryStat = _zkMetaClient.exists(zkParentKey);

      Assert.assertEquals(_zkMetaClient.get(zkParentKey), String.valueOf(i));
      Assert.assertEquals(entryStat.getVersion(), i + 1);
    }

    _zkMetaClient.delete(zkParentKey);
    _zkMetaClient.create(zkParentKey, "parent_node");

    // Set with bad expected version - should fail
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      try {
        _zkMetaClient.set(zkParentKey, "test-should-fail", 12345);
        Assert.fail("Should have failed due to bad expected version in set function.");
      } catch (MetaClientException ex) {
        Assert.assertEquals(ex.getClass().getName(),
            "org.apache.helix.metaclient.exception.MetaClientBadVersionException");
      }
    }
      _zkMetaClient.delete(zkParentKey);
      _zkMetaClient.create(zkParentKey, "parent_node");

      // Set with path to non-existent node - should fail
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        try {
          _zkMetaClient.set("/a/b/c", "test-should-fail", -1);
          Assert.fail("Should have failed due to path to non-existent node");
        } catch (Exception ignoredException) {
        }
      }

      _zkMetaClient.delete(zkParentKey);
      _zkMetaClient.create(zkParentKey, "parent_node");

      // Set with invalid path - should fail
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        try {
          _zkMetaClient.set("a/b/c", "test-should-fail", -1);
          Assert.fail("Should have failed due to invalid path - no leading /.");
        } catch (Exception ignoredException) {
        }
      }

      // cleanup
      _zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
    }

  @Test
  public void testSetMultiNode() {
    final String zkParentKey = "/stressZk_testSetMultiNode";
    _zkMetaClient.create(zkParentKey, "parent_node");

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.create(zkParentKey + "/" + i, i);
    }

    // Set with no expected version
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.set(zkParentKey + "/" + i, "-" + i, -1);
    }

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      String childKey = zkParentKey + "/" + i;
      MetaClientInterface.Stat entryStat = _zkMetaClient.exists(childKey);

      Assert.assertEquals(_zkMetaClient.get(childKey), "-" + i);
      Assert.assertEquals(entryStat.getVersion(), 1);
      Assert.assertEquals(entryStat.getEntryType().name(), PERSISTENT.name());
    }

    // Set with expected version
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      String childKey = zkParentKey + "/" + i;
      _zkMetaClient.set(childKey, String.valueOf(i), 1);
      MetaClientInterface.Stat entryStat = _zkMetaClient.exists(childKey);

      Assert.assertEquals(_zkMetaClient.get(childKey), String.valueOf(i));
      Assert.assertEquals(entryStat.getVersion(), 2);
    }

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testUpdateSingleNode() {
    final String zkParentKey = "/stressZk_testUpdateSingleNode";
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR)
            .setZkSerializer(new DataRecordSerializer()).build();
    try (ZkMetaClient<DataRecord> zkMetaClient = new ZkMetaClient<>(config)) {
      zkMetaClient.connect();
      DataRecord record = new DataRecord(zkParentKey);

      record.setIntField("key", 0);
      zkMetaClient.create(zkParentKey, record);

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        MetaClientInterface.Stat entryStat = zkMetaClient.exists(zkParentKey);
        Assert.assertEquals(entryStat.getVersion(), i);

        DataRecord newData = zkMetaClient.update(zkParentKey, new DataUpdater<DataRecord>() {
          @Override
          public DataRecord update(DataRecord currentData) {
            int value = currentData.getIntField("key", 0);
            currentData.setIntField("key", value + 1);
            return currentData;
          }
        });
        Assert.assertEquals(newData.getIntField("key", 0), i+1);
      }

      // cleanup
      zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testUpdateMultiNode() {
    final String zkParentKey = "/stressZk_testUpdateMultiNode";
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR)
            .setZkSerializer(new DataRecordSerializer()).build();
    try (ZkMetaClient<DataRecord> zkMetaClient = new ZkMetaClient<>(config)) {
      zkMetaClient.connect();
      DataRecord parentRecord = new DataRecord(zkParentKey);
      zkMetaClient.create(zkParentKey, parentRecord);

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        String childKey = zkParentKey + "/" + i;
        DataRecord record = new DataRecord(childKey);
        record.setIntField("key", 0);
        zkMetaClient.create(childKey, record);
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        String childKey = zkParentKey + "/" + i;
        MetaClientInterface.Stat entryStat = zkMetaClient.exists(childKey);
        Assert.assertEquals(entryStat.getVersion(), 0);

        DataRecord newData = zkMetaClient.update(childKey, new DataUpdater<DataRecord>() {
          @Override
          public DataRecord update(DataRecord currentData) {
            int value = currentData.getIntField("key", 0);
            currentData.setIntField("key", value + 1);
            return currentData;
          }
        });

        Assert.assertEquals(newData.getIntField("key",0), 1);
        entryStat = zkMetaClient.exists(childKey);
        Assert.assertEquals(entryStat.getVersion(), 1);
      }

      // cleanup
      zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testMultipleDataChangeListeners() throws Exception {
    final String zkParentKey = "/stressZk_testMultipleDataChangeListeners";
    final int listenerCount = 10;
    final String testData = "test-data";
    final AtomicBoolean dataExpected = new AtomicBoolean(true);

    _zkMetaClient.create(zkParentKey, "parent_node");
    CountDownLatch countDownLatch = new CountDownLatch((int) TEST_ITERATION_COUNT * listenerCount);

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.create(zkParentKey + "/" + i, i);
    }

    // create paths
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      String childKey = zkParentKey + "/" + i;
      for (int j = 0; j < listenerCount; j++) {
        DataChangeListener listener = new DataChangeListener() {
          @Override
          public void handleDataChange(String key, Object data, ChangeType changeType) {
            countDownLatch.countDown();
            dataExpected.set(dataExpected.get() && testData.equals(data));
          }
        };
        _zkMetaClient.subscribeDataChange(childKey, listener, false);
      }
    }

    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      _zkMetaClient.set(zkParentKey + "/" + i, testData, -1);
    }

    Assert.assertTrue(countDownLatch.await(5000, TimeUnit.MILLISECONDS));
    Assert.assertTrue(dataExpected.get());

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testTransactionOps() {
    String zkParentKey = "/stressZk_testTransactionOp";
    _zkMetaClient.create(zkParentKey, "parent_node");

    // Transaction Create
    List<Op> ops = new ArrayList<>();
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      ops.add(Op.create(zkParentKey + "/" + i, new byte[0], PERSISTENT));
    }
    List<OpResult> opResults = _zkMetaClient.transactionOP(ops);
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      Assert.assertTrue(opResults.get(i) instanceof OpResult.CreateResult);
    }
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      Assert.assertNotNull(_zkMetaClient.exists(zkParentKey + "/" + i));
    }

    // Transaction Set
    List<Op> ops_set = new ArrayList<>();
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      ops_set.add(Op.set(zkParentKey + "/" + i, new byte[0], -1));
    }
    List<OpResult> opsResultSet = _zkMetaClient.transactionOP(ops_set);
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      Assert.assertTrue(opsResultSet.get(i) instanceof OpResult.SetDataResult);
    }

    // Transaction Delete
    List<Op> ops_delete = new ArrayList<>();
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      ops_delete.add(Op.delete(zkParentKey + "/" + i, -1));
    }
    List<OpResult> opsResultDelete = _zkMetaClient.transactionOP(ops_delete);
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      Assert.assertTrue(opsResultDelete.get(i) instanceof OpResult.DeleteResult);
    }

    // Transaction Create
    List<Op> ops_error = new ArrayList<>();
    for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
      ops_error.add(Op.create("/_invalid/a/b/c" + "/" + i, new byte[0], PERSISTENT));
    }
    try {
      _zkMetaClient.transactionOP(ops_error);
      Assert.fail("Should fail");
    } catch (Exception e) {
      // OK
    }


    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
}