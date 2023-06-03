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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.metaclient.api.DataUpdater;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.datamodel.DataRecord;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.testng.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import static org.apache.helix.metaclient.api.MetaClientInterface.EntryMode.*;

public class TestStressZkClient extends ZkMetaClientTestBase {

  private static final long TEST_ITERATION_COUNT = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(TestStressZkClient.class);


  @Test
  public void testCreate() {
    String zkParentKey = "/stressZk_testCreate";

    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(zkParentKey, "parent_node");

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.create(zkParentKey + "/" + i, i);
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        Assert.assertEquals(String.valueOf(zkMetaClient.get(zkParentKey + "/" + i)), String.valueOf(i));
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        try {
          zkMetaClient.create("/a/b/c", "invalid_path");
          Assert.fail("Should have failed with incorrect path.");
        } catch (MetaClientException ignoredException) {
        }
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        try {
          zkMetaClient.create("a/b/c", "invalid_path");
          Assert.fail("Should have failed with invalid path - no leading /.");
        } catch (Exception ignoredException) {
        }
      }

      // cleanup
      zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testCreateContainer() {
    final String zkParentKey = "/stressZk_testCreateContainer";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(zkParentKey, "parent_node");

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.create(zkParentKey + "/" + i, i, CONTAINER);
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        Assert.assertEquals(String.valueOf(zkMetaClient.get(zkParentKey + "/" + i)), String.valueOf(i));
      }

      // cleanup
      zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testGet() {
    final String zkParentKey = "/stressZk_testGet";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(zkParentKey, "parent_node");

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.create(zkParentKey + "/" + i, i);
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        Assert.assertEquals(zkMetaClient.get(zkParentKey + "/" + i), i);
      }

      // Test with non-existent node path
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        Assert.assertNull(zkMetaClient.get("/a/b/c"));
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        try {
          zkMetaClient.get("a/b/c");
          Assert.fail("Should have failed due to invalid path - no leading /.");
        } catch (Exception ignoredException) {
        }
      }

      // cleanup
      zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testSetSingleNode() {
    final String zkParentKey = "/stressZk_testSetSingleNode";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(zkParentKey, "parent_node");

      // Set with no expected version
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.set(zkParentKey, String.valueOf(i), -1);
        MetaClientInterface.Stat entryStat = zkMetaClient.exists(zkParentKey);

        Assert.assertEquals(zkMetaClient.get(zkParentKey), String.valueOf(i));
        Assert.assertEquals(entryStat.getVersion(), i + 1);
        Assert.assertEquals(entryStat.getEntryType().name(), PERSISTENT.name());
      }

      zkMetaClient.delete(zkParentKey);
      zkMetaClient.create(zkParentKey, "parent_node");

      // Set with expected version
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.set(zkParentKey, String.valueOf(i), i);
        MetaClientInterface.Stat entryStat = zkMetaClient.exists(zkParentKey);

        Assert.assertEquals(zkMetaClient.get(zkParentKey), String.valueOf(i));
        Assert.assertEquals(entryStat.getVersion(), i + 1);
      }

      zkMetaClient.delete(zkParentKey);
      zkMetaClient.create(zkParentKey, "parent_node");

      // Set with bad expected version - should fail
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        try {
          zkMetaClient.set(zkParentKey, "test-should-fail", 12345);
          Assert.fail("Should have failed due to bad expected version in set function.");
        } catch (MetaClientException ex) {
          Assert.assertEquals(ex.getClass().getName(),
              "org.apache.helix.metaclient.exception.MetaClientBadVersionException");
        }
      }

      zkMetaClient.delete(zkParentKey);
      zkMetaClient.create(zkParentKey, "parent_node");

      // Set with path to non-existent node - should fail
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        try {
          zkMetaClient.set("/a/b/c", "test-should-fail", -1);
          Assert.fail("Should have failed due to path to non-existent node");
        } catch (Exception ignoredException) {
        }
      }

      zkMetaClient.delete(zkParentKey);
      zkMetaClient.create(zkParentKey, "parent_node");

      // Set with invalid path - should fail
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        try {
          zkMetaClient.set("a/b/c", "test-should-fail", -1);
          Assert.fail("Should have failed due to invalid path - no leading /.");
        } catch (Exception ignoredException) {
        }
      }

      // cleanup
      zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testSetMultiNode() {
    final String zkParentKey = "/stressZk_testSetMultiNode";
    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(zkParentKey, "parent_node");

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.create(zkParentKey + "/" + i, i);
      }

      // Set with no expected version
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.set(zkParentKey + "/" + i, "-" + i, -1);
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        String childKey = zkParentKey + "/" + i;
        MetaClientInterface.Stat entryStat = zkMetaClient.exists(childKey);

        Assert.assertEquals(zkMetaClient.get(childKey), "-" + i);
        Assert.assertEquals(entryStat.getVersion(), 1);
        Assert.assertEquals(entryStat.getEntryType().name(), PERSISTENT.name());
      }

      // Set with expected version
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        String childKey = zkParentKey + "/" + i;
        zkMetaClient.set(childKey, String.valueOf(i), 1);
        MetaClientInterface.Stat entryStat = zkMetaClient.exists(childKey);

        Assert.assertEquals(zkMetaClient.get(childKey), String.valueOf(i));
        Assert.assertEquals(entryStat.getVersion(), 2);
      }

      // cleanup
      zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testUpdateSingleNode() {
    final String zkParentKey = "/stressZk_testUpdateSingleNode";
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR).build();
    try (ZkMetaClient<byte[]> zkMetaClient = new ZkMetaClient<>(config)) {
      zkMetaClient.connect();
      ZNRecord record = new ZNRecord("foo");
      record.setIntField("key", 0);
      ZNRecordSerializer serializer = new ZNRecordSerializer();
      zkMetaClient.create(zkParentKey, serializer.serialize(record));


      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        MetaClientInterface.Stat entryStat = zkMetaClient.exists(zkParentKey);
        Assert.assertEquals(entryStat.getVersion(), i);

        byte[] newData = zkMetaClient.update(zkParentKey, new DataUpdater<byte[]>() {
          @Override
          public byte[] update(byte[] currentData) {
            ZNRecordSerializer serializer = new ZNRecordSerializer();
            ZNRecord record = (ZNRecord) serializer.deserialize(currentData);
            int value = record.getIntField("key", 0);
            record.setIntField("key", value + 1);
            return serializer.serialize(record);
          }
        });
        ZNRecord newRecord = (ZNRecord) serializer.deserialize(newData);
        Assert.assertEquals(newRecord.getIntField("key", 0), i+1);
      }


//      zkMetaClient.delete(zkParentKey);
//      zkMetaClient.create(zkParentKey, "parent_node");

//       Set with path to non-existent node - should fail
//      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
//        try {
//          zkMetaClient.update("/a/b/c", new DataUpdater<Integer>() {
//            @Override
//            public Integer update(Integer currentData) {
//              return currentData + 1;
//            }
//          });
//
//          Assert.fail("Should have failed due to path to non-existent node");
//        } catch (Exception ignoredException) {
//        }
//      }

//      zkMetaClient.delete(zkParentKey);
//      zkMetaClient.create(zkParentKey, "parent_node");

      // Set with invalid path - should fail
//      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
//        try {
//          zkMetaClient.update("/a/b/c", new DataUpdater<Integer>() {
//            @Override
//            public Integer update(Integer currentData) {
//              return currentData + 1;
//            }
//          });
//          Assert.fail("Should have failed due to invalid path - no leading /.");
//        } catch (Exception ignoredException) {
//        }
//      }

      // cleanup
      zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }

  @Test
  public void testUpdateMultiNode() {
    final String zkParentKey = "/stressZk_testUpdateSingleNode";
    ZkMetaClientConfig config =
        new ZkMetaClientConfig.ZkMetaClientConfigBuilder().setConnectionAddress(ZK_ADDR).build();
    try (ZkMetaClient<Integer> zkMetaClient = new ZkMetaClient<>(config)) {
      zkMetaClient.connect();
      zkMetaClient.create(zkParentKey, "test_parent");

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.create(zkParentKey + "/" + i, 0);
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        String childKey = zkParentKey + "/" + i;
        MetaClientInterface.Stat entryStat = zkMetaClient.exists(childKey);
        Assert.assertEquals(entryStat.getVersion(), 0);

        Integer newData = zkMetaClient.update(childKey, new DataUpdater<Integer>() {
          @Override
          public Integer update(Integer currentData) {
            return currentData + 1;
          }
        });

        Assert.assertEquals((int) newData, 1);
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

    try (ZkMetaClient<String> zkMetaClient = createZkMetaClient()) {
      zkMetaClient.connect();
      zkMetaClient.create(zkParentKey, "parent_node");
      Map<String, Set<DataChangeListener>> listeners = new HashMap<>();
      CountDownLatch countDownLatch = new CountDownLatch((int) TEST_ITERATION_COUNT * listenerCount);

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.create(zkParentKey + "/" + i, i);
      }

      // create paths
      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        String childKey = zkParentKey + "/" + i;
        listeners.put(childKey, new HashSet<>());
        for (int j = 0; j < listenerCount; j++) {
          DataChangeListener listener = new DataChangeListener() {
            @Override
            public void handleDataChange(String key, Object data, ChangeType changeType) {
              countDownLatch.countDown();
              dataExpected.set(dataExpected.get() && testData.equals(data));
            }
          };
          listeners.get(childKey).add(listener);
          zkMetaClient.subscribeDataChange(childKey, listener, false);
        }
      }

      for (int i = 0; i < TEST_ITERATION_COUNT; i++) {
        zkMetaClient.set(zkParentKey + "/" + i, testData, -1);
      }

      Assert.assertTrue(countDownLatch.await(5000, TimeUnit.MILLISECONDS));
      Assert.assertTrue(dataExpected.get());

      // cleanup
      zkMetaClient.recursiveDelete(zkParentKey);
      Assert.assertEquals(zkMetaClient.countDirectChildren(zkParentKey), 0);
    }
  }
}