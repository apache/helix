package org.apache.helix.zookeeper.zkclient;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkClientPersistWatcher extends ZkTestBase {

  @Test
  void testZkClientDataChange() throws Exception {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(true);
    org.apache.helix.zookeeper.impl.client.ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    int count = 1000;
    final int[] event_count = {0};
    CountDownLatch countDownLatch1 = new CountDownLatch(count);
    String path = "/dataChangeTestPath";
    IZkDataListener dataListener = new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception {
        countDownLatch1.countDown();
        event_count[0]++;
      }

      @Override
      public void handleDataDeleted(String dataPath) throws Exception {
      }
    };

    zkClient.subscribeDataChanges(path, dataListener);
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    for (int i = 0; i < count; ++i) {
      zkClient.writeData(path, ("datat" + i), -1);
    }

    Assert.assertTrue(countDownLatch1.await(15000, TimeUnit.MILLISECONDS));
    zkClient.close();
  }

  @Test(dependsOnMethods = "testZkClientDataChange")
  void testZkClientChildChange() throws Exception {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(true);
    org.apache.helix.zookeeper.impl.client.ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    int count = 100;
    final int[] event_count = {0};
    CountDownLatch countDownLatch1 = new CountDownLatch(count);
    CountDownLatch countDownLatch2 = new CountDownLatch(count);
    String path = "/testZkClientChildChange";
    IZkChildListener childListener = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        countDownLatch1.countDown();
        event_count[0]++;
      }
    };
    IZkChildListener childListener2 = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        countDownLatch2.countDown();
        event_count[0]++;
      }
    };
    zkClient.subscribeChildChanges(path, childListener);
    zkClient.subscribeChildChanges(path, childListener2);
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    for (int i = 0; i < count; ++i) {
      zkClient.create(path + "/child" + i, "datat", CreateMode.PERSISTENT);
    }
    Assert.assertTrue(countDownLatch1.await(15000, TimeUnit.MILLISECONDS));
    Assert.assertTrue(countDownLatch2.await(15000, TimeUnit.MILLISECONDS));
    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  @Test(dependsOnMethods = "testZkClientChildChange")
  void testZkClientPersistRecursiveChange() throws Exception {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(true);
    org.apache.helix.zookeeper.impl.client.ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    int count = 100;
    final AtomicInteger[] event_count = {new AtomicInteger(0)};
    final AtomicInteger[] event_count2 = {new AtomicInteger(0)};
    // for each iteration, we will edit a node, create a child, create a grand child, and
    // delete child. Expect 4 event per iteration. -> total event should be count*4
    CountDownLatch countDownLatch1 = new CountDownLatch(count * 4);
    CountDownLatch countDownLatch2 = new CountDownLatch(count);
    String path = "/testZkClientPersistRecursiveChange";
    RecursivePersistListener rcListener = new RecursivePersistListener() {
      @Override
      public void handleZNodeChange(String dataPath, Watcher.Event.EventType eventType) throws Exception {
        countDownLatch1.countDown();
        event_count[0].incrementAndGet();
      }
    };
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    zkClient.subscribePersistRecursiveListener(path, rcListener);
    for (int i = 0; i < count; ++i) {
      zkClient.writeData(path, "data7" + i, -1);
      zkClient.create(path + "/c1_" + i, "datat", CreateMode.PERSISTENT);
      zkClient.create(path + "/c1_" + i + "/c2", "datat", CreateMode.PERSISTENT);
      zkClient.delete(path + "/c1_" + i + "/c2");
    }
    Assert.assertTrue(countDownLatch1.await(50000000, TimeUnit.MILLISECONDS));

    // subscribe a persist child watch, it should throw exception
    IZkChildListener childListener2 = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        countDownLatch2.countDown();
        event_count2[0].incrementAndGet();
      }
    };
    try {
      zkClient.subscribeChildChanges(path, childListener2, false);
    } catch (Exception ex) {
      Assert.assertEquals(ex.getClass().getName(), "java.lang.UnsupportedOperationException");
    }

    // unsubscribe recursive persist watcher, and subscribe persist watcher should success.
    zkClient.unsubscribePersistRecursiveListener(path, rcListener);
    zkClient.subscribeChildChanges(path, childListener2, false);
    // we should only get 100 event since only 100 direct child change.
    for (int i = 0; i < count; ++i) {
      zkClient.writeData(path, "data7" + i, -1);
      zkClient.create(path + "/c2_" + i, "datat", CreateMode.PERSISTENT);
      zkClient.create(path + "/c2_" + i + "/c3", "datat", CreateMode.PERSISTENT);
      zkClient.delete(path + "/c2_" + i + "/c3");
    }
    Assert.assertTrue(countDownLatch2.await(50000000, TimeUnit.MILLISECONDS));

    zkClient.deleteRecursively(path);
    zkClient.close();
  }

  @Test
  void testSubscribeOneTimeChangeWhenUsingPersistWatcher() {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(true);
    ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));

    String path = "/testSubscribeOneTimeChangeWhenUsingPersistWatcher";
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    try {
      zkClient.exists(path, true);
      Assert.fail("Should throw exception when subscribe one time listener");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass().getName(), "java.lang.IllegalArgumentException");
    }

    try {
      zkClient.readData(path, null, true);
      Assert.fail("Should throw exception when subscribe one time listener");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass().getName(), "java.lang.IllegalArgumentException");
    }

    try {
      zkClient.getChildren(path, true);
      Assert.fail("Should throw exception when subscribe one time listener");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass().getName(), "java.lang.IllegalArgumentException");
    }
    zkClient.delete(path);
    zkClient.close();
  }

  @Test
  void testCrudOperationWithResubscribe() {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false).setUsePersistWatcher(false);
    ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));

    String path = "/testCrudOperationWithResubscribe";
    zkClient.create(path, "datat", CreateMode.PERSISTENT);
    zkClient.exists(path, true);
    zkClient.readData(path, null, true);
    zkClient.getChildren(path, true);
    zkClient.delete(path);
    zkClient.close();
  }
}