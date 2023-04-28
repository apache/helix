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

import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkClientPersistWatcher extends ZkTestBase {

  @Test
  void testZkClientDataChange() throws Exception {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false)
        .setUsePersistWatcher(true);
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
    for(int i=0; i<count; ++i) {
      zkClient.writeData(path, ("datat"+i), -1);
    }

    Assert.assertTrue(countDownLatch1.await(15000, TimeUnit.MILLISECONDS));
    zkClient.close();
  }

  @Test(dependsOnMethods = "testZkClientDataChange")
  void testZkClientChildChange() throws Exception {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false)
        .setUsePersistWatcher(true);
    org.apache.helix.zookeeper.impl.client.ZkClient zkClient = builder.build();
    zkClient.setZkSerializer(new BasicZkSerializer(new SerializableSerializer()));
    int count = 100;
    final int[] event_count = {0};
    CountDownLatch countDownLatch1 = new CountDownLatch(count);
    CountDownLatch countDownLatch2 = new CountDownLatch(count);
    String path = "/testZkClientChildChange";
    IZkChildListener childListener = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds)
          throws Exception {
        countDownLatch1.countDown();
        event_count[0]++ ;
      }
    };
    IZkChildListener childListener2 = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds)
          throws Exception {
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
    zkClient.close();
  }

  @Test
  void testSubscribeOneTimeChangeWhenUsingPersistWatcher() {
    org.apache.helix.zookeeper.impl.client.ZkClient.Builder builder =
        new org.apache.helix.zookeeper.impl.client.ZkClient.Builder();
    builder.setZkServer(ZkTestBase.ZK_ADDR).setMonitorRootPathOnly(false)
        .setUsePersistWatcher(true);
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
      zkClient.readData(path, null,  true);
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
  }
}