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
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZKWatch extends ZkTestBase {
  private ZkClient _zkClient;

  @BeforeClass
  public void beforeClass() {
    _zkClient = new ZkClient(ZK_ADDR);
  }

  @AfterClass
  public void afterClass() {
    _zkClient.close();
  }

  @Test
  public void testSubscribeDataChange() throws Exception {
    String existPath = "/existPath";
    _zkClient.createPersistent(existPath);
    final CountDownLatch deleteCondition = new CountDownLatch(1);
    final IZkDataListener dataListener = new IZkDataListener() {
      @Override
      public void handleDataChange(String s, Object o) throws Exception {

      }

      @Override
      public void handleDataDeleted(String path) throws Exception {
        _zkClient.unsubscribeDataChanges(path, this);
        deleteCondition.countDown();
      }
    };
    _zkClient.subscribeDataChanges(existPath, dataListener);

    Assert.assertEquals(_zkClient.numberOfListeners(), 1);
    Map<String, List<String>> zkWatch = ZkTestHelper.getZkWatch(_zkClient);
    Assert.assertEquals(zkWatch.get("dataWatches").size(), 1);
    Assert.assertEquals(zkWatch.get("existWatches").size(), 0);
    Assert.assertEquals(zkWatch.get("childWatches").size(), 0);
    // remove the zk node, the NodeDeleted event will be processed
    _zkClient.delete(existPath);
    deleteCondition.await();
    zkWatch = ZkTestHelper.getZkWatch(_zkClient);
    Assert.assertEquals(zkWatch.get("dataWatches").size(), 0);
    Assert.assertEquals(zkWatch.get("existWatches").size(), 0);
    Assert.assertEquals(zkWatch.get("childWatches").size(), 0);

    Assert.assertEquals(_zkClient.numberOfListeners(), 0);
  }

  @Test(dependsOnMethods = "testSubscribeDataChange")
  public void testSubscribeChildChange() throws Exception {
    String parentPath = "/tmp";
    String childPath = parentPath + "/childNode";
    _zkClient.createPersistent(childPath, true);
    final CountDownLatch deleteCondition = new CountDownLatch(1);

    IZkChildListener childListener = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> childrenPaths) throws Exception {
        _zkClient.unsubscribeChildChanges(parentPath, this);
        deleteCondition.countDown();
      }
    };
    _zkClient.subscribeChildChanges(parentPath, childListener);
    Map<String, List<String>> zkWatch = ZkTestHelper.getZkWatch(_zkClient);
    Assert.assertEquals(zkWatch.get("dataWatches").size(), 1);
    Assert.assertEquals(zkWatch.get("dataWatches").get(0), parentPath);
    Assert.assertEquals(zkWatch.get("existWatches").size(), 0);
    Assert.assertEquals(zkWatch.get("childWatches").size(), 1);
    Assert.assertEquals(zkWatch.get("childWatches").get(0), parentPath);

    // Delete the child node
    _zkClient.delete(childPath);

    deleteCondition.await();
    zkWatch = ZkTestHelper.getZkWatch(_zkClient);
    // Expectation: the child listener should still exist
    Assert.assertEquals(zkWatch.get("dataWatches").size(), 1);
    Assert.assertEquals(zkWatch.get("dataWatches").get(0), parentPath);
    Assert.assertEquals(zkWatch.get("existWatches").size(), 0);
    Assert.assertEquals(zkWatch.get("childWatches").size(), 1);
    Assert.assertEquals(zkWatch.get("childWatches").get(0), parentPath);
    Assert.assertEquals(_zkClient.numberOfListeners(), 0);

    // delete the parent path
    _zkClient.delete(parentPath);
    zkWatch = ZkTestHelper.getZkWatch(_zkClient);
    Assert.assertEquals(zkWatch.get("dataWatches").size(), 0);
    Assert.assertEquals(zkWatch.get("existWatches").size(), 0);
    Assert.assertEquals(zkWatch.get("childWatches").size(), 0);
  }

  @Test(dependsOnMethods = "testSubscribeChildChange")
  public void testSubscribeDataChangeOnNonExistPath() throws Exception {
    String nonExistPath = "/nonExistPath";
    IZkDataListener dataListener = new IZkDataListener() {
      @Override
      public void handleDataChange(String s, Object o) throws Exception {

      }

      @Override
      public void handleDataDeleted(String s) throws Exception {

      }
    };
    _zkClient.subscribeDataChanges(nonExistPath, dataListener);
    Map<String, List<String>> zkWatch = ZkTestHelper.getZkWatch(_zkClient);
    Assert.assertEquals(zkWatch.get("dataWatches").size(), 0);
    Assert.assertEquals(zkWatch.get("existWatches").size(), 1);
    Assert.assertEquals(zkWatch.get("childWatches").size(), 0);
    // cleanup (unsubscribe will not clean up the watcher on ZK server
    _zkClient.unsubscribeDataChanges(nonExistPath, dataListener);
    zkWatch = ZkTestHelper.getZkWatch(_zkClient);
    Assert.assertEquals(zkWatch.get("dataWatches").size(), 0);
    Assert.assertEquals(zkWatch.get("existWatches").size(), 1);
    Assert.assertEquals(zkWatch.get("childWatches").size(), 0);
  }
}
