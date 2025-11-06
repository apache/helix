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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.ZNRecordUpdater;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.helix.store.HelixPropertyListener;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkCacheSyncOpSingleThread extends ZkUnitTestBase {
  class TestListener implements HelixPropertyListener {
    ConcurrentLinkedQueue<String> _deletePathQueue = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<String> _createPathQueue = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<String> _changePathQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void onDataDelete(String path) {
      // System.out.println(Thread.currentThread().getName() + ", onDelete: " + path);
      _deletePathQueue.add(path);
    }

    @Override
    public void onDataCreate(String path) {
      // System.out.println(Thread.currentThread().getName() + ", onCreate: " + path);
      _createPathQueue.add(path);
    }

    @Override
    public void onDataChange(String path) {
      // System.out.println(Thread.currentThread().getName() + ", onChange: " + path);
      _changePathQueue.add(path);
    }

    public void reset() {
      _deletePathQueue.clear();
      _createPathQueue.clear();
      _changePathQueue.clear();
    }
  }

  @Test
  public void testZkCacheCallbackExternalOpNoChroot() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // init external base data accessor
    HelixZkClient zkclient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR));
    zkclient.setZkSerializer(new ZNRecordSerializer());
    ZkBaseDataAccessor<ZNRecord> extBaseAccessor = new ZkBaseDataAccessor<>(zkclient);

    // init zkCacheDataAccessor
    String curStatePath = PropertyPathBuilder.instanceCurrentState(clusterName, "localhost_8901");
    String extViewPath = PropertyPathBuilder.externalView(clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);

    extBaseAccessor.create(curStatePath, null, AccessOption.PERSISTENT);

    List<String> cachePaths = Arrays.asList(curStatePath, extViewPath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<>(baseAccessor, null, null, cachePaths);

    TestListener listener = new TestListener();
    accessor.subscribe(curStatePath, listener);

    // create 10 current states
    List<String> createPaths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      createPaths.add(path);
      boolean success =
          extBaseAccessor.create(path, new ZNRecord("TestDB" + i), AccessOption.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in create: " + path);
    }

    Thread.sleep(500);

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    boolean ret = TestHelper.verifyZkCache(cachePaths, accessor._zkCache._cache, _gZkClient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");
    System.out.println("createCnt: " + listener._createPathQueue.size());
    Assert.assertEquals(listener._createPathQueue.size(), 11, "Shall get 11 onCreate callbacks.");

    // verify each callback path
    createPaths.add(curStatePath + "/session_0");
    List<String> createCallbackPaths = new ArrayList<>(listener._createPathQueue);
    Collections.sort(createPaths);
    Collections.sort(createCallbackPaths);
    // System.out.println("createCallbackPaths: " + createCallbackPaths);
    Assert.assertEquals(createCallbackPaths, createPaths,
        "Should get create callbacks at " + createPaths + ", but was " + createCallbackPaths);

    // update each current state, single thread
    List<String> updatePaths = new ArrayList<>();
    listener.reset();
    for (int i = 0; i < 10; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      for (int j = 0; j < 10; j++) {
        updatePaths.add(path);
        ZNRecord newRecord = new ZNRecord("TestDB" + i);
        newRecord.setSimpleField("" + j, "" + j);
        boolean success =
            accessor.update(path, new ZNRecordUpdater(newRecord), AccessOption.PERSISTENT);
        Assert.assertTrue(success, "Should succeed in update: " + path);
      }
    }
    Thread.sleep(500);

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._zkCache._cache, _gZkClient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");
    System.out.println("changeCnt: " + listener._changePathQueue.size());
    Assert.assertEquals(listener._changePathQueue.size(), 100, "Shall get 100 onChange callbacks.");

    // verify each callback path
    List<String> updateCallbackPaths = new ArrayList<>(listener._changePathQueue);
    Collections.sort(updatePaths);
    Collections.sort(updateCallbackPaths);
    Assert.assertEquals(updateCallbackPaths, updatePaths,
        "Should get change callbacks at " + updatePaths + ", but was " + updateCallbackPaths);

    // remove 10 current states
    TreeSet<String> removePaths = new TreeSet<>();
    listener.reset();
    for (int i = 0; i < 10; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      removePaths.add(path);
      boolean success = accessor.remove(path, AccessOption.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in remove: " + path);
    }
    Thread.sleep(500);

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._zkCache._cache, _gZkClient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");
    System.out.println("deleteCnt: " + listener._deletePathQueue.size());
    Assert.assertTrue(listener._deletePathQueue.size() >= 10,
        "Shall get at least 10 onDelete callbacks.");

    // verify each callback path
    Set<String> removeCallbackPaths = new TreeSet<>(listener._deletePathQueue);
    Assert.assertEquals(removeCallbackPaths, removePaths,
        "Should get remove callbacks at " + removePaths + ", but was " + removeCallbackPaths);

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testGetStatsWithCache() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    String curStatePath = PropertyPathBuilder.instanceCurrentState(clusterName, "localhost_8901");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);

    List<String> cachePaths = Arrays.asList(curStatePath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<>(baseAccessor, null, null, cachePaths);

    // Create 5 current state nodes
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      paths.add(path);
      boolean success = accessor.create(path, new ZNRecord("TestDB" + i), AccessOption.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in create: " + path);
    }

    // First call - populates cache
    org.apache.zookeeper.data.Stat[] stats = accessor.getStats(paths, 0);
    Assert.assertNotNull(stats);
    Assert.assertEquals(stats.length, 5);
    for (int i = 0; i < 5; i++) {
      Assert.assertNotNull(stats[i], "Stat should exist for TestDB" + i);
      Assert.assertEquals(stats[i].getVersion(), 0);
    }

    // Verify cache is populated after first getStats call
    for (int i = 0; i < 5; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      Assert.assertNotNull(accessor._zkCache._cache.get(path), 
          "Cache should contain TestDB" + i + " after first getStats call");
    }

    // Second call - should hit cache
    org.apache.zookeeper.data.Stat[] cachedStats = accessor.getStats(paths, 0);
    Assert.assertEquals(cachedStats.length, 5);
    for (int i = 0; i < 5; i++) {
      Assert.assertNotNull(cachedStats[i]);
      Assert.assertEquals(cachedStats[i].getVersion(), 0);
    }

    // Verify data still in cache and matches
    for (int i = 0; i < 5; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      Assert.assertNotNull(accessor._zkCache._cache.get(path), 
          "Cache should still contain TestDB" + i);
      // Verify we got the same stat from cache (version should match)
      Assert.assertEquals(
          accessor._zkCache._cache.get(path).getStat().getVersion(), 
          cachedStats[i].getVersion(),
          "Cached stat version should match returned stat for TestDB" + i);
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testGetStatsAfterUpdate() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    String curStatePath = PropertyPathBuilder.instanceCurrentState(clusterName, "localhost_8901");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);

    List<String> cachePaths = Arrays.asList(curStatePath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<>(baseAccessor, null, null, cachePaths);

    // Create nodes
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      paths.add(path);
      accessor.create(path, new ZNRecord("TestDB" + i), AccessOption.PERSISTENT);
    }

    // Get initial stats - populates cache
    org.apache.zookeeper.data.Stat[] initialStats = accessor.getStats(paths, 0);
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(initialStats[i].getVersion(), 0);
    }

    // Verify cache is populated with version 0
    for (int i = 0; i < 3; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      Assert.assertNotNull(accessor._zkCache._cache.get(path), 
          "Cache should contain TestDB" + i);
      Assert.assertEquals(
          accessor._zkCache._cache.get(path).getStat().getVersion(), 0,
          "Cached version should be 0 for TestDB" + i);
    }

    // Update nodes
    for (int i = 0; i < 3; i++) {
      ZNRecord updatedRecord = new ZNRecord("TestDB" + i);
      updatedRecord.setSimpleField("key", "value");
      accessor.set(paths.get(i), updatedRecord, AccessOption.PERSISTENT);
    }

    // Get stats after update - should reflect new version from updated cache
    org.apache.zookeeper.data.Stat[] updatedStats = accessor.getStats(paths, 0);
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(updatedStats[i].getVersion(), 1, "Version should be 1 after update");
    }

    // Verify cache is updated with new version
    for (int i = 0; i < 3; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      Assert.assertNotNull(accessor._zkCache._cache.get(path), 
          "Cache should still contain TestDB" + i);
      Assert.assertEquals(
          accessor._zkCache._cache.get(path).getStat().getVersion(), 1,
          "Cached version should be updated to 1 for TestDB" + i);
      // Verify the cached stat matches what getStats returned
      Assert.assertEquals(
          accessor._zkCache._cache.get(path).getStat().getVersion(),
          updatedStats[i].getVersion(),
          "Cached stat should match returned stat for TestDB" + i);
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
