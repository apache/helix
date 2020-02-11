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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.ZNRecordUpdater;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestWtCacheAsyncOpSingleThread extends ZkUnitTestBase {
  @Test
  public void testHappyPathZkCacheBaseDataAccessor() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // init zkCacheDataAccessor
    String curStatePath = PropertyPathBuilder.instanceCurrentState(clusterName, "localhost_8901");
    String extViewPath = PropertyPathBuilder.externalView(clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);

    baseAccessor.create(curStatePath, null, AccessOption.PERSISTENT);

    List<String> cachePaths = Arrays.asList(curStatePath, extViewPath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<>(baseAccessor, null, cachePaths, null);

    boolean ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // create 10 current states
    List<String> paths = new ArrayList<>();
    List<ZNRecord> records = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String path = PropertyPathBuilder.instanceCurrentState(clusterName, "localhost_8901",
          "session_0", "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }

    boolean[] success = accessor.createChildren(paths, records, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(success[i], "Should succeed in create: " + paths.get(i));
    }

    // verify wtCache
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // update each current state 10 times
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<>();
    for (int j = 0; j < 10; j++) {
      paths.clear();
      updaters.clear();
      for (int i = 0; i < 10; i++) {
        String path = curStatePath + "/session_0/TestDB" + i;
        ZNRecord newRecord = new ZNRecord("TestDB" + i);
        newRecord.setSimpleField("" + j, "" + j);
        DataUpdater<ZNRecord> updater = new ZNRecordUpdater(newRecord);
        paths.add(path);
        updaters.add(updater);
      }
      success = accessor.updateChildren(paths, updaters, AccessOption.PERSISTENT);

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(success[i], "Should succeed in update: " + paths.get(i));
      }
    }

    // verify cache
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // set 10 external views
    paths.clear();
    records.clear();
    for (int i = 0; i < 10; i++) {
      String path = PropertyPathBuilder.externalView(clusterName, "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }
    success = accessor.setChildren(paths, records, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(success[i], "Should succeed in set: " + paths.get(i));
    }

    // verify wtCache
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // get 10 external views
    paths.clear();
    records.clear();
    for (int i = 0; i < 10; i++) {
      String path = PropertyPathBuilder.externalView(clusterName, "TestDB" + i);
      paths.add(path);
    }

    records = accessor.get(paths, null, 0);
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(records.get(i).getId(), "TestDB" + i);
    }

    // getChildren
    records.clear();
    records = accessor.getChildren(extViewPath, null, 0);
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(records.get(i).getId(), "TestDB" + i);
    }

    // exists
    paths.clear();
    for (int i = 0; i < 10; i++) {
      String path = PropertyPathBuilder.instanceCurrentState(clusterName, "localhost_8901",
          "session_0", "TestDB" + i);
      paths.add(path);
    }
    success = accessor.exists(paths, 0);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(success[i], "Should exits: TestDB" + i);
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCreateFailZkCacheBaseAccessor() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // init zkCacheDataAccessor
    String curStatePath = PropertyPathBuilder.instanceCurrentState(clusterName, "localhost_8901");
    String extViewPath = PropertyPathBuilder.externalView(clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);

    baseAccessor.create(curStatePath, null, AccessOption.PERSISTENT);

    ZkCacheBaseDataAccessor<ZNRecord> accessor = new ZkCacheBaseDataAccessor<>(baseAccessor, null,
        Arrays.asList(curStatePath, extViewPath), null);

    Assert.assertEquals(accessor._wtCache._cache.size(), 1,
        "Should contain only:\n" + curStatePath);
    Assert.assertTrue(accessor._wtCache._cache.containsKey(curStatePath));

    // create 10 current states
    List<String> paths = new ArrayList<>();
    List<ZNRecord> records = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String path = PropertyPathBuilder.instanceCurrentState(clusterName, "localhost_8901",
          "session_1", "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }

    boolean[] success = accessor.createChildren(paths, records, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(success[i], "Should succeed in create: " + paths.get(i));
    }

    // create same 10 current states again, should fail on NodeExists
    success = accessor.createChildren(paths, records, AccessOption.PERSISTENT);
    // System.out.println(Arrays.toString(success));
    for (int i = 0; i < 10; i++) {
      Assert.assertFalse(success[i], "Should fail on create: " + paths.get(i));
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
