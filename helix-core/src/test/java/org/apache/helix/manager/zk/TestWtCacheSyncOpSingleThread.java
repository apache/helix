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

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZNRecordUpdater;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestWtCacheSyncOpSingleThread extends ZkTestBase {
  // TODO: add TestZkCacheSyncOpSingleThread
  // TODO: add TestZkCacheAsyncOpMultiThread
  @Test
  public void testHappyPathZkCacheBaseDataAccessor() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // init zkCacheDataAccessor
    String curStatePath =
        PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName, "localhost_8901");
    String extViewPath = PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    baseAccessor.create(curStatePath, null, AccessOption.PERSISTENT);

    List<String> cachePaths = Arrays.asList(curStatePath, extViewPath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<ZNRecord>(baseAccessor, null, cachePaths, null);

    boolean ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _zkclient, true);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // create 10 current states
    for (int i = 0; i < 10; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      boolean success = accessor.create(path, new ZNRecord("TestDB" + i), AccessOption.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in create: " + path);
    }

    // verify wtCache
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _zkclient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // update each current state 10 times, single thread
    for (int i = 0; i < 10; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      for (int j = 0; j < 10; j++) {
        ZNRecord newRecord = new ZNRecord("TestDB" + i);
        newRecord.setSimpleField("" + j, "" + j);
        boolean success =
            accessor.update(path, new ZNRecordUpdater(newRecord), AccessOption.PERSISTENT);
        Assert.assertTrue(success, "Should succeed in update: " + path);

      }
    }

    // verify cache
    // TestHelper.printCache(accessor._wtCache._cache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _zkclient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // set 10 external views
    for (int i = 0; i < 10; i++) {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
      boolean success = accessor.set(path, new ZNRecord("TestDB" + i), AccessOption.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in set: " + path);
    }

    // verify wtCache
    // accessor.printWtCache();
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _zkclient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // get 10 external views
    for (int i = 0; i < 10; i++) {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
      ZNRecord record = accessor.get(path, null, 0);
      Assert.assertEquals(record.getId(), "TestDB" + i);
    }

    // getChildNames
    List<String> childNames = accessor.getChildNames(extViewPath, 0);
    // System.out.println(childNames);
    Assert.assertEquals(childNames.size(), 10, "Should contain only: TestDB0-9");
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(childNames.contains("TestDB" + i));
    }

    // exists
    for (int i = 0; i < 10; i++) {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName, "localhost_8901",
              "session_0", "TestDB" + i);

      Assert.assertTrue(accessor.exists(path, 0));
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCreateFailZkCacheBaseDataAccessor() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // init zkCacheDataAccessor
    String curStatePath =
        PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName, "localhost_8901");

    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<ZNRecord>(baseAccessor, null, Arrays.asList(curStatePath), null);

    // create 10 current states
    for (int i = 0; i < 10; i++) {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName, "localhost_8901",
              "session_1", "TestDB" + i);
      boolean success = accessor.create(path, new ZNRecord("TestDB" + i), AccessOption.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in create: " + path);
    }

    // create same 10 current states again, should fail
    for (int i = 0; i < 10; i++) {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName, "localhost_8901",
              "session_1", "TestDB" + i);
      boolean success = accessor.create(path, new ZNRecord("TestDB" + i), AccessOption.PERSISTENT);
      Assert.assertFalse(success, "Should fail in create due to NodeExists: " + path);
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
