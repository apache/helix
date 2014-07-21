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

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZNRecordUpdater;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkCacheAsyncOpSingleThread extends ZkTestBase {
  @Test
  public void testHappyPathExtOpZkCacheBaseDataAccessor() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // init external base data accessor
    ZkClient extZkclient = new ZkClient(_zkaddr);
    extZkclient.setZkSerializer(new ZNRecordSerializer());
    ZkBaseDataAccessor<ZNRecord> extBaseAccessor = new ZkBaseDataAccessor<ZNRecord>(extZkclient);

    // init zkCacheBaseDataAccessor
    String curStatePath =
        PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName, "localhost_8901");
    String extViewPath = PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    extBaseAccessor.create(curStatePath, null, AccessOption.PERSISTENT);

    List<String> zkCacheInitPaths = Arrays.asList(curStatePath, extViewPath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<ZNRecord>(baseAccessor, null, null, zkCacheInitPaths);

    // TestHelper.printCache(accessor._zkCache);
    boolean ret =
        TestHelper.verifyZkCache(zkCacheInitPaths, accessor._zkCache._cache, _zkclient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // create 10 current states using external base accessor
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++) {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName, "localhost_8901",
              "session_0", "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }

    boolean[] success = extBaseAccessor.createChildren(paths, records, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(success[i], "Should succeed in create: " + paths.get(i));
    }

    // wait zkEventThread update zkCache
    // verify wtCache
    for (int i = 0; i < 10; i++) {
      // TestHelper.printCache(accessor._zkCache);
      ret = TestHelper.verifyZkCache(zkCacheInitPaths, accessor._zkCache._cache, _zkclient, true);
      if (ret == true)
        break;
      Thread.sleep(100);
    }

    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // update each current state 10 times by external base accessor
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
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
      success = extBaseAccessor.updateChildren(paths, updaters, AccessOption.PERSISTENT);

      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(success[i], "Should succeed in update: " + paths.get(i));
      }
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify cache
    // TestHelper.printCache(accessor._zkCache);
    ret = TestHelper.verifyZkCache(zkCacheInitPaths, accessor._zkCache._cache, _zkclient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // set 10 external views by external accessor
    paths.clear();
    records.clear();
    for (int i = 0; i < 10; i++) {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }
    success = extBaseAccessor.setChildren(paths, records, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(success[i], "Should succeed in set: " + paths.get(i));
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret = TestHelper.verifyZkCache(zkCacheInitPaths, accessor._zkCache._cache, _zkclient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // remove 10 external views by external accessor
    paths.clear();
    for (int i = 0; i < 10; i++) {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);

      paths.add(path);
    }
    success = extBaseAccessor.remove(paths, 0);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(success[i], "Should succeed in remove: " + paths.get(i));
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret = TestHelper.verifyZkCache(zkCacheInitPaths, accessor._zkCache._cache, _zkclient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // clean up
    extZkclient.close();
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testHappyPathSelfOpZkCacheBaseDataAccessor() throws Exception {
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

    List<String> zkCacheInitPaths = Arrays.asList(curStatePath, extViewPath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<ZNRecord>(baseAccessor, null, null, zkCacheInitPaths);

    // TestHelper.printCache(accessor._zkCache._cache);
    boolean ret =
        TestHelper.verifyZkCache(zkCacheInitPaths, accessor._zkCache._cache, _zkclient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // create 10 current states using this accessor
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++) {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName, "localhost_8901",
              "session_0", "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }

    boolean[] success = accessor.createChildren(paths, records, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(success[i], "Should succeed in create: " + paths.get(i));
    }

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret = TestHelper.verifyZkCache(zkCacheInitPaths, accessor._zkCache._cache, _zkclient, false);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // update each current state 10 times by this accessor
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
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
    // TestHelper.printCache(accessor._zkCache._cache);
    ret =
        TestHelper.verifyZkCache(zkCacheInitPaths, zkCacheInitPaths, accessor._zkCache._cache,
            _zkclient, true);
    // ret = TestHelper.verifyZkCache(zkCacheInitPaths, accessor, zkclient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // set 10 external views 10 times by this accessor
    paths.clear();
    records.clear();
    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 10; i++) {
        String path =
            PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
        ZNRecord record = new ZNRecord("TestDB" + i);
        record.setSimpleField("setKey", "" + j);

        paths.add(path);
        records.add(record);
      }
      success = accessor.setChildren(paths, records, AccessOption.PERSISTENT);
      for (int i = 0; i < 10; i++) {
        Assert.assertTrue(success[i], "Should succeed in set: " + paths.get(i));
      }
    }

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret = TestHelper.verifyZkCache(zkCacheInitPaths, accessor._zkCache._cache, _zkclient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // get 10 external views
    paths.clear();
    records.clear();
    for (int i = 0; i < 10; i++) {
      String path = extViewPath + "/TestDB" + i;
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

    // // exists
    paths.clear();
    for (int i = 0; i < 10; i++) {
      String path = curStatePath + "/session_0/TestDB" + i;
      // // PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
      // // clusterName,
      // // "localhost_8901",
      // // "session_0",
      // // "TestDB" + i);
      paths.add(path);
    }
    success = accessor.exists(paths, 0);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(success[i], "Should exits: " + paths.get(i));
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

}
