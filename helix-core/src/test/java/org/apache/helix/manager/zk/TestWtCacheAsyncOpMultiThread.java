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
import java.util.concurrent.Callable;

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

public class TestWtCacheAsyncOpMultiThread extends ZkTestBase {
  class TestCreateZkCacheBaseDataAccessor implements Callable<Boolean> {
    final ZkCacheBaseDataAccessor<ZNRecord> _accessor;
    final String _clusterName;
    final int _id;

    public TestCreateZkCacheBaseDataAccessor(ZkCacheBaseDataAccessor<ZNRecord> accessor,
        String clusterName, int id) {
      _accessor = accessor;
      _clusterName = clusterName;
      _id = id;
    }

    @Override
    public Boolean call() throws Exception {
      // create 10 current states in 2 steps
      List<String> paths = new ArrayList<String>();
      List<ZNRecord> records = new ArrayList<ZNRecord>();
      for (int j = 0; j < 2; j++) {
        paths.clear();
        records.clear();

        if (_id == 1 && j == 0) {
          // let thread_0 create 0-4
          Thread.sleep(30);
        }

        if (_id == 0 && j == 1) {
          // let thread_1 create 5-9
          Thread.sleep(100);
        }

        for (int i = 0; i < 5; i++) {
          int k = j * 5 + i;
          String path =
              PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, _clusterName,
                  "localhost_8901", "session_0", "TestDB" + k);
          ZNRecord record = new ZNRecord("TestDB" + k);

          paths.add(path);
          records.add(record);
        }

        boolean[] success = _accessor.createChildren(paths, records, AccessOption.PERSISTENT);
        // System.out.println("thread-" + _id + " creates " + j + ": " + Arrays.toString(success));

        // create all all sync'ed, so we shall see either all true or all false
        for (int i = 1; i < 5; i++) {
          Assert.assertEquals(success[i], success[0], "Should be either all succeed of all fail");
        }
      }

      return true;
    }
  }

  class TestUpdateZkCacheBaseDataAccessor implements Callable<Boolean> {
    final ZkCacheBaseDataAccessor<ZNRecord> _accessor;
    final String _clusterName;
    final int _id;

    public TestUpdateZkCacheBaseDataAccessor(ZkCacheBaseDataAccessor<ZNRecord> accessor,
        String clusterName, int id) {
      _accessor = accessor;
      _clusterName = clusterName;
      _id = id;
    }

    @Override
    public Boolean call() throws Exception {
      // create 10 current states in 2 steps
      List<String> paths = new ArrayList<String>();
      List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
      for (int j = 0; j < 10; j++) {
        paths.clear();
        updaters.clear();

        for (int i = 0; i < 10; i++) {
          String path =
              PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, _clusterName,
                  "localhost_8901", "session_0", "TestDB" + i);

          ZNRecord newRecord = new ZNRecord("TestDB" + i);
          newRecord.setSimpleField("" + j, "" + j);
          DataUpdater<ZNRecord> updater = new ZNRecordUpdater(newRecord);
          paths.add(path);
          updaters.add(updater);
        }

        boolean[] success = _accessor.updateChildren(paths, updaters, AccessOption.PERSISTENT);
        // System.out.println("thread-" + _id + " updates " + j + ": " + Arrays.toString(success));

        for (int i = 0; i < 10; i++) {
          Assert.assertEquals(success[i], true, "Should be all succeed");
        }
      }

      return true;
    }
  }

  class TestSetZkCacheBaseDataAccessor implements Callable<Boolean> {
    final ZkCacheBaseDataAccessor<ZNRecord> _accessor;
    final String _clusterName;
    final int _id;

    public TestSetZkCacheBaseDataAccessor(ZkCacheBaseDataAccessor<ZNRecord> accessor,
        String clusterName, int id) {
      _accessor = accessor;
      _clusterName = clusterName;
      _id = id;
    }

    @Override
    public Boolean call() throws Exception {
      // create 10 current states in 2 steps
      List<String> paths = new ArrayList<String>();
      List<ZNRecord> records = new ArrayList<ZNRecord>();
      for (int j = 0; j < 2; j++) {
        paths.clear();
        records.clear();

        if (_id == 1 && j == 0) {
          // let thread_0 create 0-4
          Thread.sleep(30);
        }

        if (_id == 0 && j == 1) {
          // let thread_1 create 5-9
          Thread.sleep(100);
        }

        for (int i = 0; i < 5; i++) {
          int k = j * 5 + i;
          String path =
              PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, _clusterName, "TestDB" + k);
          ZNRecord record = new ZNRecord("TestDB" + k);

          paths.add(path);
          records.add(record);
        }
        boolean[] success = _accessor.setChildren(paths, records, AccessOption.PERSISTENT);
        // System.out.println("thread-" + _id + " sets " + j + ": " + Arrays.toString(success));

        for (int i = 0; i < 5; i++) {
          Assert.assertEquals(success[i], true);
        }
      }

      return true;
    }
  }

  @Test
  public void testHappyPathZkCacheBaseDataAccessor() {
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

    // TestHelper.printCache(accessor._wtCache);
    boolean ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _zkclient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // create 10 current states using 2 threads
    List<Callable<Boolean>> threads = new ArrayList<Callable<Boolean>>();
    for (int i = 0; i < 2; i++) {
      threads.add(new TestCreateZkCacheBaseDataAccessor(accessor, clusterName, i));
    }
    TestHelper.startThreadsConcurrently(threads, 1000);

    // verify wtCache
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _zkclient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // update 10 current states 10 times using 2 threads
    threads.clear();
    for (int i = 0; i < 2; i++) {
      threads.add(new TestUpdateZkCacheBaseDataAccessor(accessor, clusterName, i));
    }
    TestHelper.startThreadsConcurrently(threads, 1000);

    // verify wtCache
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _zkclient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // set 10 external views using 2 threads
    threads.clear();
    for (int i = 0; i < 2; i++) {
      threads.add(new TestSetZkCacheBaseDataAccessor(accessor, clusterName, i));
    }
    TestHelper.startThreadsConcurrently(threads, 1000);

    // verify wtCache
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache._cache, _zkclient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
