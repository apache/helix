package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.BaseDataAccessor.Option;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordUpdater;
import com.linkedin.helix.ZkUnitTestBase;

public class TestZkCacheAsyncOpSingleThread extends ZkUnitTestBase
{

  // TODO: remove this
  @Test
  public void testHappyPathExternalOp() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    // init external base data accessor
    ZkClient zkclient = new ZkClient(ZK_ADDR);
    zkclient.setZkSerializer(new ZNRecordSerializer());
    ZkBaseDataAccessor<ZNRecord> extBaseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(zkclient);

    // init zkCacheDataAccessor
    String curStatePath =
        PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                   clusterName,
                                   "localhost_8901");
    String extViewPath =
        PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    extBaseAccessor.create(curStatePath, null, Option.PERSISTENT);

    List<String> cachePaths = Arrays.asList(curStatePath, extViewPath);
    ZkCachedDataAccessor<ZNRecord> accessor =
        new ZkCachedDataAccessor<ZNRecord>(baseAccessor, null, cachePaths, null);

    // TestHelper.printCache(accessor._zkCache);
    boolean ret =
        TestHelper.verifyZkCache(cachePaths, accessor._zkCache, _gZkClient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // create 10 current states using external base accessor
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_0",
                                     "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }

    boolean[] success = extBaseAccessor.createChildren(paths, records, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should succeed in create: " + paths.get(i));
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify wtCache
    // TestHelper.printCache(accessor._zkCache);

    ret = TestHelper.verifyZkCache(cachePaths, accessor._zkCache, _gZkClient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // update each current state 10 times by external base accessor
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
    for (int j = 0; j < 10; j++)
    {
      paths.clear();
      updaters.clear();
      for (int i = 0; i < 10; i++)
      {
        String path = curStatePath + "/session_0/TestDB" + i;
        ZNRecord newRecord = new ZNRecord("TestDB" + i);
        newRecord.setSimpleField("" + j, "" + j);
        DataUpdater<ZNRecord> updater = new ZNRecordUpdater(newRecord);
        paths.add(path);
        updaters.add(updater);
      }
      success = extBaseAccessor.updateChildren(paths, updaters, Option.PERSISTENT);

      for (int i = 0; i < 10; i++)
      {
        Assert.assertTrue(success[i], "Should succeed in update: " + paths.get(i));
      }
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify cache
    // TestHelper.printCache(accessor._zkCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._zkCache, _gZkClient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // set 10 external views
    paths.clear();
    records.clear();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }
    success = extBaseAccessor.setChildren(paths, records, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should succeed in set: " + paths.get(i));
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify cache
    // TestHelper.printCache(accessor._zkCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._zkCache, _gZkClient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // clean up
    zkclient.close();
    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testHappyPathSelfOp() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    // init zkCacheDataAccessor
    String curStatePath =
        PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                   clusterName,
                                   "localhost_8901");
    String extViewPath =
        PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    baseAccessor.create(curStatePath, null, Option.PERSISTENT);

    List<String> cachePaths = Arrays.asList(curStatePath, extViewPath);
    ZkCachedDataAccessor<ZNRecord> accessor =
        new ZkCachedDataAccessor<ZNRecord>(baseAccessor, null, cachePaths, null);

    // TestHelper.printCache(accessor._zkCache);
    boolean ret =
        TestHelper.verifyZkCache(cachePaths, accessor._zkCache, _gZkClient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // create 10 current states using external base accessor
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_0",
                                     "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }

    boolean[] success = accessor.create(paths, records, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should succeed in create: " + paths.get(i));
    }

    // verify wtCache
    // TestHelper.printCache(accessor._zkCache);

    ret = TestHelper.verifyZkCache(cachePaths, accessor._zkCache, _gZkClient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // update each current state 10 times by external base accessor
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
    for (int j = 0; j < 10; j++)
    {
      paths.clear();
      updaters.clear();
      for (int i = 0; i < 10; i++)
      {
        String path = curStatePath + "/session_0/TestDB" + i;
        ZNRecord newRecord = new ZNRecord("TestDB" + i);
        newRecord.setSimpleField("" + j, "" + j);
        DataUpdater<ZNRecord> updater = new ZNRecordUpdater(newRecord);
        paths.add(path);
        updaters.add(updater);
      }
      success = accessor.update(paths, updaters, Option.PERSISTENT);

      for (int i = 0; i < 10; i++)
      {
        Assert.assertTrue(success[i], "Should succeed in update: " + paths.get(i));
      }
    }

    // verify cache
    // TestHelper.printCache(accessor._zkCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._zkCache, _gZkClient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // set 10 external views
    paths.clear();
    records.clear();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }
    success = accessor.set(paths, records, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should succeed in set: " + paths.get(i));
    }

    // verify cache
    // TestHelper.printCache(accessor._zkCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._zkCache, _gZkClient, true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // get 10 external views
    paths.clear();
    records.clear();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
      paths.add(path);
    }

    records = accessor.get(paths, null, 0);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertEquals(records.get(i).getId(), "TestDB" + i);
    }

    // getChildren
    records.clear();
    records = accessor.getChildren(extViewPath, 0);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertEquals(records.get(i).getId(), "TestDB" + i);
    }

    // exists
    paths.clear();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_0",
                                     "TestDB" + i);
      paths.add(path);
    }
    success = accessor.exists(paths);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should exits: TestDB" + i);
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testHappyPathExtOpZkCacheBaseDataAccessor() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    // init external base data accessor
    ZkClient extZkclient = new ZkClient(ZK_ADDR);
    extZkclient.setZkSerializer(new ZNRecordSerializer());
    ZkBaseDataAccessor<ZNRecord> extBaseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(extZkclient);

    // init zkCacheBaseDataAccessor
    String curStatePath =
        PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                   clusterName,
                                   "localhost_8901");
    String extViewPath =
        PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    extBaseAccessor.create(curStatePath, null, Option.PERSISTENT);

    List<String> zkCacheInitPaths = Arrays.asList(curStatePath, extViewPath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<ZNRecord>(baseAccessor, null, null, zkCacheInitPaths);

    // TestHelper.printCache(accessor._zkCache);
    boolean ret =
        TestHelper.verifyZkCache(zkCacheInitPaths,
                                 accessor._zkCache._cache,
                                 _gZkClient,
                                 true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // create 10 current states using external base accessor
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_0",
                                     "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }

    boolean[] success = extBaseAccessor.createChildren(paths, records, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should succeed in create: " + paths.get(i));
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify wtCache
    // TestHelper.printCache(accessor._zkCache);

    ret =
        TestHelper.verifyZkCache(zkCacheInitPaths,
                                 accessor._zkCache._cache,
                                 _gZkClient,
                                 true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // update each current state 10 times by external base accessor
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
    for (int j = 0; j < 10; j++)
    {
      paths.clear();
      updaters.clear();
      for (int i = 0; i < 10; i++)
      {
        String path = curStatePath + "/session_0/TestDB" + i;
        ZNRecord newRecord = new ZNRecord("TestDB" + i);
        newRecord.setSimpleField("" + j, "" + j);
        DataUpdater<ZNRecord> updater = new ZNRecordUpdater(newRecord);
        paths.add(path);
        updaters.add(updater);
      }
      success = extBaseAccessor.updateChildren(paths, updaters, Option.PERSISTENT);

      for (int i = 0; i < 10; i++)
      {
        Assert.assertTrue(success[i], "Should succeed in update: " + paths.get(i));
      }
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify cache
    // TestHelper.printCache(accessor._zkCache);
    ret =
        TestHelper.verifyZkCache(zkCacheInitPaths,
                                 accessor._zkCache._cache,
                                 _gZkClient,
                                 true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // set 10 external views by external accessor
    paths.clear();
    records.clear();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }
    success = extBaseAccessor.setChildren(paths, records, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should succeed in set: " + paths.get(i));
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret =
        TestHelper.verifyZkCache(zkCacheInitPaths,
                                 accessor._zkCache._cache,
                                 _gZkClient,
                                 true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // remove 10 external views by external accessor
    paths.clear();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);

      paths.add(path);
    }
    success = extBaseAccessor.remove(paths, 0);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should succeed in remove: " + paths.get(i));
    }

    // wait zkEventThread update zkCache
    Thread.sleep(100);

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret =
        TestHelper.verifyZkCache(zkCacheInitPaths,
                                 accessor._zkCache._cache,
                                 _gZkClient,
                                 true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // clean up
    extZkclient.close();
    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testHappyPathSelfOpZkCacheBaseDataAccessor() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    // init zkCacheDataAccessor
    String curStatePath =
        PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                   clusterName,
                                   "localhost_8901");
    String extViewPath =
        PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName);

    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    baseAccessor.create(curStatePath, null, Option.PERSISTENT);

    List<String> zkCacheInitPaths = Arrays.asList(curStatePath, extViewPath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<ZNRecord>(baseAccessor, null, null, zkCacheInitPaths);

    // TestHelper.printCache(accessor._zkCache._cache);
    boolean ret =
        TestHelper.verifyZkCache(zkCacheInitPaths,
                                 accessor._zkCache._cache,
                                 _gZkClient,
                                 true);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // create 10 current states using this accessor
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_0",
                                     "TestDB" + i);
      ZNRecord record = new ZNRecord("TestDB" + i);

      paths.add(path);
      records.add(record);
    }

    boolean[] success = accessor.createChildren(paths, records, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should succeed in create: " + paths.get(i));
    }

    // verify cache
//    TestHelper.printCache(accessor._zkCache._cache);
    ret =
        TestHelper.verifyZkCache(zkCacheInitPaths,
                                 accessor._zkCache._cache,
                                 _gZkClient,
                                 false);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // update each current state 10 times by this accessor
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
    for (int j = 0; j < 10; j++)
    {
      paths.clear();
      updaters.clear();
      for (int i = 0; i < 10; i++)
      {
        String path = curStatePath + "/session_0/TestDB" + i;
        ZNRecord newRecord = new ZNRecord("TestDB" + i);
        newRecord.setSimpleField("" + j, "" + j);
        DataUpdater<ZNRecord> updater = new ZNRecordUpdater(newRecord);
        paths.add(path);
        updaters.add(updater);
      }
      success = accessor.updateChildren(paths, updaters, Option.PERSISTENT);

      for (int i = 0; i < 10; i++)
      {
        Assert.assertTrue(success[i], "Should succeed in update: " + paths.get(i));
      }
    }

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret =
        TestHelper.verifyZkCache(zkCacheInitPaths,
                                 zkCacheInitPaths,
                                 accessor._zkCache._cache,
                                 _gZkClient,
                                 true);
    // ret = TestHelper.verifyZkCache(zkCacheInitPaths, accessor, _gZkClient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // set 10 external views 10 times by this accessor
    paths.clear();
    records.clear();
    for (int j = 0; j < 10; j++)
    {
      for (int i = 0; i < 10; i++)
      {
        String path =
            PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB"
                + i);
        ZNRecord record = new ZNRecord("TestDB" + i);
        record.setSimpleField("setKey", "" + j);

        paths.add(path);
        records.add(record);
      }
      success = accessor.setChildren(paths, records, Option.PERSISTENT);
      for (int i = 0; i < 10; i++)
      {
        Assert.assertTrue(success[i], "Should succeed in set: " + paths.get(i));
      }
    }

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret =
        TestHelper.verifyZkCache(zkCacheInitPaths,
                                 accessor._zkCache._cache,
                                 _gZkClient,
                                 true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");

    // get 10 external views
    paths.clear();
    records.clear();
    for (int i = 0; i < 10; i++)
    {
      String path = extViewPath + "/TestDB" + i;
      paths.add(path);
    }

    records = accessor.get(paths, null, 0);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertEquals(records.get(i).getId(), "TestDB" + i);
    }

    // getChildren
    records.clear();
    records = accessor.getChildren(extViewPath, null, 0);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertEquals(records.get(i).getId(), "TestDB" + i);
    }

    // // exists
    paths.clear();
    for (int i = 0; i < 10; i++)
    {
      String path = curStatePath + "/session_0/TestDB" + i;
      // // PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
      // // clusterName,
      // // "localhost_8901",
      // // "session_0",
      // // "TestDB" + i);
      paths.add(path);
    }
    success = accessor.exists(paths, 0);
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(success[i], "Should exits: " + paths.get(i));
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }

}
