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

public class TestWtCacheAsyncOpSingleThread extends ZkUnitTestBase
{
  @Test
  public void testHappyPath()
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
        new ZkCachedDataAccessor<ZNRecord>(baseAccessor,
                                           null,
                                           null,
                                           cachePaths);

    boolean ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");


    // create 10 current states
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
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");


    // update each current state 10 times
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
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");


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

    // verify wtCache
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");


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
  public void testCreateFail()
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

    ZkCachedDataAccessor<ZNRecord> accessor =
        new ZkCachedDataAccessor<ZNRecord>(baseAccessor,
                                           null,
                                           null,
                                           Arrays.asList(curStatePath, extViewPath));

    Assert.assertEquals(accessor._wtCache.size(), 1, "Should contain only:\n"
        + curStatePath);
    Assert.assertTrue(accessor._wtCache.containsKey(curStatePath));

    // create 10 current states
    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_1",
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
    
    
    // create same 10 current states again, should fail on NodeExists
    success = accessor.create(paths, records, Option.PERSISTENT);
    // System.out.println(Arrays.toString(success));
    for (int i = 0; i < 10; i++)
    {
      Assert.assertFalse(success[i], "Should fail on create: " + paths.get(i));
    }
    
    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }
}
