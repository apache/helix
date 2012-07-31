package com.linkedin.helix.manager.zk;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.BaseDataAccessor.Option;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordUpdater;
import com.linkedin.helix.ZkUnitTestBase;

public class TestWtCacheSyncOpSingleThread extends ZkUnitTestBase
{
  // TODO: add TestZkCacheSyncOpSingleThread
  // TODO: add TestZkCacheAsyncOpSingleThread
  // TODO: add TestZkCacheAsyncOpMultiThread

  
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
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_0",
                                     "TestDB" + i);
      boolean success =
          accessor.create(path, new ZNRecord("TestDB" + i), Option.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in create: " + path);
    }

    // verify wtCache
    // TestHelper.printCache(accessor._wtCache);
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");


    // update each current state 10 times, single thread
    for (int i = 0; i < 10; i++)
    {
      String path = curStatePath + "/session_0/TestDB" + i;
      for (int j = 0; j < 10; j++)
      {
        ZNRecord newRecord = new ZNRecord("TestDB" + i);
        newRecord.setSimpleField("" + j, "" + j);
        boolean success =
            accessor.update(path, new ZNRecordUpdater(newRecord), Option.PERSISTENT);
        Assert.assertTrue(success, "Should succeed in update: " + path);

      }
    }

    // verify cache
    // accessor.printWtCache();
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");

    // set 10 external views
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
      boolean success = accessor.set(path, new ZNRecord("TestDB" + i), Option.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in set: " + path);
    }

    // verify wtCache
    // accessor.printWtCache();
    ret = TestHelper.verifyZkCache(cachePaths, accessor._wtCache, _gZkClient, false);
    Assert.assertTrue(ret, "wtCache doesn't match data on Zk");


    // get 10 external views
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName, "TestDB" + i);
      ZNRecord record = accessor.get(path, null, 0);
      Assert.assertEquals(record.getId(), "TestDB" + i);
    }

    // getChildNames
    List<String> childNames = accessor.getChildNames(extViewPath, 0);
    // System.out.println(childNames);
    Assert.assertEquals(childNames.size(), 10, "Should contain only: TestDB0-9");
    for (int i = 0; i < 10; i++)
    {
      Assert.assertTrue(childNames.contains("TestDB" + i));
    }
    
    // exists
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_0",
                                     "TestDB" + i);

      Assert.assertTrue(accessor.exists(path));
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
    
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    ZkCachedDataAccessor<ZNRecord> accessor =
        new ZkCachedDataAccessor<ZNRecord>(baseAccessor,
                                           null,
                                           null,
                                           Arrays.asList(curStatePath));

    // create 10 current states
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_1",
                                     "TestDB" + i);
      boolean success =
          accessor.create(path, new ZNRecord("TestDB" + i), Option.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in create: " + path);
    }
    
    // create same 10 current states again, should fail
    for (int i = 0; i < 10; i++)
    {
      String path =
          PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                     clusterName,
                                     "localhost_8901",
                                     "session_1",
                                     "TestDB" + i);
      boolean success =
          accessor.create(path, new ZNRecord("TestDB" + i), Option.PERSISTENT);
      Assert.assertFalse(success, "Should fail in create due to NodeExists: " + path);
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
