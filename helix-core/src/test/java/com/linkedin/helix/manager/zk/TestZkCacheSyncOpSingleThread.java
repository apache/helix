package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.AccessOption;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordUpdater;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.store.HelixPropertyListener;

public class TestZkCacheSyncOpSingleThread extends ZkUnitTestBase
{
  class TestListener implements HelixPropertyListener
  {
    ConcurrentLinkedQueue<String> _deletePathQueue = new ConcurrentLinkedQueue<String>();
    ConcurrentLinkedQueue<String> _createPathQueue = new ConcurrentLinkedQueue<String>();
    ConcurrentLinkedQueue<String> _changePathQueue = new ConcurrentLinkedQueue<String>();

    @Override
    public void onDataDelete(String path)
    {
      // System.out.println(Thread.currentThread().getName() + ", onDelete: " + path);
      _deletePathQueue.add(path);
    }

    @Override
    public void onDataCreate(String path)
    {
      // System.out.println(Thread.currentThread().getName() + ", onCreate: " + path);
      _createPathQueue.add(path);
    }

    @Override
    public void onDataChange(String path)
    {
      // System.out.println(Thread.currentThread().getName() + ", onChange: " + path);
      _changePathQueue.add(path);
    }

    public void reset()
    {
      _deletePathQueue.clear();
      _createPathQueue.clear();
      _changePathQueue.clear();
    }
  }

  @Test
  public void testZkCacheCallbackExternalOpNoChroot() throws Exception
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

    extBaseAccessor.create(curStatePath, null, AccessOption.PERSISTENT);

    List<String> cachePaths = Arrays.asList(curStatePath, extViewPath);
    ZkCacheBaseDataAccessor<ZNRecord> accessor =
        new ZkCacheBaseDataAccessor<ZNRecord>(baseAccessor, null, null, cachePaths);
    // TestHelper.printCache(accessor._zkCache._cache);

    TestListener listener = new TestListener();
    accessor.subscribe(curStatePath, listener);

    // create 10 current states
    List<String> createPaths = new ArrayList<String>();
    for (int i = 0; i < 10; i++)
    {
      String path = curStatePath + "/session_0/TestDB" + i;
      createPaths.add(path);
      boolean success =
          extBaseAccessor.create(path,
                                 new ZNRecord("TestDB" + i),
                                 AccessOption.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in create: " + path);
    }

    Thread.sleep(500);

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    boolean ret =
        TestHelper.verifyZkCache(cachePaths, accessor._zkCache._cache, _gZkClient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");
    System.out.println("createCnt: " + listener._createPathQueue.size());
    Assert.assertEquals(listener._createPathQueue.size(),
                        11,
                        "Shall get 11 onCreate callbacks.");

    // verify each callback path
    createPaths.add(curStatePath + "/session_0");
    List<String> createCallbackPaths = new ArrayList<String>(listener._createPathQueue);
    Collections.sort(createPaths);
    Collections.sort(createCallbackPaths);
    // System.out.println("createCallbackPaths: " + createCallbackPaths);
    Assert.assertEquals(createCallbackPaths,
                        createPaths,
                        "Should get create callbacks at " + createPaths + ", but was "
                            + createCallbackPaths);

    // update each current state, single thread
    List<String> updatePaths = new ArrayList<String>();
    listener.reset();
    for (int i = 0; i < 10; i++)
    {
      String path = curStatePath + "/session_0/TestDB" + i;
      for (int j = 0; j < 10; j++)
      {
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
    ret =
        TestHelper.verifyZkCache(cachePaths, accessor._zkCache._cache, _gZkClient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");
    System.out.println("changeCnt: " + listener._changePathQueue.size());
    Assert.assertEquals(listener._changePathQueue.size(),
                        100,
                        "Shall get 100 onChange callbacks.");

    // verify each callback path
    List<String> updateCallbackPaths = new ArrayList<String>(listener._changePathQueue);
    Collections.sort(updatePaths);
    Collections.sort(updateCallbackPaths);
    Assert.assertEquals(updateCallbackPaths,
                        updatePaths,
                        "Should get change callbacks at " + updatePaths + ", but was "
                            + updateCallbackPaths);

    // remove 10 current states
    List<String> removePaths = new ArrayList<String>();
    listener.reset();
    for (int i = 0; i < 10; i++)
    {
      String path = curStatePath + "/session_0/TestDB" + i;
      removePaths.add(path);
      boolean success = accessor.remove(path, AccessOption.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in remove: " + path);
    }
    Thread.sleep(500);

    // verify cache
    // TestHelper.printCache(accessor._zkCache._cache);
    ret =
        TestHelper.verifyZkCache(cachePaths, accessor._zkCache._cache, _gZkClient, true);
    // System.out.println("ret: " + ret);
    Assert.assertTrue(ret, "zkCache doesn't match data on Zk");
    System.out.println("deleteCnt: " + listener._deletePathQueue.size());
    Assert.assertEquals(listener._deletePathQueue.size(),
                        10,
                        "Shall get 10 onDelete callbacks.");

    // verify each callback path
    List<String> removeCallbackPaths = new ArrayList<String>(listener._deletePathQueue);
    Collections.sort(removePaths);
    Collections.sort(removeCallbackPaths);
    Assert.assertEquals(removeCallbackPaths,
                        removePaths,
                        "Should get remove callbacks at " + removePaths + ", but was "
                            + removeCallbackPaths);

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
