package com.linkedin.helix.store.zk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.BaseDataAccessor.Option;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.HelixPropertyListener;

public class TestZkHelixPropertyStore extends ZkUnitTestBase
{
  final String _root         = "/" + getShortClassName();
  final int    bufSize       = 128;
  final int    mapNr         = 10;
  final int    firstLevelNr  = 10;
  final int    secondLevelNr = 10;

  // final int totalNodes = firstLevelNr * secondLevelNr;

  class TestListener implements HelixPropertyListener
  {
    Map<String, Long> _changeKeys = new HashMap<String, Long>();
    Map<String, Long> _createKeys = new HashMap<String, Long>();
    Map<String, Long> _deleteKeys = new HashMap<String, Long>();

    public void reset()
    {
      _changeKeys.clear();
      _createKeys.clear();
      _deleteKeys.clear();
    }

    @Override
    public void onDataChange(String path)
    {
      _changeKeys.put(path, new Long(System.currentTimeMillis()));
    }

    @Override
    public void onDataCreate(String path)
    {
      _createKeys.put(path, new Long(System.currentTimeMillis()));
    }

    @Override
    public void onDataDelete(String path)
    {
      _deleteKeys.put(path, new Long(System.currentTimeMillis()));
    }
  }

  @Test
  public void testSet()
  {
    System.out.println("START testSet() at " + new Date(System.currentTimeMillis()));

    String subRoot = _root + "/" + "set";
    List<String> subscribedPaths = new ArrayList<String>();
    subscribedPaths.add(subRoot);
    ZkHelixPropertyStore<ZNRecord> store =
        new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_gZkClient),
                                           subRoot,
                                           subscribedPaths);

    setNodes(store, 'a', false);
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = getNodeId(i, j);
        String key = getSecondLevelKey(i, j);
        ZNRecord record = store.get(key, null, 0);
        Assert.assertEquals(record.getId(), nodeId);
      }
    }

    long startT = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++)
    {
      ZNRecord record = store.get("/node_0/childNode_0_0", null, 0);
      Assert.assertNotNull(record);
    }
    long endT = System.currentTimeMillis();
    System.out.println("1000 Get() time used: " + (endT - startT) + "ms");
    Assert.assertTrue((endT - startT) < 60, "1000 Gets should be finished within 50ms");

    System.out.println("END testSet() at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testLocalTriggeredCallback() throws Exception
  {
    System.out.println("START testLocalTriggeredCallback() at "
        + new Date(System.currentTimeMillis()));

    String subRoot = _root + "/" + "localCallback";
    List<String> subscribedPaths = new ArrayList<String>();
    subscribedPaths.add(subRoot);
    ZkHelixPropertyStore<ZNRecord> store =
        new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_gZkClient),
                                           subRoot,
                                           subscribedPaths);

    // change nodes via property store interface
    // and verify all notifications have been received
    TestListener listener = new TestListener();
    store.subscribe("/", listener);

    // test dataCreate callbacks
    listener.reset();
    setNodes(store, 'a', true);

    // wait until all callbacks have been received
    Thread.sleep(500);
    int expectCreateNodes = 1 + firstLevelNr + firstLevelNr * secondLevelNr;
    System.out.println("createKey#:" + listener._createKeys.size() + ", changeKey#:"
        + listener._changeKeys.size() + ", deleteKey#:" + listener._deleteKeys.size());
    Assert.assertTrue(listener._createKeys.size() == expectCreateNodes, "Should receive "
        + expectCreateNodes + " create callbacks");

    // test dataChange callbacks
    listener.reset();
    setNodes(store, 'b', true);

    // wait until all callbacks have been received
    Thread.sleep(500);
    int expectChangeNodes = firstLevelNr * secondLevelNr;
    System.out.println("createKey#:" + listener._createKeys.size() + ", changeKey#:"
        + listener._changeKeys.size() + ", deleteKey#:" + listener._deleteKeys.size());
    Assert.assertTrue(listener._changeKeys.size() >= expectChangeNodes,
                      "Should receive at least " + expectChangeNodes
                          + " change callbacks");

    // test delete callbacks
    listener.reset();
    int expectDeleteNodes = 1 + firstLevelNr + firstLevelNr * secondLevelNr;
    store.remove("/", 0);
    // wait until all callbacks have been received
    Thread.sleep(500);

    System.out.println("createKey#:" + listener._createKeys.size() + ", changeKey#:"
        + listener._changeKeys.size() + ", deleteKey#:" + listener._deleteKeys.size());
    Assert.assertTrue(listener._deleteKeys.size() == expectDeleteNodes, "Should receive "
        + expectDeleteNodes + " delete callbacks");

    System.out.println("END testLocalTriggeredCallback() at "
        + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testZkTriggeredCallback() throws Exception
  {
    System.out.println("START testZkTriggeredCallback() at "
        + new Date(System.currentTimeMillis()));

    String subRoot = _root + "/" + "zkCallback";
    List<String> subscribedPaths = Arrays.asList(subRoot);
    ZkHelixPropertyStore<ZNRecord> store =
        new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_gZkClient),
                                           subRoot,
                                           subscribedPaths);

    // change nodes via property store interface
    // and verify all notifications have been received
    TestListener listener = new TestListener();
    store.subscribe("/", listener);

    // test create callbacks
    listener.reset();
    setNodes(_gZkClient, subRoot, 'a', true);
    int expectCreateNodes = 1 + firstLevelNr + firstLevelNr * secondLevelNr;
    Thread.sleep(500);

    System.out.println("createKey#:" + listener._createKeys.size() + ", changeKey#:"
        + listener._changeKeys.size() + ", deleteKey#:" + listener._deleteKeys.size());
    Assert.assertTrue(listener._createKeys.size() == expectCreateNodes, "Should receive "
        + expectCreateNodes + " create callbacks");

    // test change callbacks
    listener.reset();
    setNodes(_gZkClient, subRoot, 'b', true);
    int expectChangeNodes = firstLevelNr * secondLevelNr;
    Thread.sleep(500);

    System.out.println("createKey#:" + listener._createKeys.size() + ", changeKey#:"
        + listener._changeKeys.size() + ", deleteKey#:" + listener._deleteKeys.size());
    Assert.assertTrue(listener._changeKeys.size() >= expectChangeNodes,
                      "Should receive at least " + expectChangeNodes
                          + " change callbacks");

    // test delete callbacks
    listener.reset();
    int expectDeleteNodes = 1 + firstLevelNr + firstLevelNr * secondLevelNr;
    _gZkClient.deleteRecursive(subRoot);
    Thread.sleep(1000);

    System.out.println("createKey#:" + listener._createKeys.size() + ", changeKey#:"
        + listener._changeKeys.size() + ", deleteKey#:" + listener._deleteKeys.size());
    Assert.assertTrue(listener._deleteKeys.size() == expectDeleteNodes, "Should receive "
        + expectDeleteNodes + " delete callbacks");

    System.out.println("END testZkTriggeredCallback() at "
        + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testBackToBackRemoveAndSet() throws Exception
  {
    System.out.println("START testBackToBackRemoveAndSet() at "
        + new Date(System.currentTimeMillis()));

    String subRoot = _root + "/" + "backToBackRemoveAndSet";
    List<String> subscribedPaths = new ArrayList<String>();
    subscribedPaths.add(subRoot);
    ZkHelixPropertyStore<ZNRecord> store =
        new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_gZkClient),
                                           subRoot,
                                           subscribedPaths);

    store.set("/child0", new ZNRecord("child0"), BaseDataAccessor.Option.PERSISTENT);

    ZNRecord record = store.get("/child0", null, 0); // will put the record in cache
    Assert.assertEquals(record.getId(), "child0");
    // System.out.println("1:get:" + record);

    String child0Path = subRoot + "/child0";
    for (int i = 0; i < 2; i++)
    {
      _gZkClient.delete(child0Path);
      _gZkClient.createPersistent(child0Path, new ZNRecord("child0-new-" + i));
    }

    Thread.sleep(500); // should wait for zk callback to add "/child0" into cache
    record = store.get("/child0", null, 0);
    Assert.assertEquals(record.getId(),
                        "child0-new-1",
                        "Cache shoulde be updated to latest create");
    // System.out.println("2:get:" + record);

    _gZkClient.delete(child0Path);
    Thread.sleep(500); // should wait for zk callback to remove "/child0" from cache
    try
    {
      record = store.get("/child0", null, 0);
      Assert.fail("/child0 should have been removed");
    }
    catch (ZkNoNodeException e)
    {
      // OK.
    }
    // System.out.println("3:get:" + record);

    System.out.println("END testBackToBackRemoveAndSet() at "
        + new Date(System.currentTimeMillis()));
  }

  private String getNodeId(int i, int j)
  {
    return "childNode_" + i + "_" + j;
  }

  private String getSecondLevelKey(int i, int j)
  {
    return "/node_" + i + "/" + getNodeId(i, j);
  }

  private String getFirstLevelKey(int i)
  {
    return "/node_" + i;
  }

  private void setNodes(ZkHelixPropertyStore<ZNRecord> store,
                        char c,
                        boolean needTimestamp)
  {
    char[] data = new char[bufSize];

    for (int i = 0; i < bufSize; i++)
    {
      data[i] = c;
    }

    Map<String, String> map = new TreeMap<String, String>();
    for (int i = 0; i < mapNr; i++)
    {
      map.put("key_" + i, new String(data));
    }

    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String nodeId = getNodeId(i, j);
        ZNRecord record = new ZNRecord(nodeId);
        record.setSimpleFields(map);
        if (needTimestamp)
        {
          long now = System.currentTimeMillis();
          record.setSimpleField("SetTimestamp", Long.toString(now));
        }
        String key = getSecondLevelKey(i, j);
        store.set(key, record, Option.PERSISTENT);
      }
    }
  }

  private void setNodes(ZkClient zkClient, String root, char c, boolean needTimestamp)
  {
    char[] data = new char[bufSize];

    for (int i = 0; i < bufSize; i++)
    {
      data[i] = c;
    }

    Map<String, String> map = new TreeMap<String, String>();
    for (int i = 0; i < mapNr; i++)
    {
      map.put("key_" + i, new String(data));
    }

    for (int i = 0; i < firstLevelNr; i++)
    {
      String firstLevelKey = getFirstLevelKey(i);

      for (int j = 0; j < secondLevelNr; j++)
      {
        String nodeId = getNodeId(i, j);
        ZNRecord record = new ZNRecord(nodeId);
        record.setSimpleFields(map);
        if (needTimestamp)
        {
          long now = System.currentTimeMillis();
          record.setSimpleField("SetTimestamp", Long.toString(now));
        }
        String key = getSecondLevelKey(i, j);
        try
        {
          zkClient.writeData(root + key, record);
        }
        catch (ZkNoNodeException e)
        {
          zkClient.createPersistent(root + firstLevelKey, true);
          zkClient.createPersistent(root + key, record);
        }
      }
    }
  }

}
