package org.apache.helix.store.zk;

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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ByteArraySerializer;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.PropertyChangeListener;
import org.apache.helix.store.PropertyJsonComparator;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.store.PropertyStoreException;
import org.apache.helix.store.zk.ZKPropertyStore;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


// TODO need to write performance test for zk-property store
public class TestZKPropertyStore extends ZkUnitTestBase
{
  private static final Logger LOG = Logger.getLogger(TestZKPropertyStore.class);
  final String className = getShortClassName();
  final int bufSize = 128;
  final int mapNr = 10;
  final int firstLevelNr = 10;
  final int secondLevelNr = 10;
  final int totalNodes = firstLevelNr * secondLevelNr;

  class TestListener implements PropertyChangeListener<ZNRecord>
  {
    Map<String, String> _keySet;

    public TestListener(Map<String, String> keySet)
    {
      _keySet = keySet;
    }

    @Override
    public void onPropertyChange(String key)
    {
      long now = System.currentTimeMillis();
      _keySet.put(key, Long.toString(now));
    }
  }

  private class TestUpdater implements DataUpdater<ZNRecord>
  {
    @Override
    public ZNRecord update(ZNRecord current)
    {
      char[] data = new char[bufSize];

      for (int i = 0; i < bufSize; i++)
      {
        data[i] = 'e';
      }

      Map<String, String> map = new TreeMap<String, String>();
      for (int i = 0; i < mapNr; i++)
      {
        map.put("key_" + i, new String(data));
      }

      String nodeId = current.getId();
      ZNRecord record = new ZNRecord(nodeId);
      record.setSimpleFields(map);
      return record;
    }
  }

  String getNodeId(int i, int j)
  {
    return "childNode_" + i + "_" + j;
  }

  String getSecondLevelKey(int i, int j)
  {
    return "/node_" + i + "/" + getNodeId(i, j);
  }

  String getFirstLevelKey(int i)
  {
    return "/node_" + i;
  }

  // TODO: separate into small tests
  @Test()
  public void testZKPropertyStore() throws Exception
  {
    System.out.println("START " + className + " at "
        + new Date(System.currentTimeMillis()));
    // LOG.info("number of connections is " + ZkClient.getNumberOfConnections());

    // clean up zk
    final String propertyStoreRoot = buildPropertyStoreRoot();
    if (_gZkClient.exists(propertyStoreRoot))
    {
      _gZkClient.deleteRecursive(propertyStoreRoot);
    }

    ZkClient zkclient = new ZkClient(ZK_ADDR);
    zkclient.setZkSerializer(new ByteArraySerializer());
    ZKPropertyStore<ZNRecord> store =
        new ZKPropertyStore<ZNRecord>(zkclient,
                                      new PropertyJsonSerializer<ZNRecord>(ZNRecord.class),
                                      propertyStoreRoot);

    // test back to back add-delete-add

    store.setProperty("child0", new ZNRecord("child0"));

    ZNRecord record2 = store.getProperty("child0"); // will put the record in cache
    String child0Path = propertyStoreRoot + "/child0";
    _gZkClient.subscribeDataChanges(child0Path, new IZkDataListener()
    {
      @Override
      public void handleDataDeleted(String dataPath) throws Exception
      {
        // TODO Auto-generated method stub
        System.out.println("TestZKPropertyStore.testZKPropertyStore().new IZkDataListener() {...}.handleDataDeleted()");
      }

      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception
      {
        // TODO Auto-generated method stub
        System.out.println("TestZKPropertyStore.testZKPropertyStore().new IZkDataListener() {...}.handleDataChange()");
      }
    });
    for (int i = 0; i < 2; i++)
    {
      _gZkClient.delete(child0Path);
      _gZkClient.createPersistent(child0Path, new ZNRecord("child0-new"));
    }
    record2 = store.getProperty("child0");
    Assert.assertEquals(record2.getId(), "child0-new");
    _gZkClient.delete(child0Path);
    Thread.sleep(300); // should wait for zk callback to remove "child0" from cache
    record2 = store.getProperty("child0");
    Assert.assertNull(record2);

    // zookeeper has a default 1M limit on size
    char[] data = new char[bufSize];
    for (int i = 0; i < bufSize; i++)
    {
      data[i] = 'a';
    }

    Map<String, String> map = new TreeMap<String, String>();
    for (int i = 0; i < mapNr; i++)
    {
      map.put("key_" + i, new String(data));
    }
    String node = "node";
    ZNRecord record = new ZNRecord(node);
    record.setSimpleFields(map);

    ZNRecordSerializer serializer = new ZNRecordSerializer();
    int bytesPerNode = serializer.serialize(record).length;
    System.out.println("use znode of size " + bytesPerNode / 1024 + "K");
    Assert.assertTrue(bytesPerNode < 1024 * 1024,
                      "zookeeper has a default 1M limit on size");

    // test getPropertyRootNamespace()
    String root = store.getPropertyRootNamespace();
    Assert.assertEquals(root, propertyStoreRoot);

    // set 100 nodes and get 100 nodes, verify get what we set
    long start = System.currentTimeMillis();
    setNodes(store, 'a', false);
    long end = System.currentTimeMillis();
    System.out.println("ZKPropertyStore write throughput is " + bytesPerNode * totalNodes
        / (end - start) + " kilo-bytes per second");

    start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = getNodeId(i, j);
        String key = getSecondLevelKey(i, j);
        record = store.getProperty(key);
        Assert.assertEquals(record.getId(), nodeId);
      }
    }
    end = System.currentTimeMillis();
    System.out.println("ZKPropertyStore read throughput is " + bytesPerNode * totalNodes
        / (end - start) + " kilo-bytes per second");

    // test subscribe
    Map<String, String> keyMap = new TreeMap<String, String>();
    // verify initial callbacks invoked for all 100 nodes
    PropertyChangeListener<ZNRecord> listener = new TestListener(keyMap);
    store.subscribeForPropertyChange("", listener);
    System.out.println("keyMap size: " + keyMap.size());
    Assert.assertTrue(keyMap.size() > 100);
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);
        Assert.assertTrue(keyMap.containsKey(key));
      }
    }

    // change nodes via property store interface
    // and verify all notifications have been received (TODO: without latency?)
    start = System.currentTimeMillis();
    keyMap.clear();
    setNodes(store, 'b', true);

    // wait for all callbacks completed
    for (int i = 0; i < 10; i++)
    {
      System.out.println("keySet size: " + keyMap.size());
      if (keyMap.size() == totalNodes)
      {
        break;
      }
      Thread.sleep(500);
    }
    Assert.assertEquals(keyMap.size(), totalNodes, "should receive " + totalNodes
        + " callbacks");
    end = System.currentTimeMillis();
    long waitTime = (end - start) * 2;

    long maxLatency = 0;
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);
        Assert.assertTrue(keyMap.containsKey(key));
        record = store.getProperty(key);
        start = Long.parseLong(record.getSimpleField("SetTimestamp"));
        end = Long.parseLong(keyMap.get(key));
        long latency = end - start;
        if (latency > maxLatency)
        {
          maxLatency = latency;
        }
      }
    }
    System.out.println("ZKPropertyStore callback latency is " + maxLatency
        + " millisecond");

    // change nodes via native zkclient interface
    // and verify all notifications have been received with some latency
    keyMap.clear();
    setNodes(_gZkClient, propertyStoreRoot, 'a', true);

    // wait for all callbacks completed
    Thread.sleep(waitTime);
    Assert.assertEquals(keyMap.size(), totalNodes, "should receive " + totalNodes
        + " callbacks");

    // remove node via native zkclient interface
    // should receive callbacks on parent key
    keyMap.clear();
    for (int i = 0; i < firstLevelNr; i++)
    {
      int j = 0;
      String key = getSecondLevelKey(i, j);
      _gZkClient.delete(propertyStoreRoot + key);
    }
    Thread.sleep(waitTime);
    for (int i = 0; i < firstLevelNr; i++)
    {
      String key = getFirstLevelKey(i);
      Assert.assertTrue(keyMap.containsKey(key), "should receive callbacks on " + key);
    }

    keyMap.clear();
    for (int j = 1; j < secondLevelNr; j++)
    {
      int i = 0;
      String key = getSecondLevelKey(i, j);
      _gZkClient.delete(propertyStoreRoot + key);
    }
    Thread.sleep(waitTime);
    String node0Key = getFirstLevelKey(0);
    Assert.assertTrue(keyMap.containsKey(node0Key), "should receive callback on "
        + node0Key);

    // add back removed nodes
    // should receive callbacks on parent key
    keyMap.clear();
    for (int i = 0; i < bufSize; i++)
    {
      data[i] = 'a';
    }

    map = new TreeMap<String, String>();
    for (int i = 0; i < mapNr; i++)
    {
      map.put("key_" + i, new String(data));
    }

    for (int i = 0; i < firstLevelNr; i++)
    {
      int j = 0;
      String nodeId = getNodeId(i, j);
      String key = getSecondLevelKey(i, j);
      record = new ZNRecord(nodeId);
      record.setSimpleFields(map);
      store.setProperty(key, record);
    }
    Thread.sleep(waitTime);
    for (int i = 0; i < firstLevelNr; i++)
    {
      String key = getFirstLevelKey(i);
      Assert.assertTrue(keyMap.containsKey(key), "should receive callbacks on " + key);
    }

    keyMap.clear();
    for (int j = 1; j < secondLevelNr; j++)
    {
      int i = 0;
      String nodeId = getNodeId(i, j);
      String key = getSecondLevelKey(i, j);
      record = new ZNRecord(nodeId);
      record.setSimpleFields(map);
      store.setProperty(key, record);
    }
    Thread.sleep(waitTime);
    node0Key = getFirstLevelKey(0);
    Assert.assertTrue(keyMap.containsKey(node0Key), "should receive callback on "
        + node0Key);

    // test unsubscribe
    store.unsubscribeForPropertyChange("", listener);
    // change all nodes and verify no notification happens
    keyMap.clear();
    setNodes(store, 'c', false);
    Thread.sleep(waitTime);
    Assert.assertEquals(keyMap.size(), 0);

    // test getPropertyNames
    List<String> names = store.getPropertyNames("");
    int cnt = 0;
    for (String name : names)
    {
      int i = cnt / 10;
      int j = cnt % 10;
      cnt++;
      String key = getSecondLevelKey(i, j);
      Assert.assertEquals(name, key);
    }

    // test compare and set
    char[] updateData = new char[bufSize];
    for (int i = 0; i < bufSize; i++)
    {
      data[i] = 'c';
      updateData[i] = 'd';
    }

    Map<String, String> updateMap = new TreeMap<String, String>();
    for (int i = 0; i < 10; i++)
    {
      map.put("key_" + i, new String(data));
      updateMap.put("key_" + i, new String(updateData));
    }

    PropertyJsonComparator<ZNRecord> comparator =
        new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String nodeId = getNodeId(i, j);
        record = new ZNRecord(nodeId);
        record.setSimpleFields(map);
        String key = getSecondLevelKey(i, j);

        ZNRecord update = new ZNRecord(nodeId);
        update.setSimpleFields(updateMap);
        boolean succeed = store.compareAndSet(key, record, update, comparator);
        Assert.assertTrue(succeed);
        record = store.getProperty(key);
        Assert.assertEquals(record.getSimpleField("key_0").charAt(0), 'd');
      }
    }

    // test updateUntilSucceed
    TestUpdater updater = new TestUpdater();
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);

        store.updatePropertyUntilSucceed(key, updater);
        record = store.getProperty(key);
        Assert.assertEquals(record.getSimpleField("key_0").charAt(0), 'e');
      }
    }

    // test exist
    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);

        boolean exist = store.exists(key);
        Assert.assertTrue(exist);
      }
    }

    // test removeProperty
    for (int i = 0; i < firstLevelNr; i++)
    {
      int j = 0;

      String key = getSecondLevelKey(i, j);

      store.removeProperty(key);
      record = store.getProperty(key);
      Assert.assertNull(record);
    }

    // test removePropertyNamespace
    for (int i = 0; i < firstLevelNr; i++)
    {
      String key = "/node_" + i;
      store.removeNamespace(key);
    }

    for (int i = 0; i < firstLevelNr; i++)
    {
      for (int j = 0; j < secondLevelNr; j++)
      {
        String key = getSecondLevelKey(i, j);

        store.removeProperty(key);
        record = store.getProperty(key);
        Assert.assertNull(record);
      }
    }

    System.out.println("END " + className + " at " + new Date(System.currentTimeMillis()));
  }

  private String buildPropertyStoreRoot()
  {
    return "/" + className;
  }

  private void setNodes(ZKPropertyStore<ZNRecord> store, char c, boolean needTimestamp) throws PropertyStoreException
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
        store.setProperty(key, record);
      }
    }
  }

  private void setNodes(ZkClient zkClient, String root, char c, boolean needTimestamp) throws PropertyStoreException
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
        zkClient.writeData(root + key, record);
      }
    }
  }
}
