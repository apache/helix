package com.linkedin.helix.store.zk;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyChangeListener;
import com.linkedin.helix.store.PropertyJsonComparator;
import com.linkedin.helix.store.PropertyJsonSerializer;
import com.linkedin.helix.store.PropertyStoreException;

// TODO need to write performance test for zk-property store
public class TestZKPropertyStore extends ZkUnitTestBase
{
  private static final Logger LOG = Logger.getLogger(TestZKPropertyStore.class);
  final String className = getShortClassName();

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
      char[] data1k = new char[1024];

      for (int i = 0; i < 1024; i++)
      {
        data1k[i] = 'e';
      }

      Map<String, String> map1m = new TreeMap<String, String>();
      for (int i = 0; i < 1000; i++)
      {
        map1m.put("key_" + i, new String(data1k));
      }

      String nodeId = current.getId();
      ZNRecord record = new ZNRecord(nodeId);
      record.setSimpleFields(map1m);
      return record;
    }
  }

  @Test ()
  public void testZKPropertyStore() throws Exception
  {
  	System.out.println("START " + className + " at " + new Date(System.currentTimeMillis()));
    // LOG.info("number of connections is " + ZkClient.getNumberOfConnections());

    // clean up zk
    final String propertyStoreRoot = "/" + className;
    if (_gZkClient.exists(propertyStoreRoot))
    {
      _gZkClient.deleteRecursive(propertyStoreRoot);
    }

    ZKPropertyStore<ZNRecord> store = new ZKPropertyStore<ZNRecord>(new ZkClient(ZK_ADDR),
        new PropertyJsonSerializer<ZNRecord>(ZNRecord.class), propertyStoreRoot);

    // zookeeper has a default limit on znode size which is 1M
    char[] data1k = new char[1024];
    for (int i = 0; i < 1024; i++)
    {
      data1k[i] = 'a';
    }

    Map<String, String> map1m = new TreeMap<String, String>();
    for (int i = 0; i < 10; i++)
    {
      map1m.put("key_" + i, new String(data1k));
    }
    String node_0 = "node_0";
    ZNRecord record = new ZNRecord(node_0);
    record.setSimpleFields(map1m);

    ZNRecordSerializer serializer = new ZNRecordSerializer();
    int bytesNb = serializer.serialize(record).length;
    System.out.println("use znode of size " + bytesNb/1024 + "K");
    Assert.assertTrue(bytesNb < 1024 * 1024);

    // test getPropertyRootNamespace()
    String root = store.getPropertyRootNamespace();
    Assert.assertEquals(root, propertyStoreRoot);

    // set 100 nodes and get 100 nodes, each of which are ~10K.
    // verify get what are set
    long start = System.currentTimeMillis();
    set100Nodes(store, 'a', false);
    long end = System.currentTimeMillis();
    System.out.println("ZKPropertyStore write throughput is " + bytesNb * 100 / (end-start) + " kilo-bytes per second");

    start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = "childNode_" + i + "_" + j;
        String key = "/node_" + i + "/" + nodeId;
        record = store.getProperty(key);
        Assert.assertEquals(record.getId(), nodeId);
      }
    }
    end = System.currentTimeMillis();
    System.out.println("ZKPropertyStore read throughput is " + bytesNb * 100 / (end-start) + " kilo-bytes per second");


    // test subscribe
    Map<String, String> keyMap = new TreeMap<String, String>();
    // verify initial callbacks invoked for all 100 nodes
    PropertyChangeListener<ZNRecord> listener = new TestListener(keyMap);
    store.subscribeForPropertyChange("", listener);
    System.out.println("keyMap size: " + keyMap.size());
    Assert.assertEquals(keyMap.size(), 100);
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = "childNode_" + i + "_" + j;
        String key = "/node_" + i + "/" + nodeId;
        Assert.assertTrue(keyMap.containsKey(key));
      }
    }

    // change 100 nodes and verify all notifications have been received
    keyMap.clear();
    set100Nodes(store, 'b', true);

    // wait for all callbacks completed
    for (int i = 0; i < 10; i++)
    {
      System.out.println("keySet size: " + keyMap.size());
      if (keyMap.size() == 100)
      {
        break;
      }
      Thread.sleep(500);
    }
    Assert.assertEquals(keyMap.size(), 100);

    long maxLatency = 0;
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = "childNode_" + i + "_" + j;
        String key = "/node_" + i + "/" + nodeId;
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
    System.out.println("ZKPropertyStore callback latency is " + maxLatency + " millisecond");

    // test unsubscribe
    store.unsubscribeForPropertyChange("", listener);
    // change all nodes and verify no notification happens
    keyMap.clear();
    set100Nodes(store, 'c', false);
    Thread.sleep(maxLatency);
    Assert.assertEquals(keyMap.size(), 0);

    // test getPropertyNames
    List<String> names = store.getPropertyNames("");
    int cnt = 0;
    for (String name : names)
    {
      int i = cnt / 10;
      int j = cnt % 10;
      cnt++;
      String nodeId = "childNode_" + i + "_" + j;
      String key = "/node_" + i + "/" + nodeId;
      Assert.assertEquals(name, key);
    }

    // test compare and set
    char[] data1kUpdate = new char[1024];
    for (int i = 0; i < 1024; i++)
    {
      data1k[i] = 'c';
      data1kUpdate[i] = 'd';
    }

    Map<String, String> map1mUpdate = new TreeMap<String, String>();
    for (int i = 0; i < 1000; i++)
    {
      map1m.put("key_" + i, new String(data1k));
      map1mUpdate.put("key_" + i, new String(data1kUpdate));
    }

    PropertyJsonComparator<ZNRecord> comparator = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = "childNode_" + i + "_" + j;
        record = new ZNRecord(nodeId);
        record.setSimpleFields(map1m);
        String key = "/node_" + i + "/" + nodeId;

        ZNRecord update = new ZNRecord(nodeId);
        update.setSimpleFields(map1mUpdate);
        boolean succeed = store.compareAndSet(key, record, update, comparator);
        Assert.assertTrue(succeed);
        record = store.getProperty(key);
        Assert.assertEquals(record.getSimpleField("key_0").charAt(0), 'd');
      }
    }

    // test updateUntilSucceed
    TestUpdater updater = new TestUpdater();
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = "childNode_" + i + "_" + j;
        String key = "/node_" + i + "/" + nodeId;

        store.updatePropertyUntilSucceed(key, updater);
        record = store.getProperty(key);
        Assert.assertEquals(record.getSimpleField("key_0").charAt(0), 'e');
      }
    }

    // test exist
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = "childNode_" + i + "_" + j;
        String key = "/node_" + i + "/" + nodeId;

        boolean exist = store.exists(key);
        Assert.assertTrue(exist);
      }
    }

    // test removeProperty
    for (int i = 0; i < 10; i++)
    {
      int j = 0;

      String nodeId = "childNode_" + i + "_" + j;
      String key = "/node_" + i + "/" + nodeId;

      store.removeProperty(key);
      record = store.getProperty(key);
      Assert.assertNull(record);
    }

    // test removePropertyNamespace
    for (int i = 0; i < 10; i++)
    {
      String key = "/node_" + i;
      store.removeNamespace(key);
    }
    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = "childNode_" + i + "_" + j;
        String key = "/node_" + i + "/" + nodeId;

        store.removeProperty(key);
        record = store.getProperty(key);
        Assert.assertNull(record);
      }
    }

    // misc tests
    // subscribe a znode, remove it, and add it back
    store.subscribeForPropertyChange("", listener);
    keyMap.clear();
    store.createPropertyNamespace("/node_0");
    Thread.sleep(maxLatency);
    Assert.assertTrue(keyMap.containsKey("/node_0"));
    keyMap.clear();
    store.setProperty("/node_0/childNode_0_0", record);
    Thread.sleep(maxLatency);
    Assert.assertTrue(keyMap.containsKey("/node_0/childNode_0_0"));
    keyMap.clear();
    store.removeProperty("/node_0/childNode_0_0");
    Thread.sleep(maxLatency);
    Assert.assertTrue(keyMap.containsKey("/node_0"));
    keyMap.clear();
    store.setProperty("/node_0/childNode_0_0", record);
    Thread.sleep(maxLatency);
    Assert.assertTrue(keyMap.containsKey("/node_0/childNode_0_0"));

    System.out.println("END " + className + " at " + new Date(System.currentTimeMillis()));
    // TODO fix getNbOfConnections()
    // LOG.info("number of connections is " + ZkClient.getNumberOfConnections());
  }

  private void set100Nodes(ZKPropertyStore<ZNRecord> store, char c, boolean needTimestamp) throws PropertyStoreException
  {
    char[] data1k = new char[1024];

    for (int i = 0; i < 1024; i++)
    {
      data1k[i] = c;
    }

    Map<String, String> map1m = new TreeMap<String, String>();
    for (int i = 0; i < 1000; i++)
    {
      map1m.put("key_" + i, new String(data1k));
    }

    for (int i = 0; i < 10; i++)
    {
      for (int j = 0; j < 10; j++)
      {
        String nodeId = "childNode_" + i + "_" + j;
        ZNRecord record = new ZNRecord(nodeId);
        record.setSimpleFields(map1m);
        if (needTimestamp)
        {
          long now = System.currentTimeMillis();
          record.setSimpleField("SetTimestamp", Long.toString(now));
        }
        String key = "/node_" + i + "/" + nodeId;
        store.setProperty(key, record);
      }
    }
  }
}
