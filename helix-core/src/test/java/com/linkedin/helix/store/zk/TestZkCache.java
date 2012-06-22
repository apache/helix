package com.linkedin.helix.store.zk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZkClient;

public class TestZkCache extends ZkUnitTestBase
{
  final String zkAddr = ZK_ADDR;
  ZkClient client;
  
  @BeforeClass
  public void beforeClass()
  {
    client = new ZkClient(zkAddr, 30000, 10000, new ZkSerializer()
    {
      
      @Override
      public byte[] serialize(Object data) throws ZkMarshallingError
      {
        return (byte[])data;
      }
      
      @Override
      public Object deserialize(byte[] bytes) throws ZkMarshallingError
      {
        return bytes;
      }
    });
  }
  
  // create 10, modify 10, and delete 10, verify that all callbacks are received
  @Test
  public void testCallback() throws Exception
  {
    String testName = "TestZkCache-callback";
    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    
    String root = "/" + testName;
    client.deleteRecursive(root);
    client.createPersistent(root);

    final List<String> createCallbacks = new ArrayList<String>();
    final List<String> deleteCallbacks = new ArrayList<String>();
    final List<String> updateCallbacks = new ArrayList<String>();

    
    ZkCache cache = new ZkCache(root, client, new ZkListener()
    {
      @Override
      public void handleNodeDelete(String path)
      {
        System.out.println("handleNodeDelete: " + path);
        deleteCallbacks.add(path);
      }
      
      @Override
      public void handleNodeCreate(String path)
      {
        System.out.println("handleNodeCreate: " + path);
        createCallbacks.add(path);
      }
      
      @Override
      public void handleDataChange(String path)
      {
        System.out.println("handleDataChange: " + path);
        updateCallbacks.add(path);
      }
    });
    
    cache.updateCache(root);
    Assert.assertTrue(createCallbacks.equals(Arrays.asList(root)), "Should have " + root + " in cache");
    createCallbacks.clear();
    
    // create
    List<String> createKeys = randomCreateByZkClient(10, root, client);
    // wait for zk callbacks to complete
    Thread.sleep(2000);
    Collections.sort(createKeys);
    Collections.sort(createCallbacks);
    if (!createKeys.equals(createCallbacks))
    {
      // debug
      System.out.println("createKeys: " + createKeys);
      System.out.println("createCallback: " + createCallbacks);
    }
    Assert.assertTrue(createKeys.equals(createCallbacks), "Should have all path create-callback, but not necessarily in the same order they created");
    
    // update
    List<String> updateKeys = new ArrayList<String>();
    dfUpdateZk(updateKeys, client, root);
    Thread.sleep(2000);
    if (!updateKeys.equals(updateCallbacks))
    {
      // debug
      System.out.println("updateKeys: " + updateKeys);
      System.out.println("updateCallbacks: " + updateCallbacks);
    }
    Assert.assertTrue(updateKeys.equals(updateCallbacks), "Should have all paths datachange-callbacks in depth first order");
    
    
    // delete
    List<String> deleteKeys = new ArrayList<String>();
    dfGetZkPaths(deleteKeys, client, root);
    client.deleteRecursive(root);
    Thread.sleep(2000);
    if (!deleteKeys.equals(deleteCallbacks))
    {
      // debug
      System.out.println("deleteKeys: " + deleteKeys);
      System.out.println("deleteCallbacks: " + deleteCallbacks);
    }
    Assert.assertTrue(deleteKeys.equals(deleteCallbacks), "Should have all paths delete-callbacks in depth first order");
    
    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }
  
  // return the keys created
  static List<String> randomCreateByZkClient(int number, String root, ZkClient client)
  {
    int count = 0;
    int maxDepth = 10;
    String delim = "/";
    List<String> createKeys = new ArrayList<String>();
    
    while (createKeys.size() < number)
    {
      int depth = ((int) (Math.random() * 10000)) % maxDepth + 1;
      StringBuilder sb = new StringBuilder(root);
      for (int i = 0; i < depth; i++)
      {
        int childId = ((int) (Math.random() * 10000)) % 5;
        sb.append(delim).append("child-" + childId);
      }
      String key = sb.toString();
      byte[] val = key.getBytes();

      String keyToCreate = key;
      while (keyToCreate.startsWith(root))
      {
        if (client.exists(keyToCreate))
        {
          break;
        }
//        changes.add(keyToCreate + "-" + "create" + "-" + System.currentTimeMillis());
        System.out.println("Creating key: " + keyToCreate);
        createKeys.add(keyToCreate);
        keyToCreate = keyToCreate.substring(0, keyToCreate.lastIndexOf('/'));
      }

      client.createPersistent(key, true);

      System.out.println("Writing key: " + key);
      client.writeData(key, val);
      count = count + 1;
//      changes.add(key + "-" + "write" + "-" + System.currentTimeMillis());
    }
    return createKeys;
  }
  
//  // depth first read from zk
//  static void dfReadZk(Map<String, ZNode> map, ZkClient client, String root)
//  {
//    List<String> childs = client.getChildren(root);
//    if (childs != null)
//    {
//      for (String child : childs)
//      {
//        String childPath = root + "/" + child;
//        dfReadZk(map, client, childPath);
//      }
//      Stat stat = new Stat();
//      String value = client.readData(root, stat);
//      ZNode node = new ZNode(root, value, stat);
//      node._childSet.addAll(childs);
//      map.put(root, node);
//    }
//  }
  
  // depth first read paths from zk
  static void dfGetZkPaths(List<String> deletePaths, ZkClient client, String root)
  {
    List<String> childs = client.getChildren(root);
    if (childs != null)
    {
      for (String child : childs)
      {
        String childPath = root + "/" + child;
        dfGetZkPaths(deletePaths, client, childPath);
      }
      deletePaths.add(root);
    }
  }
  
  static void dfUpdateZk(List<String> updatePaths, ZkClient client, String root)
  {
    List<String> childs = client.getChildren(root);
    if (childs != null)
    {
      for (String child : childs)
      {
        String childPath = root + "/" + child;
        dfUpdateZk(updatePaths, client, childPath);
      }
      updatePaths.add(root);
      byte[] data = client.readData(root);

      String updateStr = (data != null ? new String(data) : root) + "-updated";
      client.writeData(root, updateStr.getBytes());
    }
  }
  
  
}
