package com.linkedin.clustermanager.store.zk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.ZkClient;

import com.linkedin.clustermanager.store.PropertySerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;

public class ZKClientFactory
{
  private static Map<String, ZkClient> _zkClientMap = new ConcurrentHashMap<String, ZkClient>();

  public static <T extends Object> ZkClient create(String zkServers, PropertySerializer<T> serializer)
  {
    
    // String key = zkServers + "/" + serializer.getClass().getName();
    String key = zkServers + "/" + serializer.toString();

    if(_zkClientMap.containsKey(key))
    {
      // TODO: if the ZKClient is eventually disconnected, 
      // we should get notified and remove the zkClient from the map.
      return _zkClientMap.get(key);
    }
    else
    {
      synchronized(_zkClientMap)
      {
        if(!_zkClientMap.containsKey(key))
        {
          ZkClient zkClient = new ZkClient(zkServers);
          // zkClient.setZkSerializer(new ZNRecordSerializer());
          _zkClientMap.put(key, zkClient);
        }
        
        return _zkClientMap.get(key);
      }
    }
    
  }

  public static void main(String[] args) throws Exception
  {
    String zkServers = "localhost:2190";
    PropertySerializer<String> serializer = new PropertySerializer<String>() {

      @Override
      public byte[] serialize(String data) throws PropertyStoreException
      {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String deserialize(byte[] bytes) throws PropertyStoreException
      {
        // TODO Auto-generated method stub
        return null;
      }
      
    };
    
    ZkClient zkClient = ZKClientFactory.<String>create(zkServers, serializer);
    ZkClient zkClient2 = ZKClientFactory.<String>create(zkServers, serializer);

    String testRoot = "/testPath1/testPath2";
    zkClient.createPersistent(testRoot, true);
    zkClient.writeData(testRoot, "testData2");
    
    testRoot = "/testPath1/testPath3";
    zkClient.createPersistent(testRoot, true);
    zkClient.writeData(testRoot, "testData3");
    
    
  }
}
