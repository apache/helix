package com.linkedin.clustermanager.store.zk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.ZkConnection;

import com.linkedin.clustermanager.store.PropertySerializer;

public class ZKConnectionFactory
{

  private static Map<String, ZkConnection> _zkConnectionMap = new ConcurrentHashMap<String, ZkConnection>();

  public static <T extends Object> ZkConnection create(String zkServers, PropertySerializer<T> serializer)
  {
    
    // String key = zkServers + "/" + serializer.getClass().getName();
    String key = zkServers + "/" + serializer.toString();

    if(_zkConnectionMap.containsKey(key))
    {
      // TODO: if the ZKClient is eventually disconnected, 
      // we should get notified and remove the zkClient from the map.
      return _zkConnectionMap.get(key);
    }
    else
    {
      synchronized(_zkConnectionMap)
      {
        if(!_zkConnectionMap.containsKey(key))
        {
          ZkConnection zkConnection = new ZkConnection(zkServers);
          // zkClient.setZkSerializer(new ZNRecordSerializer());
          _zkConnectionMap.put(key, zkConnection);
        }
        
        return _zkConnectionMap.get(key);
      }
    }
    
  }
}
