package com.linkedin.clustermanager.store.zk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.ZkConnection;

import com.linkedin.clustermanager.store.PropertySerializer;

public class ZKConnectionFactory
{

  private static Map<String, ZkConnection> _zkConnectionMap = new ConcurrentHashMap<String, ZkConnection>();

  // TODO can't reuse shared connection via ZkClient()
  //  can reuse connections via ZooKeeper(), enable it later if useful
  private static <T extends Object> ZkConnection create(String zkServers, PropertySerializer<T> serializer)
  {
    String key = zkServers + "/" + serializer.toString();

    if(_zkConnectionMap.containsKey(key))
    {
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
