package com.linkedin.clustermanager.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.ZkClient;

import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;

public class ZKClientPool
{
  static Map<String, ZkClient> _zkClientMap = new ConcurrentHashMap<String, ZkClient>();
  
  public static ZkClient getZkClient(String zkServer)
  {
    if(_zkClientMap.containsKey(zkServer))
    {
      // TODO: if the ZKClient is eventually disconnected, we should get notified and remove it
      // from the map.
      return _zkClientMap.get(zkServer);
    }
    else
    {
      synchronized(_zkClientMap)
      {
        if(!_zkClientMap.containsKey(zkServer))
        {
          ZkClient zkClient = new ZkClient(zkServer);
          zkClient.setZkSerializer(new ZNRecordSerializer());
          _zkClientMap.put(zkServer, zkClient);
        }
        return _zkClientMap.get(zkServer);
      }
    }
  }
}
