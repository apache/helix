package com.linkedin.helix.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.ZooKeeper.States;

import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;

public class ZKClientPool
{
  static final Map<String, ZkClient> _zkClientMap = new ConcurrentHashMap<String, ZkClient>();

  public static ZkClient getZkClient(String zkServer)
  {
    // happy path that we cache the zkclient and it's still connected
    if (_zkClientMap.containsKey(zkServer))
    {
      ZkClient zkClient = _zkClientMap.get(zkServer);
      if (zkClient.getConnection().getZookeeperState() == States.CONNECTED)
      {
        return zkClient;
      }
    }

    synchronized (_zkClientMap)
    {
      // if we cache a stale zkclient, purge it
      if (_zkClientMap.containsKey(zkServer))
      {
        ZkClient zkClient = _zkClientMap.get(zkServer);
        if (zkClient.getConnection().getZookeeperState() != States.CONNECTED)
        {
          _zkClientMap.remove(zkServer);
        }
      }

      // get a new zkclient
      if (!_zkClientMap.containsKey(zkServer))
      {
        ZkClient zkClient = new ZkClient(zkServer);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        _zkClientMap.put(zkServer, zkClient);
      }
      return _zkClientMap.get(zkServer);
    }
  }

  public static void reset()
  {
    _zkClientMap.clear();
  }
}
