/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.zookeeper.ZooKeeper.States;


public class ZKClientPool
{
  static final Map<String, ZkClient> _zkClientMap = new ConcurrentHashMap<String, ZkClient>();
  static final int DEFAULT_SESSION_TIMEOUT = 30 * 1000;

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
        ZkClient zkClient = new ZkClient(zkServer, DEFAULT_SESSION_TIMEOUT, 
            ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());

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
