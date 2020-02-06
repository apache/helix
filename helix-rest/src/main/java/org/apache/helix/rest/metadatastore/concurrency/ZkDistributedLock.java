package org.apache.helix.rest.metadatastore.concurrency;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.I0Itec.zkclient.IZkChildListener;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.rest.metadatastore.exceptions.RoutingDataLockException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;


public class ZkDistributedLock implements IZkChildListener, RoutingDataLock {
  private final HelixZkClient _zkClient;
  private final String _lockBasePath;
  private final String _lockName;
  private String _lockPath;
  private final Object _lock = new Object();

  public ZkDistributedLock(HelixZkClient zkClient, String lockBasePath, String lockName) {
    _zkClient = zkClient;
    _lockBasePath = lockBasePath;
    _lockName = lockName;
  }

  public void lock()
      throws RoutingDataLockException {
    try {
      // lockPath will be different than (lockBasePath + "/" + lockName)
      // because of the sequence number ZooKeeper appends (ephemeral sequential)
      _lockPath = _zkClient
          .create(_lockBasePath + "/" + _lockName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.EPHEMERAL_SEQUENTIAL);
      synchronized (_lock) {
        while (true) {
          // TODO: Can optimize this by making it only be triggered on Event.EventType.NodeDeleted
          _zkClient.subscribeChildChanges(_lockBasePath, this);
          List<String> nodes = _zkClient.getChildren(_lockBasePath);
          Collections.sort(nodes); // ZNode names can be sorted lexicographically
          if (_lockPath.endsWith(nodes.get(0))) {
            return;
          } else {
            _lock.wait();
          }
        }
      }
    } catch (InterruptedException e) {
      throw new RoutingDataLockException(e);
    }
  }

  public void unlock() {
    _zkClient.delete(_lockPath);
    _lockPath = null;
  }

  /**
   * Let ZooKeeper notify the lock of any unlock operations.
   * @param s
   * @param list
   * @throws Exception
   */
  @Override
  public void handleChildChange(String s, List<String> list)
      throws Exception {
    synchronized (_lock) {
      _lock.notifyAll();
    }
  }
}

