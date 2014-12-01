package org.apache.helix.manager.zk;

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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.manager.zk.ZkCacheEventThread.ZkCacheEvent;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.zk.ZNode;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

public class ZkCallbackCache<T> extends Cache<T> implements IZkChildListener, IZkDataListener,
    IZkStateListener {
  private static Logger LOG = Logger.getLogger(ZkCallbackCache.class);

  final BaseDataAccessor<T> _accessor;
  final String _chrootPath;

  private final ZkCacheEventThread _eventThread;
  private final Map<String, Set<HelixPropertyListener>> _listener;

  public ZkCallbackCache(BaseDataAccessor<T> accessor, String chrootPath, List<String> paths,
      ZkCacheEventThread eventThread) {
    super();
    _accessor = accessor;
    _chrootPath = chrootPath;

    _listener = new ConcurrentHashMap<String, Set<HelixPropertyListener>>();
    _eventThread = eventThread;

    // init cache
    // System.out.println("init cache: " + paths);
    if (paths != null && !paths.isEmpty()) {
      for (String path : paths) {
        updateRecursive(path);
      }
    }
  }

  @Override
  public void update(String path, T data, Stat stat) {
    String parentPath = HelixUtil.getZkParentPath(path);
    String childName = HelixUtil.getZkName(path);

    addToParentChildSet(parentPath, childName);
    ZNode znode = _cache.get(path);
    if (znode == null) {
      _cache.put(path, new ZNode(path, data, stat));
      fireEvents(path, EventType.NodeCreated);
    } else {
      Stat oldStat = znode.getStat();

      znode.setData(data);
      znode.setStat(stat);
      // System.out.println("\t\t--setData. path: " + path + ", data: " + data);

      if (oldStat.getCzxid() != stat.getCzxid()) {
        fireEvents(path, EventType.NodeDeleted);
        fireEvents(path, EventType.NodeCreated);
      } else if (oldStat.getVersion() != stat.getVersion()) {
        // System.out.println("\t--fireNodeChanged: " + path + ", oldVersion: " +
        // oldStat.getVersion() + ", newVersion: " + stat.getVersion());
        fireEvents(path, EventType.NodeDataChanged);
      }
    }
  }

  // TODO: make readData async
  @Override
  public void updateRecursive(String path) {
    if (path == null) {
      return;
    }

    try {
      _lock.writeLock().lock();
      try {
        // subscribe changes before read
        _accessor.subscribeDataChanges(path, this);

        // update this node
        Stat stat = new Stat();
        T readData = _accessor.get(path, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);

        update(path, readData, stat);
      } catch (ZkNoNodeException e) {
        // OK. znode not exists
        // we still need to subscribe child change
      }

      // recursively update children nodes if not exists
      // System.out.println("subcribeChildChange: " + path);
      ZNode znode = _cache.get(path);
      List<String> childNames = _accessor.subscribeChildChanges(path, this);
      if (childNames != null && !childNames.isEmpty()) {
        for (String childName : childNames) {
          if (!znode.hasChild(childName)) {
            String childPath = path + "/" + childName;
            znode.addChild(childName);
            updateRecursive(childPath);
          }
        }
      }
    } finally {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
    // System.out.println("handleChildChange: " + parentPath + ", " + currentChilds);

    // this is invoked if subscribed for childChange and node gets deleted
    if (currentChilds == null) {
      return;
    }

    updateRecursive(parentPath);
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {
    // System.out.println("handleDataChange: " + dataPath);
    try {
      _lock.writeLock().lock();

      // TODO: optimize it by get stat from callback
      Stat stat = new Stat();
      Object readData = _accessor.get(dataPath, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);

      ZNode znode = _cache.get(dataPath);
      if (znode != null) {
        Stat oldStat = znode.getStat();

        // System.out.println("handleDataChange: " + dataPath + ", data: " + data);
        // System.out.println("handleDataChange: " + dataPath + ", oldCzxid: " +
        // oldStat.getCzxid() + ", newCzxid: " + stat.getCzxid()
        // + ", oldVersion: " + oldStat.getVersion() + ", newVersion: " +
        // stat.getVersion());
        znode.setData(readData);
        znode.setStat(stat);

        // if create right after delete, and zkCallback comes after create
        // no DataDelete() will be fired, instead will fire 2 DataChange()
        // see ZkClient.fireDataChangedEvents()
        if (oldStat.getCzxid() != stat.getCzxid()) {
          fireEvents(dataPath, EventType.NodeDeleted);
          fireEvents(dataPath, EventType.NodeCreated);
        } else if (oldStat.getVersion() != stat.getVersion()) {
          // System.out.println("\t--fireNodeChanged: " + dataPath + ", oldVersion: " +
          // oldStat.getVersion() + ", newVersion: " + stat.getVersion());
          fireEvents(dataPath, EventType.NodeDataChanged);
        }
      } else {
        // we may see dataChange on child before childChange on parent
        // in this case, let childChange update cache
      }
    } finally {
      _lock.writeLock().unlock();
    }

  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
    // System.out.println("handleDataDeleted: " + dataPath);

    try {
      _lock.writeLock().lock();
      _accessor.unsubscribeDataChanges(dataPath, this);
      _accessor.unsubscribeChildChanges(dataPath, this);

      String parentPath = HelixUtil.getZkParentPath(dataPath);
      String name = HelixUtil.getZkName(dataPath);
      removeFromParentChildSet(parentPath, name);
      _cache.remove(dataPath);

      fireEvents(dataPath, EventType.NodeDeleted);
    } finally {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleStateChanged(KeeperState state) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void handleNewSession() throws Exception {
    // TODO Auto-generated method stub

  }

  public void subscribe(String path, HelixPropertyListener listener) {
    synchronized (_listener) {
      Set<HelixPropertyListener> listeners = _listener.get(path);
      if (listeners == null) {
        listeners = new CopyOnWriteArraySet<HelixPropertyListener>();
        _listener.put(path, listeners);
      }
      listeners.add(listener);
    }
  }

  public void unsubscribe(String path, HelixPropertyListener childListener) {
    synchronized (_listener) {
      final Set<HelixPropertyListener> listeners = _listener.get(path);
      if (listeners != null) {
        listeners.remove(childListener);
      }
    }
  }

  private void fireEvents(final String path, EventType type) {
    String tmpPath = path;
    final String clientPath =
        (_chrootPath == null ? path : (_chrootPath.equals(path) ? "/" : path.substring(_chrootPath
            .length())));

    while (tmpPath != null) {
      Set<HelixPropertyListener> listeners = _listener.get(tmpPath);

      if (listeners != null && !listeners.isEmpty()) {
        for (final HelixPropertyListener listener : listeners) {
          try {
            switch (type) {
            case NodeDataChanged:
              // listener.onDataChange(path);
              _eventThread.send(new ZkCacheEvent("dataChange on " + path + " send to " + listener) {
                @Override
                public void run() throws Exception {
                  listener.onDataChange(clientPath);
                }
              });
              break;
            case NodeCreated:
              // listener.onDataCreate(path);
              _eventThread.send(new ZkCacheEvent("dataCreate on " + path + " send to " + listener) {
                @Override
                public void run() throws Exception {
                  listener.onDataCreate(clientPath);
                }
              });
              break;
            case NodeDeleted:
              // listener.onDataDelete(path);
              _eventThread.send(new ZkCacheEvent("dataDelete on " + path + " send to " + listener) {
                @Override
                public void run() throws Exception {
                  listener.onDataDelete(clientPath);
                }
              });
              break;
            default:
              break;
            }
          } catch (Exception e) {
            LOG.error("Exception in handle events.", e);
          }
        }
      }

      tmpPath = HelixUtil.getZkParentPath(tmpPath);
    }
  }

}
