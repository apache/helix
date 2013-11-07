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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.helix.store.zk.ZNode;
import org.apache.helix.util.HelixUtil;
import org.apache.zookeeper.data.Stat;

public abstract class Cache<T> {
  final ReadWriteLock _lock;
  final ConcurrentHashMap<String, ZNode> _cache;

  public Cache() {
    _lock = new ReentrantReadWriteLock();
    _cache = new ConcurrentHashMap<String, ZNode>();
  }

  public void addToParentChildSet(String parentPath, String childName) {
    ZNode znode = _cache.get(parentPath);
    if (znode != null) {
      znode.addChild(childName);
    }
  }

  public void addToParentChildSet(String parentPath, List<String> childNames) {
    if (childNames != null && !childNames.isEmpty()) {
      ZNode znode = _cache.get(parentPath);
      if (znode != null) {
        znode.addChildren(childNames);
      }
    }
  }

  public void removeFromParentChildSet(String parentPath, String name) {
    ZNode zNode = _cache.get(parentPath);
    if (zNode != null) {
      zNode.removeChild(name);
    }
  }

  public boolean exists(String path) {
    return _cache.containsKey(path);
  }

  public ZNode get(String path) {
    try {
      _lock.readLock().lock();
      return _cache.get(path);
    } finally {
      _lock.readLock().unlock();
    }
  }

  public void lockWrite() {
    _lock.writeLock().lock();
  }

  public void unlockWrite() {
    _lock.writeLock().unlock();
  }

  public void lockRead() {
    _lock.readLock().lock();
  }

  public void unlockRead() {
    _lock.readLock().unlock();
  }

  public void purgeRecursive(String path) {
    try {
      _lock.writeLock().lock();

      String parentPath = HelixUtil.getZkParentPath(path);
      String name = HelixUtil.getZkName(path);
      removeFromParentChildSet(parentPath, name);

      ZNode znode = _cache.remove(path);
      if (znode != null) {
        // recursively remove children nodes
        Set<String> childNames = znode.getChildSet();
        for (String childName : childNames) {
          String childPath = path + "/" + childName;
          purgeRecursive(childPath);
        }
      }
    } finally {
      _lock.writeLock().unlock();
    }
  }

  public void reset() {
    try {
      _lock.writeLock().lock();
      _cache.clear();
    } finally {
      _lock.writeLock().unlock();
    }
  }

  public abstract void update(String path, T data, Stat stat);

  public abstract void updateRecursive(String path);

  // debug
  public Map<String, ZNode> getCache() {
    return _cache;
  }

}
