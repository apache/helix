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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor.RetCode;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZNode;
import org.apache.helix.util.PathUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;

public class ZkCacheBaseDataAccessor<T> implements HelixPropertyStore<T> {
  private static final Logger LOG = Logger.getLogger(ZkCacheBaseDataAccessor.class);

  protected WriteThroughCache<T> _wtCache;
  protected ZkCallbackCache<T> _zkCache;

  final ZkBaseDataAccessor<T> _baseAccessor;
  final Map<String, Cache<T>> _cacheMap;

  final String _chrootPath;
  final List<String> _wtCachePaths;
  final List<String> _zkCachePaths;

  final HelixGroupCommit<T> _groupCommit = new HelixGroupCommit<T>();

  // fire listeners
  private final ReentrantLock _eventLock = new ReentrantLock();
  private ZkCacheEventThread _eventThread;

  private ZkClient _zkclient = null;

  public ZkCacheBaseDataAccessor(ZkBaseDataAccessor<T> baseAccessor, List<String> wtCachePaths) {
    this(baseAccessor, null, wtCachePaths, null);
  }

  public ZkCacheBaseDataAccessor(ZkBaseDataAccessor<T> baseAccessor, String chrootPath,
      List<String> wtCachePaths, List<String> zkCachePaths) {
    _baseAccessor = baseAccessor;

    if (chrootPath == null || chrootPath.equals("/")) {
      _chrootPath = null;
    } else {
      PathUtils.validatePath(chrootPath);
      _chrootPath = chrootPath;
    }

    _wtCachePaths = wtCachePaths;
    _zkCachePaths = zkCachePaths;

    // TODO: need to make sure no overlap between wtCachePaths and zkCachePaths
    // TreeMap key is ordered by key string length, so more general (i.e. short) prefix
    // comes first
    _cacheMap = new TreeMap<String, Cache<T>>(new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        int len1 = o1.split("/").length;
        int len2 = o2.split("/").length;
        return len1 - len2;
      }
    });

    start();
  }

  public ZkCacheBaseDataAccessor(String zkAddress, ZkSerializer serializer, String chrootPath,
      List<String> wtCachePaths, List<String> zkCachePaths) {
    _zkclient =
        new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT, serializer);
    _zkclient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    _baseAccessor = new ZkBaseDataAccessor<T>(_zkclient);

    if (chrootPath == null || chrootPath.equals("/")) {
      _chrootPath = null;
    } else {
      PathUtils.validatePath(chrootPath);
      _chrootPath = chrootPath;
    }

    _wtCachePaths = wtCachePaths;
    _zkCachePaths = zkCachePaths;

    // TODO: need to make sure no overlap between wtCachePaths and zkCachePaths
    // TreeMap key is ordered by key string length, so more general (i.e. short) prefix
    // comes first
    _cacheMap = new TreeMap<String, Cache<T>>(new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        int len1 = o1.split("/").length;
        int len2 = o2.split("/").length;
        return len1 - len2;
      }
    });

    start();
  }

  private String prependChroot(String clientPath) {
    PathUtils.validatePath(clientPath);

    if (_chrootPath != null) {
      // handle clientPath = "/"
      if (clientPath.length() == 1) {
        return _chrootPath;
      }
      return _chrootPath + clientPath;
    } else {
      return clientPath;
    }
  }

  private List<String> prependChroot(List<String> clientPaths) {
    List<String> serverPaths = new ArrayList<String>();
    for (String clientPath : clientPaths) {
      serverPaths.add(prependChroot(clientPath));
    }
    return serverPaths;
  }

  /**
   * find the first path in paths that is a descendant
   */
  private String firstCachePath(List<String> paths) {
    for (String cachePath : _cacheMap.keySet()) {
      for (String path : paths) {
        if (path.startsWith(cachePath)) {
          return path;
        }
      }
    }
    return null;
  }

  private Cache<T> getCache(String path) {
    for (String cachePath : _cacheMap.keySet()) {
      if (path.startsWith(cachePath)) {
        return _cacheMap.get(cachePath);
      }
    }

    return null;
  }

  private Cache<T> getCache(List<String> paths) {
    Cache<T> cache = null;
    for (String path : paths) {
      for (String cachePath : _cacheMap.keySet()) {
        if (cache == null && path.startsWith(cachePath)) {
          cache = _cacheMap.get(cachePath);
        } else if (cache != null && cache != _cacheMap.get(cachePath)) {
          throw new IllegalArgumentException("Couldn't do cross-cache async operations. paths: "
              + paths);
        }
      }
    }

    return cache;
  }

  private void updateCache(Cache<T> cache, List<String> createPaths, boolean success,
      String updatePath, T data, Stat stat) {
    if (createPaths == null || createPaths.isEmpty()) {
      if (success) {
        cache.update(updatePath, data, stat);
      }
    } else {
      String firstPath = firstCachePath(createPaths);
      if (firstPath != null) {
        cache.updateRecursive(firstPath);
      }
    }
  }

  @Override
  public boolean create(String path, T data, int options) {
    String clientPath = path;
    String serverPath = prependChroot(clientPath);

    Cache<T> cache = getCache(serverPath);
    if (cache != null) {
      try {
        cache.lockWrite();
        ZkBaseDataAccessor<T>.AccessResult result =
            _baseAccessor.doCreate(serverPath, data, options);
        boolean success = (result._retCode == RetCode.OK);

        updateCache(cache, result._pathCreated, success, serverPath, data, ZNode.ZERO_STAT);

        return success;
      } finally {
        cache.unlockWrite();
      }
    }

    // no cache
    return _baseAccessor.create(serverPath, data, options);
  }

  @Override
  public boolean set(String path, T data, int options) {
    return set(path, data, -1, options);
  }

  @Override
  public boolean set(String path, T data, int expectVersion, int options) {
    String clientPath = path;
    String serverPath = prependChroot(clientPath);

    Cache<T> cache = getCache(serverPath);
    if (cache != null) {
      try {
        cache.lockWrite();
        ZkBaseDataAccessor<T>.AccessResult result =
            _baseAccessor.doSet(serverPath, data, expectVersion, options);
        boolean success = (result._retCode == RetCode.OK);

        updateCache(cache, result._pathCreated, success, serverPath, data, result._stat);

        return success;
      } catch (Exception e) {
        return false;
      } finally {
        cache.unlockWrite();
      }
    }

    // no cache
    return _baseAccessor.set(serverPath, data, expectVersion, options);
  }

  @Override
  public boolean update(String path, DataUpdater<T> updater, int options) {
    String clientPath = path;
    String serverPath = prependChroot(clientPath);

    Cache<T> cache = getCache(serverPath);

    if (cache != null) {
      try {
        cache.lockWrite();
        ZkBaseDataAccessor<T>.AccessResult result =
            _baseAccessor.doUpdate(serverPath, updater, options);
        boolean success = (result._retCode == RetCode.OK);
        updateCache(cache, result._pathCreated, success, serverPath, result._resultValue,
            result._stat);

        return success;
      } finally {
        cache.unlockWrite();
      }
    }

    // no cache
    return _groupCommit.commit(_baseAccessor, options, serverPath, updater);
    // return _baseAccessor.update(serverPath, updater, options);
  }

  @Override
  public boolean exists(String path, int options) {
    String clientPath = path;
    String serverPath = prependChroot(clientPath);

    Cache<T> cache = getCache(serverPath);
    if (cache != null) {
      boolean exists = cache.exists(serverPath);
      if (exists) {
        return true;
      }
    }

    // if not exists in cache, always fall back to zk
    return _baseAccessor.exists(serverPath, options);
  }

  @Override
  public boolean remove(String path, int options) {
    String clientPath = path;
    String serverPath = prependChroot(clientPath);

    Cache<T> cache = getCache(serverPath);
    if (cache != null) {
      try {
        cache.lockWrite();

        boolean success = _baseAccessor.remove(serverPath, options);
        if (success) {
          cache.purgeRecursive(serverPath);
        }

        return success;
      } finally {
        cache.unlockWrite();
      }
    }

    // no cache
    return _baseAccessor.remove(serverPath, options);
  }

  @Override
  public T get(String path, Stat stat, int options) {
    String clientPath = path;
    String serverPath = prependChroot(clientPath);

    Cache<T> cache = getCache(serverPath);
    if (cache != null) {
      T record = null;
      ZNode znode = cache.get(serverPath);

      if (znode != null) {
        // TODO: shall return a deep copy instead of reference
        record = ((T) znode.getData());
        if (stat != null) {
          DataTree.copyStat(znode.getStat(), stat);
        }
        return record;

      } else {
        // if cache miss, fall back to zk and update cache
        try {
          cache.lockWrite();
          record =
              _baseAccessor
                  .get(serverPath, stat, options | AccessOption.THROW_EXCEPTION_IFNOTEXIST);
          cache.update(serverPath, record, stat);
        } catch (ZkNoNodeException e) {
          if (AccessOption.isThrowExceptionIfNotExist(options)) {
            throw e;
          }
        } finally {
          cache.unlockWrite();
        }

        return record;
      }
    }

    // no cache
    return _baseAccessor.get(serverPath, stat, options);
  }

  @Override
  public Stat getStat(String path, int options) {
    String clientPath = path;
    String serverPath = prependChroot(clientPath);

    Cache<T> cache = getCache(serverPath);
    if (cache != null) {
      Stat stat = new Stat();
      ZNode znode = cache.get(serverPath);

      if (znode != null) {
        return znode.getStat();

      } else {
        // if cache miss, fall back to zk and update cache
        try {
          cache.lockWrite();
          T data = _baseAccessor.get(serverPath, stat, options);
          cache.update(serverPath, data, stat);
        } catch (ZkNoNodeException e) {
          return null;
        } finally {
          cache.unlockWrite();
        }

        return stat;
      }
    }

    // no cache
    return _baseAccessor.getStat(serverPath, options);
  }

  @Override
  public boolean[] createChildren(List<String> paths, List<T> records, int options) {
    final int size = paths.size();
    List<String> serverPaths = prependChroot(paths);

    Cache<T> cache = getCache(serverPaths);
    if (cache != null) {
      try {
        cache.lockWrite();
        boolean[] needCreate = new boolean[size];
        Arrays.fill(needCreate, true);
        List<ZkBaseDataAccessor<T>.AccessResult> results =
            _baseAccessor.doCreate(serverPaths, records, needCreate, options);

        boolean[] success = new boolean[size];
        for (int i = 0; i < size; i++) {
          ZkBaseDataAccessor<T>.AccessResult result = results.get(i);
          success[i] = (result._retCode == RetCode.OK);

          updateCache(cache, results.get(i)._pathCreated, success[i], serverPaths.get(i),
              records.get(i), ZNode.ZERO_STAT);
        }

        return success;
      } finally {
        cache.unlockWrite();
      }
    }

    // no cache
    return _baseAccessor.createChildren(serverPaths, records, options);
  }

  @Override
  public boolean[] setChildren(List<String> paths, List<T> records, int options) {
    final int size = paths.size();
    List<String> serverPaths = prependChroot(paths);

    Cache<T> cache = getCache(serverPaths);
    if (cache != null) {
      try {
        cache.lockWrite();
        List<ZkBaseDataAccessor<T>.AccessResult> results =
            _baseAccessor.doSet(serverPaths, records, options);

        boolean[] success = new boolean[size];
        for (int i = 0; i < size; i++) {
          success[i] = (results.get(i)._retCode == RetCode.OK);
          updateCache(cache, results.get(i)._pathCreated, success[i], serverPaths.get(i),
              records.get(i), results.get(i)._stat);
        }

        return success;
      } finally {
        cache.unlockWrite();
      }
    }

    return _baseAccessor.setChildren(serverPaths, records, options);
  }

  @Override
  public boolean[] updateChildren(List<String> paths, List<DataUpdater<T>> updaters, int options) {
    final int size = paths.size();
    List<String> serverPaths = prependChroot(paths);

    Cache<T> cache = getCache(serverPaths);
    if (cache != null) {
      try {
        cache.lockWrite();

        boolean[] success = new boolean[size];
        List<List<String>> pathsCreatedList =
            new ArrayList<List<String>>(Collections.<List<String>> nCopies(size, null));

        List<ZkBaseDataAccessor<T>.AccessResult> results =
            _baseAccessor.doUpdate(serverPaths, updaters, options);

        for (int i = 0; i < size; i++) {
          ZkBaseDataAccessor<T>.AccessResult result = results.get(i);
          success[i] = (result._retCode == RetCode.OK);
          updateCache(cache, pathsCreatedList.get(i), success[i], serverPaths.get(i),
              result._resultValue, results.get(i)._stat);
        }
        return success;
      } finally {
        cache.unlockWrite();
      }
    }

    // no cache
    return _baseAccessor.updateChildren(serverPaths, updaters, options);
  }

  // TODO: change to use async_exists
  @Override
  public boolean[] exists(List<String> paths, int options) {
    final int size = paths.size();

    boolean exists[] = new boolean[size];
    for (int i = 0; i < size; i++) {
      exists[i] = exists(paths.get(i), options);
    }
    return exists;
  }

  @Override
  public boolean[] remove(List<String> paths, int options) {
    final int size = paths.size();
    List<String> serverPaths = prependChroot(paths);

    Cache<T> cache = getCache(serverPaths);
    if (cache != null) {
      try {
        cache.lockWrite();

        boolean[] success = _baseAccessor.remove(serverPaths, options);

        for (int i = 0; i < size; i++) {
          if (success[i]) {
            cache.purgeRecursive(serverPaths.get(i));
          }
        }
        return success;
      } finally {
        cache.unlockWrite();
      }
    }

    // no cache
    return _baseAccessor.remove(serverPaths, options);
  }

  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options) {
    if (paths == null || paths.isEmpty()) {
      return Collections.emptyList();
    }

    final int size = paths.size();
    List<String> serverPaths = prependChroot(paths);

    List<T> records = new ArrayList<T>(Collections.<T> nCopies(size, null));
    List<Stat> readStats = new ArrayList<Stat>(Collections.<Stat> nCopies(size, null));

    boolean needRead = false;
    boolean needReads[] = new boolean[size]; // init to false

    Cache<T> cache = getCache(serverPaths);
    if (cache != null) {
      try {
        cache.lockRead();
        for (int i = 0; i < size; i++) {
          ZNode zNode = cache.get(serverPaths.get(i));
          if (zNode != null) {
            // TODO: shall return a deep copy instead of reference
            records.set(i, (T) zNode.getData());
            readStats.set(i, zNode.getStat());
          } else {
            needRead = true;
            needReads[i] = true;
          }
        }
      } finally {
        cache.unlockRead();
      }

      // cache miss, fall back to zk and update cache
      if (needRead) {
        cache.lockWrite();
        try {
          List<ZkBaseDataAccessor<T>.AccessResult> readResults =
              _baseAccessor.doGet(serverPaths, needReads);
          for (int i = 0; i < size; i++) {
            if (needReads[i]) {
              records.set(i, readResults.get(i)._resultValue);
              readStats.set(i, readResults.get(i)._stat);
              cache.update(serverPaths.get(i), records.get(i), readStats.get(i));
            }
          }
        } finally {
          cache.unlockWrite();
        }
      }

      if (stats != null) {
        stats.clear();
        stats.addAll(readStats);
      }

      return records;
    }

    // no cache
    return _baseAccessor.get(serverPaths, stats, options);
  }

  // TODO: add cache
  @Override
  public Stat[] getStats(List<String> paths, int options) {
    List<String> serverPaths = prependChroot(paths);
    return _baseAccessor.getStats(serverPaths, options);
  }

  @Override
  public List<String> getChildNames(String parentPath, int options) {
    String serverParentPath = prependChroot(parentPath);

    Cache<T> cache = getCache(serverParentPath);
    if (cache != null) {
      // System.out.println("zk-cache");
      ZNode znode = cache.get(serverParentPath);

      if (znode != null && znode.getChildSet() != Collections.<String> emptySet()) {
        // System.out.println("zk-cache-hit: " + parentPath);
        List<String> childNames = new ArrayList<String>(znode.getChildSet());
        Collections.sort(childNames);
        return childNames;
      } else {
        // System.out.println("zk-cache-miss");
        try {
          cache.lockWrite();

          List<String> childNames = _baseAccessor.getChildNames(serverParentPath, options);
          // System.out.println("\t--" + childNames);
          cache.addToParentChildSet(serverParentPath, childNames);

          return childNames;
        } finally {
          cache.unlockWrite();
        }
      }
    }

    // no cache
    return _baseAccessor.getChildNames(serverParentPath, options);
  }

  @Override
  public List<T> getChildren(String parentPath, List<Stat> stats, int options) {
    List<String> childNames = getChildNames(parentPath, options);
    if (childNames == null) {
      return null;
    }

    List<String> paths = new ArrayList<String>();
    for (String childName : childNames) {
      String path = parentPath.equals("/") ? "/" + childName : parentPath + "/" + childName;
      paths.add(path);
    }

    return get(paths, stats, options);
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    String serverPath = prependChroot(path);

    _baseAccessor.subscribeDataChanges(serverPath, listener);
  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener) {
    String serverPath = prependChroot(path);

    _baseAccessor.unsubscribeDataChanges(serverPath, listener);
  }

  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    String serverPath = prependChroot(path);

    return _baseAccessor.subscribeChildChanges(serverPath, listener);
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener) {
    String serverPath = prependChroot(path);

    _baseAccessor.unsubscribeChildChanges(serverPath, listener);
  }

  @Override
  public void subscribe(String parentPath, HelixPropertyListener listener) {
    String serverPath = prependChroot(parentPath);
    _zkCache.subscribe(serverPath, listener);
  }

  @Override
  public void unsubscribe(String parentPath, HelixPropertyListener listener) {
    String serverPath = prependChroot(parentPath);
    _zkCache.unsubscribe(serverPath, listener);
  }

  @Override
  public void start() {

    LOG.info("START: Init ZkCacheBaseDataAccessor: " + _chrootPath + ", " + _wtCachePaths + ", "
        + _zkCachePaths);

    // start event thread
    try {
      _eventLock.lockInterruptibly();
      if (_eventThread != null) {
        LOG.warn(_eventThread + " has already started");
      } else {

        if (_zkCachePaths == null || _zkCachePaths.isEmpty()) {
          LOG.warn("ZkCachePaths is null or empty. Will not start ZkCacheEventThread");
        } else {
          LOG.debug("Starting ZkCacheEventThread...");

          _eventThread = new ZkCacheEventThread("");
          _eventThread.start();
        }
      }
    } catch (InterruptedException e) {
      LOG.error("Current thread is interrupted when starting ZkCacheEventThread. ", e);
    } finally {
      _eventLock.unlock();
    }
    LOG.debug("Start ZkCacheEventThread...done");

    _wtCache = new WriteThroughCache<T>(_baseAccessor, _wtCachePaths);
    _zkCache = new ZkCallbackCache<T>(_baseAccessor, _chrootPath, _zkCachePaths, _eventThread);

    if (_wtCachePaths != null && !_wtCachePaths.isEmpty()) {
      for (String path : _wtCachePaths) {
        _cacheMap.put(path, _wtCache);
      }
    }

    if (_zkCachePaths != null && !_zkCachePaths.isEmpty()) {
      for (String path : _zkCachePaths) {
        _cacheMap.put(path, _zkCache);
      }
    }
  }

  @Override
  public void stop() {
    try {
      _eventLock.lockInterruptibly();

      if (_zkclient != null) {
        _zkclient.close();
        _zkclient = null;
      }

      if (_eventThread == null) {
        LOG.warn(_eventThread + " has already stopped");
        return;
      }

      LOG.debug("Stopping ZkCacheEventThread...");
      _eventThread.interrupt();
      _eventThread.join(2000);
      _eventThread = null;
    } catch (InterruptedException e) {
      LOG.error("Current thread is interrupted when stopping ZkCacheEventThread.");
    } finally {
      _eventLock.unlock();
    }

    LOG.debug("Stop ZkCacheEventThread...done");

  }

  @Override
  public void reset() {
    if (_wtCache != null) {
      _wtCache.reset();
    }

    if (_zkCache != null) {
      _zkCache.reset();
    }
  }
}
