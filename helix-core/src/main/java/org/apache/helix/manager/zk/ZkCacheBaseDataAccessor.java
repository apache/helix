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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.ZkBaseDataAccessor.RetCode;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZNode;
import org.apache.helix.util.PathUtils;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkCacheBaseDataAccessor<T> implements HelixPropertyStore<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ZkCacheBaseDataAccessor.class);

  protected WriteThroughCache<T> _wtCache;
  protected ZkCallbackCache<T> _zkCache;

  final ZkBaseDataAccessor<T> _baseAccessor;

  // TODO: need to make sure no overlap between wtCachePaths and zkCachePaths
  // TreeMap key is ordered by key string length, so more general (i.e. short) prefix
  // comes first
  final Map<String, Cache<T>> _cacheMap = new TreeMap<>((o1, o2) -> {
    int len1 = o1.split("/").length;
    int len2 = o2.split("/").length;
    return len1 - len2;
  });

  final String _chrootPath;
  final List<String> _wtCachePaths;
  final List<String> _zkCachePaths;

  final HelixGroupCommit<T> _groupCommit = new HelixGroupCommit<T>();

  // fire listeners
  private final ReentrantLock _eventLock = new ReentrantLock();
  private ZkCacheEventThread _eventThread;

  private RealmAwareZkClient _zkClient;

  @Deprecated
  public ZkCacheBaseDataAccessor(ZkBaseDataAccessor<T> baseAccessor, List<String> wtCachePaths) {
    this(baseAccessor, null, wtCachePaths, null);
  }

  @Deprecated
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

    start();
  }

  @Deprecated
  public ZkCacheBaseDataAccessor(String zkAddress, ZkSerializer serializer, String chrootPath,
      List<String> wtCachePaths, List<String> zkCachePaths) {
    this(zkAddress, serializer, chrootPath, wtCachePaths, zkCachePaths, null, null,
        ZkBaseDataAccessor.ZkClientType.SHARED);
  }

  @Deprecated
  public ZkCacheBaseDataAccessor(String zkAddress, ZkSerializer serializer, String chrootPath,
      List<String> wtCachePaths, List<String> zkCachePaths, String monitorType, String monitorkey) {
    this(zkAddress, serializer, chrootPath, wtCachePaths, zkCachePaths, monitorType, monitorkey,
        ZkBaseDataAccessor.ZkClientType.SHARED);
  }

  @Deprecated
  public ZkCacheBaseDataAccessor(String zkAddress, ZkSerializer serializer, String chrootPath,
      List<String> wtCachePaths, List<String> zkCachePaths, String monitorType, String monitorkey,
      ZkBaseDataAccessor.ZkClientType zkClientType) {
    _zkClient = ZkBaseDataAccessor.buildRealmAwareZkClientWithDefaultConfigs(
        new RealmAwareZkClient.RealmAwareZkClientConfig().setZkSerializer(serializer)
            .setMonitorType(monitorType).setMonitorKey(monitorkey), zkAddress, zkClientType);
    _baseAccessor = new ZkBaseDataAccessor<>(_zkClient);

    if (chrootPath == null || chrootPath.equals("/")) {
      _chrootPath = null;
    } else {
      PathUtils.validatePath(chrootPath);
      _chrootPath = chrootPath;
    }

    _wtCachePaths = wtCachePaths;
    _zkCachePaths = zkCachePaths;

    start();
  }

  private ZkCacheBaseDataAccessor(RealmAwareZkClient zkClient, String chrootPath,
      List<String> wtCachePaths, List<String> zkCachePaths) {
    _zkClient = zkClient;
    _baseAccessor = new ZkBaseDataAccessor<>(_zkClient);

    _chrootPath = chrootPath;
    _wtCachePaths = wtCachePaths;
    _zkCachePaths = zkCachePaths;

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
          throw new IllegalArgumentException(
              "Couldn't do cross-cache async operations. paths: " + paths);
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
    boolean success = false;
    try {
      if (cache != null) {
        cache.lockWrite();
        ZkBaseDataAccessor<T>.AccessResult result =
            _baseAccessor.doSet(serverPath, data, expectVersion, options);
        success = result._retCode == RetCode.OK;

        updateCache(cache, result._pathCreated, success, serverPath, data, result._stat);
      } else {
        // no cache
        success = _baseAccessor.set(serverPath, data, expectVersion, options);
      }
    } catch (Exception e) {
    } finally {
      if (cache != null) {
        cache.unlockWrite();
      }
    }
    return success;
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
        updateCache(cache, result._pathCreated, success, serverPath, result._updatedValue,
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
          record = _baseAccessor
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
        List<List<String>> pathsCreatedList =
            new ArrayList<List<String>>(Collections.<List<String>>nCopies(size, null));
        ZkAsyncCallbacks.CreateCallbackHandler[] createCbList =
            _baseAccessor.create(serverPaths, records, needCreate, pathsCreatedList, options);

        boolean[] success = new boolean[size];
        for (int i = 0; i < size; i++) {
          ZkAsyncCallbacks.CreateCallbackHandler cb = createCbList[i];
          success[i] = (Code.get(cb.getRc()) == Code.OK);

          updateCache(cache, pathsCreatedList.get(i), success[i], serverPaths.get(i),
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
        List<Stat> setStats = new ArrayList<Stat>();
        List<List<String>> pathsCreatedList =
            new ArrayList<List<String>>(Collections.<List<String>>nCopies(size, null));
        boolean[] success =
            _baseAccessor.set(serverPaths, records, pathsCreatedList, setStats, options);

        for (int i = 0; i < size; i++) {
          updateCache(cache, pathsCreatedList.get(i), success[i], serverPaths.get(i),
              records.get(i), setStats.get(i));
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

        List<Stat> setStats = new ArrayList<Stat>();
        boolean[] success = new boolean[size];
        List<List<String>> pathsCreatedList =
            new ArrayList<List<String>>(Collections.<List<String>>nCopies(size, null));
        List<T> updateData =
            _baseAccessor.update(serverPaths, updaters, pathsCreatedList, setStats, options);

        // System.out.println("updateChild: ");
        // for (T data : updateData)
        // {
        // System.out.println(data);
        // }

        for (int i = 0; i < size; i++) {
          success[i] = (updateData.get(i) != null);
          updateCache(cache, pathsCreatedList.get(i), success[i], serverPaths.get(i),
              updateData.get(i), setStats.get(i));
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

  @Deprecated
  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options) {
    return get(paths, stats, options, false);
  }

  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options, boolean throwException)
      throws HelixException {
    if (paths == null || paths.isEmpty()) {
      return Collections.emptyList();
    }

    final int size = paths.size();
    List<String> serverPaths = prependChroot(paths);

    List<T> records = new ArrayList<T>(Collections.<T>nCopies(size, null));
    List<Stat> readStats = new ArrayList<Stat>(Collections.<Stat>nCopies(size, null));

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
          List<T> readRecords =
              _baseAccessor.get(serverPaths, readStats, needReads, throwException);
          for (int i = 0; i < size; i++) {
            if (needReads[i]) {
              records.set(i, readRecords.get(i));
              cache.update(serverPaths.get(i), readRecords.get(i), readStats.get(i));
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
    return _baseAccessor.get(serverPaths, stats, options, throwException);
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

      if (znode != null && znode.getChildSet() != Collections.<String>emptySet()) {
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

  @Deprecated
  @Override
  public List<T> getChildren(String parentPath, List<Stat> stats, int options) {
    return getChildren(parentPath, stats, options, false);
  }

  @Override
  public List<T> getChildren(String parentPath, List<Stat> stats, int options, int retryCount,
      int retryInterval) throws HelixException {
    // TODO add retry logic according to retryCount and retryInterval input
    return getChildren(parentPath, stats, options, true);
  }

  private List<T> getChildren(String parentPath, List<Stat> stats, int options, boolean throwException) {
    List<String> childNames = getChildNames(parentPath, options);
    if (childNames == null) {
      return null;
    }

    List<String> paths = new ArrayList<>();
    for (String childName : childNames) {
      String path = parentPath.equals("/") ? "/" + childName : parentPath + "/" + childName;
      paths.add(path);
    }

    return get(paths, stats, options, throwException);
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

      if (_zkClient != null) {
        _zkClient.close();
        _zkClient = null;
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

  @Override
  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
    }
  }

  @Override
  public void finalize() {
    close();
  }

  public static class Builder<T> extends GenericBaseDataAccessorBuilder<Builder<T>> {
    /** ZkCacheBaseDataAccessor-specific parameters */
    private String _chrootPath;
    private List<String> _wtCachePaths;
    private List<String> _zkCachePaths;

    public Builder() {
    }

    public Builder<T> setChrootPath(String chrootPath) {
      _chrootPath = chrootPath;
      return this;
    }

    public Builder<T> setWtCachePaths(List<String> wtCachePaths) {
      _wtCachePaths = wtCachePaths;
      return this;
    }

    public Builder<T> setZkCachePaths(List<String> zkCachePaths) {
      _zkCachePaths = zkCachePaths;
      return this;
    }

    public ZkCacheBaseDataAccessor<T> build() {
      validate();
      return new ZkCacheBaseDataAccessor<>(
          createZkClient(_realmMode, _realmAwareZkConnectionConfig, _realmAwareZkClientConfig,
              _zkAddress), _chrootPath, _wtCachePaths, _zkCachePaths);
    }
  }
}
