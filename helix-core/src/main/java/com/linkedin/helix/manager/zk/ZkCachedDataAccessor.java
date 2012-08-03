package com.linkedin.helix.manager.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.BaseDataAccessor.Option;
import com.linkedin.helix.IZkListener;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordUpdater;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.CreateCallbackHandler;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor.RetCode;
import com.linkedin.helix.store.HelixPropertyListener;
import com.linkedin.helix.store.zk.ZNode;

// TODO: rename to ZkCacheDataAccessor
public class ZkCachedDataAccessor<T> implements IZkListener
{
  private static final Logger                   LOG =
                                                        Logger.getLogger(ZkCachedDataAccessor.class);

  protected ConcurrentMap<String, ZNode>        _zkCache;

  protected ConcurrentMap<String, ZNode>        _wtCache;

  ReadWriteLock                                 _lock;

  final String                                  _chrootPath;
  final ZkBaseDataAccessor<T>                   _accessor;
  final List<String>                            _zkCachePaths;
  final List<String>                            _wtCachePaths;

  final Map<String, Set<HelixPropertyListener>> _listeners;

  public ZkCachedDataAccessor(ZkBaseDataAccessor<T> accessor)
  {
    this(accessor, null, null, null);
  }

  public ZkCachedDataAccessor(ZkBaseDataAccessor<T> accessor,
                              String chrootPath,
                              List<String> zkCachePaths,
                              List<String> wtCachePaths)
  {
    if (chrootPath == null || chrootPath.equals("/"))
    {
      _chrootPath = null;
    }
    else
    {
      PathUtils.validatePath(chrootPath);
      _chrootPath = chrootPath;
    }

    _accessor = accessor;
    _lock = new ReentrantReadWriteLock();

    // TODO: need validate paths in zkCachePath and wtCachePath
    _zkCachePaths = zkCachePaths;
    _zkCache = new ConcurrentHashMap<String, ZNode>();

    _wtCachePaths = wtCachePaths;
    _wtCache = new ConcurrentHashMap<String, ZNode>();

    _listeners = new ConcurrentHashMap<String, Set<HelixPropertyListener>>();

    // TODO: need check wtCachePath has no overlap with zkCachePath

    // init cache
    if (_zkCachePaths != null)
    {
      for (String absPath : _zkCachePaths)
      {
        updateCacheRecursive(absPath, _zkCache, _zkCachePaths);
      }
    }

    if (_wtCachePaths != null)
    {
      for (String absPath : _wtCachePaths)
      {
        updateCacheRecursive(absPath, _wtCache, _wtCachePaths);
      }
    }
  }

  String getAbsolutePath(String path)
  {
    if (_chrootPath != null)
    {
      if (path.equals("/"))
      {
        return _chrootPath;
      }
      return _chrootPath + path;
    }
    else
    {
      return path;
    }
  }

  List<String> getAbsolutePaths(List<String> paths)
  {
    List<String> absPaths = new ArrayList<String>();
    for (String path : paths)
    {
      absPaths.add(getAbsolutePath(path));
    }
    return absPaths;
  }

  String getRelativePath(String absPath)
  {
    if (_chrootPath == null)
    {
      return absPath;
    }
    else
    {
      return absPath.substring(_chrootPath.length());
    }
  }

  boolean isSubPath(List<String> cachePaths, String absPath)
  {
    if (cachePaths == null)
      return false;

    String path = absPath;
    while (path != null && (_chrootPath == null || absPath.startsWith(_chrootPath)))
    {
      if (cachePaths.contains(path))
      {
        return true;
      }
      path = new File(path).getParent();
    }

    return false;
  }

  private void addToParentChildSet(Map<String, ZNode> map, String absPath)
  {
    // add to parent's childSet
    String parentPath = new File(absPath).getParent();
    ZNode zNode = map.get(parentPath);

    if (zNode != null)
    {
      String name = new File(absPath).getName();
      zNode.addChild(name);
    }

  }

  private void removeFromParentChildSet(Map<String, ZNode> map, String absPath)
  {
    // remove from parent's childSet
    String parentPath = new File(absPath).getParent();
    ZNode zNode = map.get(parentPath);
    if (zNode != null)
    {
      String name = new File(absPath).getName();
      zNode.removeChild(name);
    }
  }

  // update cache for newly created paths
  private void updateZkCache(List<String> pathsCreated, String absPath, boolean success)
  {
    if (pathsCreated != null && pathsCreated.size() > 0)
    {

      for (String pathCreated : pathsCreated)
      {
        if (isSubPath(_zkCachePaths, pathCreated))
        {
          updateCacheRecursive(pathCreated, _zkCache, _zkCachePaths);
          break;
        }
      }
    }

    if (success && (pathsCreated == null || !pathsCreated.contains(absPath)))
    {
      updateCacheRecursive(absPath, _zkCache, _zkCachePaths);
    }
  }

  private void updateWtCache(List<String> pathsCreated,
                             String absPath,
                             T data,
                             boolean success)
  {
    if (pathsCreated != null && pathsCreated.size() > 0)
    {
      boolean isWtCacheable = false;
      for (String pathCreated : pathsCreated)
      {
        if (isWtCacheable || isSubPath(_wtCachePaths, pathCreated))
        {
          isWtCacheable = true;
          addToParentChildSet(_wtCache, pathCreated);
          if (pathCreated.equals(absPath))
          {
            _wtCache.put(pathCreated, new ZNode(pathCreated, data, ZNode.DUMMY_STAT));
          }
          else
          {
            _wtCache.put(pathCreated, new ZNode(pathCreated, null, ZNode.DUMMY_STAT));
          }
        }
      }
    }

    if (success && (pathsCreated == null || !pathsCreated.contains(absPath)))
    {
      addToParentChildSet(_wtCache, absPath);
      _wtCache.put(absPath, new ZNode(absPath, data, ZNode.DUMMY_STAT));
    }
  }

  void updateCacheRecursive(String absPath,
                            Map<String, ZNode> cache,
                            List<String> cachePaths)
  {
    if (absPath == null || !isSubPath(cachePaths, absPath))
      return;

    try
    {
      _lock.writeLock().lock();

      // always subscribe before read
      _accessor.subscribe(absPath, this);

      // update parent's childSet
      addToParentChildSet(cache, absPath);

      // update this node
      Stat stat = new Stat();
      T readData = _accessor.get(absPath, stat, 0);
      ZNode zNode = cache.get(absPath);
      if (zNode == null)
      {
        zNode = new ZNode(absPath, readData, stat);
        cache.put(absPath, zNode);
        // System.out.println("fire NodeCreated:" + path);
        fireListeners(absPath, EventType.NodeCreated);
      }
      else
      {
        // if in cache, and create timestamp is different
        // that indicates at least 1 delete and 1 create
        Stat oldStat = zNode.getStat();

        zNode.setData(readData);
        zNode.setStat(stat);

        if (oldStat.getCzxid() != stat.getCzxid())
        {
          fireListeners(absPath, EventType.NodeDeleted);
          fireListeners(absPath, EventType.NodeCreated);
        }
        else if (oldStat.getVersion() != stat.getVersion())
        {
          fireListeners(absPath, EventType.NodeDataChanged);
        }
      }

      // recursively update children nodes
      List<String> childNames = _accessor.getChildNames(absPath, 0);
      for (String childName : childNames)
      {
        String childPath = absPath + "/" + childName;
        if (!zNode.hasChild(childName))
        {
          zNode.addChild(childName);
          updateCacheRecursive(childPath, cache, cachePaths);
        }
      }

    }
    catch (ZkNoNodeException e)
    {
      // OK. someone delete znode while we are updating cache
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  private void purgeCacheRecursive(Map<String, ZNode> cache, String absPath)
  {
    try
    {
      _lock.writeLock().lock();

      // remove from parent's childSet
      removeFromParentChildSet(cache, absPath);

      ZNode zNode = cache.remove(absPath);
      if (zNode != null)
      {
        fireListeners(absPath, EventType.NodeDeleted);

        // recursively remove children nodes
        Set<String> childNames = zNode.getChild();
        for (String childName : childNames)
        {
          String childPath = absPath + "/" + childName;
          purgeCacheRecursive(cache, childPath);
        }
      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean create(String path, T record, int options)
  {
    try
    {
      _lock.writeLock().lock();

      String absPath = getAbsolutePath(path);
      List<String> pathsCreated = new ArrayList<String>();
      RetCode rc = _accessor.create(absPath, record, pathsCreated, options);
      boolean success = (rc == RetCode.OK);

      // even if not succeed, we might create some intermediate nodes
      updateZkCache(pathsCreated, absPath, success);
      updateWtCache(pathsCreated, absPath, record, success);

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean set(String path, T record, int options)
  {
    try
    {
      _lock.writeLock().lock();

      String absPath = getAbsolutePath(path);
      List<String> pathsCreated = new ArrayList<String>();
      boolean success = _accessor.set(absPath, record, pathsCreated, options);

      // even if not succeed, we might create some intermediate nodes
      updateZkCache(pathsCreated, absPath, success);
      updateWtCache(pathsCreated, absPath, record, success);

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean update(String path, DataUpdater<T> updater, int options)
  {
    try
    {
      _lock.writeLock().lock();
      String absPath = getAbsolutePath(path);
      List<String> pathsCreated = new ArrayList<String>();
      boolean success = _accessor.update(absPath, updater, pathsCreated, options);

      // even if not succeed, we might create some intermediate nodes
      updateZkCache(pathsCreated, absPath, success);
      T curData = get(absPath, null, 0);
      updateWtCache(pathsCreated, absPath, updater.update(curData), success);

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  // TODO: fix it. remove might remove some intermediate nodes and return false
  public boolean remove(String path)
  {
    try
    {
      _lock.writeLock().lock();
      String absPath = getAbsolutePath(path);
      boolean success = _accessor.remove(absPath);
      boolean isZkCacheable = isSubPath(_zkCachePaths, absPath);
      boolean isWtCacheable = isSubPath(_wtCachePaths, absPath);

      if (success && isZkCacheable)
      {
        purgeCacheRecursive(_zkCache, absPath);
      }
      else if (success && isWtCacheable)
      {
        purgeCacheRecursive(_wtCache, absPath);
      }

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();

    }
  }

  public boolean[] create(List<String> paths, List<T> records, int options)
  {
    try
    {
      _lock.writeLock().lock();
      List<String> absPaths = getAbsolutePaths(paths);
      boolean[] needCreate = new boolean[paths.size()];
      Arrays.fill(needCreate, true);
      List<List<String>> pathsCreated =
          new ArrayList<List<String>>(Collections.<List<String>> nCopies(paths.size(),
                                                                         null));
      CreateCallbackHandler[] createCbList =
          _accessor.create(absPaths, records, needCreate, pathsCreated, options);

      boolean[] success = new boolean[paths.size()];
      for (int i = 0; i < paths.size(); i++)
      {
        CreateCallbackHandler cb = createCbList[i];
        String absPath = absPaths.get(i);

        success[i] = (Code.get(cb.getRc()) == Code.OK);
        List<String> pathCreatedList = pathsCreated.get(i);

        // even if not succeed, we might create some intermediate nodes
        updateZkCache(pathCreatedList, absPath, success[i]);
        updateWtCache(pathCreatedList, absPath, records.get(i), success[i]);
      }

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();

    }
  }

  public boolean[] set(List<String> paths, List<T> records, int options)
  {
    try
    {
      _lock.writeLock().lock();
      List<String> absPaths = getAbsolutePaths(paths);
      List<List<String>> pathsCreated =
          new ArrayList<List<String>>(Collections.<List<String>> nCopies(paths.size(),
                                                                         null));
      boolean[] success = _accessor.set(absPaths, records, pathsCreated, options);
      for (int i = 0; i < paths.size(); i++)
      {
        String absPath = absPaths.get(i);
        List<String> pathCreatedList = pathsCreated.get(i);

        // even if not succeed, we might create some intermediate nodes
        updateZkCache(pathCreatedList, absPath, success[i]);
        updateWtCache(pathCreatedList, absPath, records.get(i), success[i]);

      }

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean[] update(List<String> paths, List<DataUpdater<T>> updaters, int options)
  {
    try
    {
      _lock.writeLock().lock();
      List<String> absPaths = getAbsolutePaths(paths);
      List<List<String>> pathsCreated =
          new ArrayList<List<String>>(Collections.<List<String>> nCopies(paths.size(),
                                                                         null));

      boolean[] success = _accessor.update(absPaths, updaters, pathsCreated, options);
      for (int i = 0; i < paths.size(); i++)
      {
        String absPath = absPaths.get(i);
        List<String> pathCreatedList = pathsCreated.get(i);

        // even if not succeed, we might create some intermediate nodes
        updateZkCache(pathCreatedList, absPath, success[i]);
        T curData = get(absPath, null, 0);
        updateWtCache(pathCreatedList,
                      absPath,
                      updaters.get(i).update(curData),
                      success[i]);
      }

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  // TODO: fix it. remove might remove some intermediate nodes and return false
  public boolean[] remove(List<String> paths)
  {
    try
    {
      _lock.writeLock().lock();
      List<String> absPaths = getAbsolutePaths(paths);
      boolean[] success = _accessor.remove(absPaths);

      for (int i = 0; i < paths.size(); i++)
      {
        String absPath = absPaths.get(i);
        boolean isZkCacheable = isSubPath(_zkCachePaths, absPath);
        boolean isWtCacheable = isSubPath(_wtCachePaths, absPath);

        if (success[i] && isZkCacheable)
        {
          purgeCacheRecursive(_zkCache, absPath);
        }
        else if (success[i] && isWtCacheable)
        {
          purgeCacheRecursive(_wtCache, absPath);
        }
      }

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public T get(String path, Stat stat, int options)
  {
    String absPath = getAbsolutePath(path);
    boolean isZkCacheable = isSubPath(_zkCachePaths, absPath);
    boolean isWtCacheable = isSubPath(_wtCachePaths, absPath);

    T record = null;

    try
    {
      _lock.readLock().lock();

      // if zk cacheable, read from zk cache
      if (isZkCacheable)
      {
        ZNode zNode = _zkCache.get(absPath);
        if (zNode != null)
        {
          // TODO: shall return a deep copy instead of reference
          record = ((T) zNode.getData());
          if (stat != null)
          {
            DataTree.copyStat(zNode.getStat(), stat);
          }
        }
        return record;
      }
      else if (isWtCacheable)
      {
        ZNode zNode = _wtCache.get(absPath);
        if (zNode != null)
        {
          // TODO: shall return a deep copy instead of reference
          record = ((T) zNode.getData());
          if (stat != null)
          {
            DataTree.copyStat(zNode.getStat(), stat);
          }
        }
        return record;
      }
      else
      {
        // TODO: handle ZkNoNodeException
        record = _accessor.get(absPath, stat, options);
        return record;
      }
    }
    finally
    {
      _lock.readLock().unlock();
    }
  }

  public List<T> get(List<String> paths, List<Stat> stats, int options)
  {
    if (paths == null || paths.size() == 0)
    {
      return Collections.emptyList();
    }

    List<String> absPaths = getAbsolutePaths(paths);
    boolean[] needRead = new boolean[paths.size()];

    List<T> records = new ArrayList<T>(Collections.<T> nCopies(paths.size(), null));
    List<Stat> curStats =
        new ArrayList<Stat>(Collections.<Stat> nCopies(paths.size(), null));

    try
    {
      _lock.readLock().lock();
      for (int i = 0; i < absPaths.size(); i++)
      {
        String absPath = absPaths.get(i);
        boolean isZkCacheable = isSubPath(_zkCachePaths, absPath);
        boolean isWtCacheable = isSubPath(_wtCachePaths, absPath);

        if (isZkCacheable)
        {
          ZNode zNode = _zkCache.get(absPath);
          if (zNode != null)
          {
            // TODO: shall return a deep copy instead of reference
            records.set(i, (T) zNode.getData());
            curStats.set(i, zNode.getStat());
          }
        }
        else if (isWtCacheable)
        {
          ZNode zNode = _wtCache.get(absPath);
          if (zNode != null)
          {
            // TODO: shall return a deep copy instead of reference
            records.set(i, (T) zNode.getData());
            curStats.set(i, zNode.getStat());
          }
        }
        else
        {
          needRead[i] = true;
        }
      }

      List<Stat> readStats =
          new ArrayList<Stat>(Collections.<Stat> nCopies(paths.size(), null));
      List<T> readRecords = _accessor.get(absPaths, readStats, needRead);

      for (int i = 0; i < paths.size(); i++)
      {
        if (needRead[i])
        {
          records.set(i, readRecords.get(i));
          curStats.set(i, readStats.get(i));
        }
      }

      if (stats != null)
      {
        stats.clear();
        stats.addAll(curStats);
      }

      return records;
    }
    finally
    {
      _lock.readLock().unlock();
    }
  }

  public List<String> getChildNames(String parentPath, int options)
  {
    String absParentPath = getAbsolutePath(parentPath);
    boolean isZkCacheable = isSubPath(_zkCachePaths, absParentPath);
    boolean isWtCacheable = isSubPath(_wtCachePaths, absParentPath);

    try
    {
      _lock.readLock().lock();

      if (isZkCacheable)
      {
        ZNode zNode = _zkCache.get(absParentPath);
        if (zNode != null)
        {
          List<String> childNames = new ArrayList<String>(zNode.getChild());
          Collections.sort(childNames);
          return childNames;
        }
        else
        {
          return Collections.emptyList();
        }
      }
      else if (isWtCacheable)
      {
        ZNode zNode = _wtCache.get(absParentPath);
        if (zNode != null)
        {
          List<String> childNames = new ArrayList<String>(zNode.getChild());
          Collections.sort(childNames);
          return childNames;
        }
        else
        {
          return Collections.emptyList();
        }
      }
      else
      {
        return _accessor.getChildNames(absParentPath, options);
      }
    }
    finally
    {
      _lock.readLock().unlock();
    }

  }

  public List<T> getChildren(String parentPath, int options)
  {
    try
    {
      _lock.readLock().lock();

      List<String> childNames = getChildNames(parentPath, options);
      List<String> paths = new ArrayList<String>();
      for (String childName : childNames)
      {
        String path = parentPath + "/" + childName;
        paths.add(path);
      }

      List<Stat> stats = new ArrayList<Stat>();
      List<T> records = get(paths, stats, options);

      // remove null children
      Iterator<T> recordIter = records.iterator();
      Iterator<Stat> statIter = stats.iterator();
      while (statIter.hasNext())
      {
        recordIter.next();
        if (statIter.next() == null)
        {
          statIter.remove();
          recordIter.remove();
        }
      }

      return records;
    }
    finally
    {
      _lock.readLock().unlock();
    }

  }

  public boolean exists(String path)
  {
    String absPath = getAbsolutePath(path);
    boolean isZkCacheable = isSubPath(_zkCachePaths, absPath);
    boolean isWtCacheable = isSubPath(_wtCachePaths, absPath);

    if (isZkCacheable)
    {
      return _zkCache.containsKey(absPath);
    }
    else if (isWtCacheable)
    {
      return _wtCache.containsKey(absPath);
    }
    else
    {
      return _accessor.exists(absPath);
    }
  }

  // TODO: refactor to use async api
  public boolean[] exists(List<String> paths)
  {
    boolean exists[] = new boolean[paths.size()];

    try
    {
      _lock.readLock().lock();

      for (int i = 0; i < paths.size(); i++)
      {
        String path = paths.get(i);
        exists[i] = exists(path);
      }
      return exists;
    }
    finally
    {
      _lock.readLock().unlock();
    }

  }

  public Stat getStat(String path)
  {
    String absPath = getAbsolutePath(path);
    boolean isZkCacheable = isSubPath(_zkCachePaths, absPath);
    boolean isWtCacheable = isSubPath(_wtCachePaths, absPath);

    if (isZkCacheable)
    {
      ZNode zNode = _zkCache.get(absPath);
      if (zNode != null)
      {
        Stat stat = new Stat();
        DataTree.copyStat(zNode.getStat(), stat);

        return stat;
      }
      else
      {
        return null;
      }
    }
    else if (isWtCacheable)
    {
      ZNode zNode = _wtCache.get(absPath);
      if (zNode != null)
      {
        Stat stat = new Stat();
        DataTree.copyStat(zNode.getStat(), stat);

        return stat;
      }
      else
      {
        return null;
      }
    }
    else
    {
      return _accessor.getStat(absPath);
    }
  }

  // TODO: refactor to use async api
  public Stat[] getStats(List<String> paths)
  {
    Stat[] stats = new Stat[paths.size()];

    try
    {
      _lock.readLock().lock();
      for (int i = 0; i < paths.size(); i++)
      {
        String path = paths.get(i);
        stats[i] = getStat(path);
      }
      return stats;
    }
    finally
    {
      _lock.readLock().unlock();
    }

  }

  public void subscribe(String parentPath, final HelixPropertyListener listener)
  {
    synchronized (_listeners)
    {
      String absParentPath = getAbsolutePath(parentPath);

      if (!isSubPath(_zkCachePaths, absParentPath))
      {
        // LOG.debug("Subscribed changes for parentPath: " + absParentPath);
        // updateCacheRecursive(absParentPath);
        throw new IllegalArgumentException(absParentPath
            + " not a subPath of zkCachePaths: " + _zkCachePaths);
      }

      if (!_listeners.containsKey(absParentPath))
      {
        _listeners.put(absParentPath, new CopyOnWriteArraySet<HelixPropertyListener>());
      }
      Set<HelixPropertyListener> listenerSet = _listeners.get(absParentPath);
      listenerSet.add(listener);
    }
  }

  public void unsubscribe(String parentPath, HelixPropertyListener listener)
  {
    synchronized (_listeners)
    {
      String absParentPath = getAbsolutePath(parentPath);

      Set<HelixPropertyListener> listenerSet = _listeners.get(absParentPath);
      if (listenerSet != null)
      {
        listenerSet.remove(listener);
        if (listenerSet.isEmpty())
        {
          _listeners.remove(absParentPath);
        }

      }

      // if (!isSubscribed(absParentPath))
      // {
      // LOG.debug("Unsubscribed changes for pathPrefix: " + absParentPath);
      // purgeCacheRecursive(absParentPath);
      // }
    }
  }

  private void fireListeners(String absPath, EventType type)
  {
    synchronized (_listeners)
    {
      String path = getRelativePath(absPath);
      String tmpAbsPath = absPath;
      while (tmpAbsPath != null
          && (_chrootPath == null || tmpAbsPath.startsWith(_chrootPath)))
      {
        Set<HelixPropertyListener> listenerSet = _listeners.get(tmpAbsPath);
        if (listenerSet != null)
        {
          for (HelixPropertyListener listener : listenerSet)
          {
            switch (type)
            {
            case NodeDataChanged:
              listener.onDataChange(path);
              break;
            case NodeCreated:
              listener.onDataCreate(path);
              break;
            case NodeDeleted:
              listener.onDataDelete(path);
              break;
            default:
              break;
            }
          }
        }
        tmpAbsPath = new File(tmpAbsPath).getParent();
      }
    }
  }

  @Override
  public void handleStateChanged(KeeperState state) throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void handleNewSession() throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception
  {
    // System.out.println("DataChange:" + dataPath);
    try
    {
      _lock.writeLock().lock();

      ZNode zNode = _zkCache.get(dataPath);
      if (zNode != null)
      {
        // TODO: optimize it by get stat from callback
        Stat stat = new Stat();
        Object readData = _accessor.get(dataPath, stat, 0);

        Stat oldStat = zNode.getStat();

        zNode.setData(readData);
        zNode.setStat(stat);

        // if create right after delete, and zkCallback comes after create
        // no DataDelete() will be fired, instead will fire 2 DataChange()
        // see ZkClient.fireDataChangedEvents()
        if (oldStat.getCzxid() != stat.getCzxid())
        {
          fireListeners(dataPath, EventType.NodeDeleted);
          fireListeners(dataPath, EventType.NodeCreated);
        }
        else
        {
          fireListeners(dataPath, EventType.NodeDataChanged);
        }
      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception
  {
    // System.out.println("DataDelete:" + dataPath);

    try
    {
      _lock.writeLock().lock();
      _accessor.unsubscribe(dataPath, this);

      _zkCache.remove(dataPath);

      // remove child from parent's childSet
      // parent stat will be updated in parent's childChange callback
      removeFromParentChildSet(_zkCache, dataPath);

      fireListeners(dataPath, EventType.NodeDeleted);
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
  {
    // System.out.println(this + ":" + "ChildChange: " + currentChilds);
    // System.out.println("ChildChange:" + parentPath);

    // this is invoked if subscribed for child change and node gets deleted
    if (currentChilds == null)
    {
      return;
    }

    updateCacheRecursive(parentPath, _zkCache, _zkCachePaths);
  }

  // temp test code
  public static void main(String[] args) throws Exception
  {
    String zkAddr = "localhost:2191";
    String root = "TestCDA";
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.deleteRecursive("/" + root);

    List<String> list = new ArrayList<String>();
    list.add("/" + root);

    ZkCachedDataAccessor<ZNRecord> accessor =
        new ZkCachedDataAccessor<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(zkClient),
                                           null,
                                           list,
                                           null);
    System.out.println("accessor1:" + accessor);

    ZkClient zkClient2 = new ZkClient(zkAddr);
    zkClient2.setZkSerializer(new ZNRecordSerializer());
    ZkCachedDataAccessor<ZNRecord> accessor2 =
        new ZkCachedDataAccessor<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(zkClient2),
                                           null,
                                           list,
                                           null);
    System.out.println("accessor2:" + accessor2);

    accessor2.subscribe("/" + root, new HelixPropertyListener()
    {

      @Override
      public void onDataChange(String path)
      {
        System.out.println("DataChange:" + path);
      }

      @Override
      public void onDataCreate(String path)
      {
        System.out.println("DataCreate:" + path);
      }

      @Override
      public void onDataDelete(String path)
      {
        System.out.println("DataDelete:" + path);
      }

    });

    System.out.println("cache: " + accessor._zkCache);
    System.out.println("cache2: " + accessor2._zkCache);

    // test sync create will update cache
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = "/" + root + "/host_0/" + msgId;
      boolean success =
          accessor.create(path, new ZNRecord(msgId), BaseDataAccessor.Option.PERSISTENT);
      System.out.println("create:" + success + ":" + msgId);
      // Assert.assertTrue(success, "Should succeed in create");
    }
    System.out.println("cache: " + accessor._zkCache);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._zkCache);

    // test sync set will update cache
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = "/" + root + "/host_0/" + msgId;
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key1", "value1");
      boolean success = accessor.set(path, newRecord, Option.PERSISTENT);
      System.out.println("set:" + success + ":" + msgId);
    }
    System.out.println("cache: " + accessor._zkCache);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._zkCache);

    // test sync update will update cache
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = "/" + root + "/host_0/" + msgId;
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key2", "value2");
      boolean success =
          accessor.update(path, new ZNRecordUpdater(newRecord), Option.PERSISTENT);
      System.out.println("update:" + success + ":" + msgId);
    }
    System.out.println("cache: " + accessor._zkCache);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._zkCache);

    // test get (from cache)
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = "/" + root + "/host_0/" + msgId;
      ZNRecord record = accessor.get(path, null, 0);
      System.out.println("get:" + record);
    }

    // test remove will update cache
    String removePath = "/" + root + "/host_0";
    boolean ret = accessor.remove(removePath);
    System.out.println("remove:" + ret);
    System.out.println("cache: " + accessor._zkCache);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._zkCache);
    System.out.println("cache: " + accessor._zkCache);

    // test createChildren()
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    List<String> paths = new ArrayList<String>();
    String parentPath = "/" + root + "/host_1";

    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      paths.add(parentPath + "/" + msgId);
      records.add(new ZNRecord(msgId));
    }

    boolean[] success =
        accessor.create(paths, records, BaseDataAccessor.Option.PERSISTENT);
    System.out.println("create:" + Arrays.toString(success));
    System.out.println("cache: " + accessor._zkCache);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._zkCache);

    // test async set
    paths = new ArrayList<String>();
    parentPath = "/" + root + "/host_1";
    records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key1", "value1");
      paths.add(parentPath + "/" + msgId);
      records.add(newRecord);
    }
    success = accessor.set(paths, records, Option.PERSISTENT);

    System.out.println("set:" + Arrays.toString(success));
    System.out.println("cache: " + accessor._zkCache);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._zkCache);

    // test async updateChildren
    paths = new ArrayList<String>();
    parentPath = "/" + root + "/host_1";
    List<DataUpdater<ZNRecord>> znrecordUpdaters = new ArrayList<DataUpdater<ZNRecord>>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key2", "value2");
      paths.add(parentPath + "/" + msgId);
      // records.add(newRecord);
      znrecordUpdaters.add(new ZNRecordUpdater(newRecord));
    }
    success = accessor.update(paths, znrecordUpdaters, Option.PERSISTENT);
    System.out.println("update:" + Arrays.toString(success));
    System.out.println("cache: " + accessor._zkCache);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._zkCache);

    // test get (from cache)
    parentPath = "/" + root + "/host_1";
    records = accessor2.getChildren(parentPath, 0);
    System.out.println("get:" + records);

    zkClient.close();
  }
}
