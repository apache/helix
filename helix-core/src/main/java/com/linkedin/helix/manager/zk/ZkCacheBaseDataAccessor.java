package com.linkedin.helix.manager.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.store.zk.ZNode;

public class ZkCacheBaseDataAccessor<T> extends ZkBaseDataAccessor<T>
{
  final ReadWriteLock                    _lock;
  final ConcurrentHashMap<String, ZNode> _wtCache;

  public ZkCacheBaseDataAccessor(ZkClient zkClient, List<String> paths)
  {
    super(zkClient);

    _lock = new ReentrantReadWriteLock();
    _wtCache = new ConcurrentHashMap<String, ZNode>();

    // init cache
    if (paths != null && paths.size() > 0)
    {
      for (String path : paths)
      {
        updateCacheRecursive(path, _wtCache);
      }
    }
  }

  private void addToParentChildSet(ConcurrentHashMap<String, ZNode> map, String path)
  {
    // add to parent's childSet
    String parentPath = new File(path).getParent();
    map.putIfAbsent(parentPath, new ZNode(parentPath, null, null));
    ZNode zNode = map.get(parentPath);

    String name = new File(path).getName();
    zNode.addChild(name);
  }

  private void updateWtCache(String path, T data)
  {
    addToParentChildSet(_wtCache, path);
    _wtCache.put(path, new ZNode(path, data, null));
  }

  void updateCacheRecursive(String path, ConcurrentHashMap<String, ZNode> cache)
  {
    if (path == null)
      return;

    try
    {
      _lock.writeLock().lock();

      // update parent's childSet
      addToParentChildSet(cache, path);

      // update this node
      Stat stat = new Stat();
      T readData = super.get(path, stat, 0);

      ZNode zNode = cache.get(path);
      if (zNode == null)
      {
        zNode = new ZNode(path, readData, stat);
        cache.put(path, zNode);
      }
      else
      {
        zNode.setData(readData);
        zNode.setStat(stat);
      }

      // recursively update children nodes
      List<String> childNames = super.getChildNames(path, 0);
      for (String childName : childNames)
      {
        String childPath = path + "/" + childName;
        if (!zNode.hasChild(childName))
        {
          zNode.addChild(childName);
          updateCacheRecursive(childPath, cache);
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

  private void removeFromParentChildSet(ConcurrentHashMap<String, ZNode> map, String path)
  {
    // remove from parent's childSet
    String parentPath = new File(path).getParent();
    ZNode zNode = map.get(parentPath);
    if (zNode != null)
    {
      String name = new File(path).getName();
      zNode.removeChild(name);
    }
  }

  void purgeCacheRecursive(ConcurrentHashMap<String, ZNode> cache, String path)
  {
    try
    {
      _lock.writeLock().lock();

      // remove from parent's childSet
      removeFromParentChildSet(cache, path);

      ZNode zNode = cache.remove(path);
      if (zNode != null)
      {
        // recursively remove children nodes
        Set<String> childNames = zNode.getChild();
        for (String childName : childNames)
        {
          String childPath = path + "/" + childName;
          purgeCacheRecursive(cache, childPath);
        }
      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public boolean create(String path, T data, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.writeLock().lock();

        boolean success = super.create(path, data, options);

        if (success)
        {
          updateWtCache(path, data);
        }
        return success;
      }
      finally
      {
        _lock.writeLock().unlock();
      }
    }
    else
    {
      return super.create(path, data, options);
    }
  }

  @Override
  public boolean set(String path, T data, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.writeLock().lock();

        boolean success = super.set(path, data, options);
        if (success)
        {
          updateWtCache(path, data);
        }

        return success;
      }
      finally
      {
        _lock.writeLock().unlock();
      }
    }
    else
    {
      return super.set(path, data, options);
    }
  }

  @Override
  public boolean update(String path, DataUpdater<T> updater, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.writeLock().lock();
        T updatedData = super.update(path, updater, null, options);

        boolean success = updatedData != null;
        if (success)
        {
          updateWtCache(path, updatedData);
        }

        return success;
      }
      finally
      {
        _lock.writeLock().unlock();
      }
    }
    else
    {
      return super.update(path, updater, options);
    }
  }

  @Override
  public boolean remove(String path, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.writeLock().lock();

        boolean success = super.remove(path, options);
        if (success)
        {
          purgeCacheRecursive(_wtCache, path);
        }
      }
      finally
      {
        _lock.writeLock().unlock();
      }
    }
    return super.remove(path, options);
  }

  @Override
  public T get(String path, Stat stat, int options)
  {
    T record = null;

    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.readLock().lock();

        ZNode zNode = _wtCache.get(path);
        if (zNode != null)
        {
          // TODO: shall return a deep copy instead of reference
          record = ((T) zNode.getData());
          // if (stat != null)
          // {
          // DataTree.copyStat(zNode.getStat(), stat);
          // }
          return record;
        }
      }
      finally
      {
        _lock.readLock().unlock();
      }
    }

    // if cache miss, fall back to zk

    try
    {
      record = super.get(path, stat, options);
    }
    catch (ZkNoNodeException e)
    {
      // OK
    }
    return record;
  }

  @Override
  public boolean[] createChildren(List<String> paths, List<T> records, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.writeLock().lock();
        boolean[] success = super.createChildren(paths, records, options);
        for (int i = 0; i < paths.size(); i++)
        {
          String path = paths.get(i);
          T data = records.get(i);
          if (success[i])
          {
            updateWtCache(path, data);
          }
        }

        return success;
      }
      finally
      {
        _lock.writeLock().unlock();
      }
    }
    else
    {
      return super.createChildren(paths, records, options);
    }
  }

  @Override
  public boolean[] setChildren(List<String> paths, List<T> records, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.writeLock().lock();
        boolean[] success = super.setChildren(paths, records, options);
        for (int i = 0; i < paths.size(); i++)
        {
          String path = paths.get(i);
          if (success[i])
          {
            updateWtCache(path, records.get(i));
          }
        }

        return success;
      }
      finally
      {
        _lock.writeLock().unlock();
      }
    }
    else
    {
      return super.setChildren(paths, records, options);
    }
  }

  @Override
  public boolean[] updateChildren(List<String> paths,
                                  List<DataUpdater<T>> updaters,
                                  int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.writeLock().lock();
        boolean[] success = new boolean[paths.size()];
        List<T> updateData = super.update(paths, updaters, null, options);
        for (int i = 0; i < paths.size(); i++)
        {
          success[i] = updateData.get(i) != null;
          String path = paths.get(i);
          if (success[i])
          {
            updateWtCache(path, updateData.get(i));
          }
        }
        return success;
      }
      finally
      {
        _lock.writeLock().unlock();
      }
    }
    else
    {
      return super.updateChildren(paths, updaters, options);
    }
  }

  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options)
  {
    if (paths == null || paths.size() == 0)
      return Collections.emptyList();

    List<T> records = new ArrayList<T>(Collections.<T> nCopies(paths.size(), null));
    // List<Stat> curStats =
    // new ArrayList<Stat>(Collections.<Stat> nCopies(paths.size(), null));

    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.readLock().lock();
        for (int i = 0; i < paths.size(); i++)
        {
          String path = paths.get(i);
          ZNode zNode = _wtCache.get(path);
          if (zNode != null)
          {
            // TODO: shall return a deep copy instead of reference
            records.set(i, (T) zNode.getData());
            // curStats.set(i, zNode.getStat());
          }
        }

        // if (stats != null)
        // {
        // stats.clear();
        // stats.addAll(curStats);
        // }

      }
      finally
      {
        _lock.readLock().unlock();
      }
    }

    // if cache miss, fall back to zk
    boolean needRead = false;
    boolean needReads[] = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      if (records.get(i) == null)
      {
        needReads[i] = true;
        needRead = true;
      }
    }

    if (needRead)
    {
      List<T> readRecords = super.get(paths, stats, needReads);
      for (int i = 0; i < paths.size(); i++)
      {
        if (records.get(i) == null)
          records.set(i, readRecords.get(i));
      }
    }

    return records;
  }

  @Override
  public boolean exists(String path, int options)
  {

    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      boolean exist = _wtCache.containsKey(path);
      if (exist)
      {
        return true;
      }
    }

    return super.exists(path, options);
  }

  @Override
  public List<T> getChildren(String parentPath, List<Stat> stats, int options)
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

      // List<Stat> curStats = new ArrayList<Stat>();
      return get(paths, null, options);
    }
    finally
    {
      _lock.readLock().unlock();
    }
  }

  @Override
  public List<String> getChildNames(String parentPath, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.readLock().lock();

        ZNode zNode = _wtCache.get(parentPath);
        if (zNode != null)
        {
          List<String> childNames = new ArrayList<String>(zNode.getChild());
          Collections.sort(childNames);
          return childNames;
        }
      }
      finally
      {
        _lock.readLock().unlock();
      }
    }

    // if cache miss, fall back to zk
    return super.getChildNames(parentPath, options);
  }

  @Override
  public boolean[] remove(List<String> paths, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _lock.writeLock().lock();
        boolean[] success = super.remove(paths, options);

        for (int i = 0; i < paths.size(); i++)
        {
          String path = paths.get(i);

          if (success[i])
          {
            purgeCacheRecursive(_wtCache, path);
          }
        }
        return success;
      }
      finally
      {
        _lock.writeLock().unlock();
      }
    }
    return super.remove(paths, options);
  }

  // TODO: change to use async_exists
  @Override
  public boolean[] exists(List<String> paths, int options)
  {
    boolean exists[] = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      String path = paths.get(i);
      exists[i] = exists(path, options);
    }
    return exists;
  }
}
