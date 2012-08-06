package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.store.zk.ZNode;

public class ZkCacheBaseDataAccessor<T> extends ZkBaseDataAccessor<T>
{
  final WriteThroughCache<T> _wtCache;

  public ZkCacheBaseDataAccessor(ZkClient zkClient, List<String> paths)
  {
    super(zkClient);
    _wtCache = new WriteThroughCache<T>(this, paths);
  }

  @Override
  public boolean create(String path, T data, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      try
      {
        _wtCache.lockWrite();

        boolean success = super.create(path, data, options);

        if (success)
        {
          _wtCache.updateWtCache(path, data);
        }
        return success;
      }
      finally
      {
        _wtCache.unlockWrite();
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
        _wtCache.lockWrite();

        boolean success = super.set(path, data, options);
        if (success)
        {
          _wtCache.updateWtCache(path, data);
        }

        return success;
      }
      finally
      {
        _wtCache.unlockWrite();
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
        _wtCache.lockWrite();

        T updatedData = super.update(path, updater, null, options);

        boolean success = updatedData != null;
        if (success)
        {
          _wtCache.updateWtCache(path, updatedData);
        }

        return success;
      }
      finally
      {
        _wtCache.unlockWrite();

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
        _wtCache.lockWrite();

        boolean success = super.remove(path, options);
        if (success)
        {
          _wtCache.purgeCache(path);
        }
      }
      finally
      {
        _wtCache.unlockWrite();

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
        _wtCache.lockWrite();
        boolean[] success = super.createChildren(paths, records, options);
        for (int i = 0; i < paths.size(); i++)
        {
          String path = paths.get(i);
          T data = records.get(i);
          if (success[i])
          {
            _wtCache.updateWtCache(path, data);
          }
        }

        return success;
      }
      finally
      {
        _wtCache.unlockWrite();
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
        _wtCache.lockWrite();

        boolean[] success = super.setChildren(paths, records, options);
        for (int i = 0; i < paths.size(); i++)
        {
          String path = paths.get(i);
          if (success[i])
          {
            _wtCache.updateWtCache(path, records.get(i));
          }
        }

        return success;
      }
      finally
      {
        _wtCache.unlockWrite();
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
        _wtCache.lockWrite();

        boolean[] success = new boolean[paths.size()];
        List<T> updateData = super.update(paths, updaters, null, options);
        for (int i = 0; i < paths.size(); i++)
        {
          success[i] = updateData.get(i) != null;
          String path = paths.get(i);
          if (success[i])
          {
            _wtCache.updateWtCache(path, updateData.get(i));
          }
        }
        return success;
      }
      finally
      {
        _wtCache.unlockWrite();

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
        _wtCache.lockRead();

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
        _wtCache.unlockRead();

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
      boolean exist = _wtCache.exists(path);
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
      _wtCache.lockRead();

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
      _wtCache.unlockRead();

    }
  }

  @Override
  public List<String> getChildNames(String parentPath, int options)
  {
    if (BaseDataAccessor.Option.isWriteThrough(options))
    {
      ZNode zNode = _wtCache.get(parentPath);
      if (zNode != null)
      {
        List<String> childNames = new ArrayList<String>(zNode.getChild());
        Collections.sort(childNames);
        return childNames;
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
        _wtCache.lockWrite();

        boolean[] success = super.remove(paths, options);

        for (int i = 0; i < paths.size(); i++)
        {
          String path = paths.get(i);

          if (success[i])
          {
            _wtCache.purgeCache(path);
          }
        }
        return success;
      }
      finally
      {
        _wtCache.unlockWrite();

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
