package com.linkedin.helix.manager.zk;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.store.zk.ZNode;

public abstract class Cache<T>
{
  final ReadWriteLock                    _lock;
  final ConcurrentHashMap<String, ZNode> _cache;

  public Cache()
  {
    _lock = new ReentrantReadWriteLock();
    _cache = new ConcurrentHashMap<String, ZNode>();
  }

  public void addToParentChildSet(String parentPath, String childName)
  {
    ZNode znode = _cache.get(parentPath);
    if (znode != null)
    {
      znode.addChild(childName);
    }
  }

  public void addToParentChildSet(String parentPath, List<String> childNames)
  {
    if (childNames != null && !childNames.isEmpty())
    {
      ZNode znode = _cache.get(parentPath);
      if (znode != null)
      {
        znode.addChildren(childNames);
      }
    }
  }

  public void removeFromParentChildSet(String parentPath, String name)
  {
    ZNode zNode = _cache.get(parentPath);
    if (zNode != null)
    {
      zNode.removeChild(name);
    }
  }

//  public void update(String path, T data, Stat stat)
//  {
//    String parentPath = new File(path).getParent();
//    String childName = new File(path).getName();
//
//    addToParentChildSet(parentPath, childName);
//    ZNode znode = _cache.get(path);
//    if (znode != null)
//    {
//      znode.setData(data);
//      znode.setStat(stat);
////      System.out.println("\t\t--setData. path: " + path + ", data: " + data);
//    }
//    else
//    {
//      _cache.put(path, new ZNode(path, data, stat));
//    }
//  }

  public boolean exists(String path)
  {
    return _cache.containsKey(path);
  }

  public ZNode get(String path)
  {
    try
    {
      _lock.readLock().lock();
      return _cache.get(path);
    } 
    finally
    {
      _lock.readLock().unlock();
    }
  }

  public void lockWrite()
  {
    _lock.writeLock().lock();
  }

  public void unlockWrite()
  {
    _lock.writeLock().unlock();
  }

  public void lockRead()
  {
    _lock.readLock().lock();
  }

  public void unlockRead()
  {
    _lock.readLock().unlock();
  }

  public void purgeRecursive(String path)
  {
    try
    {
      _lock.writeLock().lock();

      String parentPath = new File(path).getParent();
      String name = new File(path).getName();
      removeFromParentChildSet(parentPath, name);

      ZNode znode = _cache.remove(path);
      if (znode != null)
      {
        // recursively remove children nodes
        Set<String> childNames = znode.getChildSet();
        for (String childName : childNames)
        {
          String childPath = path + "/" + childName;
          purgeRecursive(childPath);
        }
      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }
  
  public void reset()
  {
    try
    {
      _lock.writeLock().lock();
      _cache.clear();
    } 
    finally
    {
      _lock.writeLock().unlock();
    }
  }
  
  public abstract void update(String path, T data, Stat stat);
  
  public abstract void updateRecursive(String path);
  
  
  // debug
  public Map<String, ZNode> getCache()
  {
    return _cache;
  }
  
}
