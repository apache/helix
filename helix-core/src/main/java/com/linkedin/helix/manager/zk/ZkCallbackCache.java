package com.linkedin.helix.manager.zk;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.IZkListener;
import com.linkedin.helix.manager.zk.ZkCacheEventThread.ZkCacheEvent;
import com.linkedin.helix.store.HelixPropertyListener;
import com.linkedin.helix.store.zk.ZNode;

public class ZkCallbackCache<T> extends Cache<T> implements IZkListener
{
  private static Logger                                 LOG =
                                                                Logger.getLogger(ZkCallbackCache.class);

  final BaseDataAccessor<T>                             _accessor;
  final String                                          _chrootPath;

  private final ZkCacheEventThread                      _eventThread;
  private final Map<String, Set<HelixPropertyListener>> _listener;

  public ZkCallbackCache(BaseDataAccessor<T> accessor,
                         String chrootPath,
                         List<String> paths,
                         ZkCacheEventThread eventThread)
  {
    super();
    _accessor = accessor;
    _chrootPath = chrootPath;

    _listener = new ConcurrentHashMap<String, Set<HelixPropertyListener>>();
    _eventThread = eventThread;

    // init cache
    // System.out.println("init cache: " + paths);
    if (paths != null && !paths.isEmpty())
    {
      for (String path : paths)
      {
        updateRecursive(path);
      }
    }
  }
  
  @Override
  public void update(String path, T data, Stat stat)
  {
    String parentPath = new File(path).getParent();
    String childName = new File(path).getName();
  
    addToParentChildSet(parentPath, childName);
    ZNode znode = _cache.get(path);
    if ( znode == null)
    {
      _cache.put(path, new ZNode(path, data, stat));
      fireEvents(path, EventType.NodeCreated);
    }
    else
    {
      Stat oldStat = znode.getStat();

      znode.setData(data);
      znode.setStat(stat);
  //    System.out.println("\t\t--setData. path: " + path + ", data: " + data);

      if (oldStat.getCzxid() != stat.getCzxid())
      {
        fireEvents(path, EventType.NodeDeleted);
        fireEvents(path, EventType.NodeCreated);
      }
      else if (oldStat.getVersion() != stat.getVersion())
      {
//        System.out.println("\t--fireNodeChanged: " + path + ", oldVersion: " + oldStat.getVersion() + ", newVersion: " + stat.getVersion());
        fireEvents(path, EventType.NodeDataChanged);
      }
    }
  }

  // TODO: make readData async
  @Override
  public void updateRecursive(String path)
  {
    if (path == null)
    {
      return;
    }

    try
    {
      _lock.writeLock().lock();
      try
      {
        // subscribe changes before read
        _accessor.subscribeDataChanges(path, this);

        // update this node
        Stat stat = new Stat();
        T readData = _accessor.get(path, stat, 0);

        update(path, readData, stat);
        
//        // update parent's childSet
//        String parentPath = new File(path).getParent();
//        String name = new File(path).getName();
//        addToParentChildSet(parentPath, name);
//
//        if (znode == null)
//        {
//          znode = new ZNode(path, readData, stat);
//          _cache.put(path, znode);
//
//          System.out.println("\t--fireNodeCreated: " + path);
//          fireEvents(path, EventType.NodeCreated);
//        }
//        else
//        {
//          // if in cache and create timestamp is different
//          // that indicates at least 1 delete and 1 create
//          Stat oldStat = znode.getStat();
//
//          znode.setData(readData);
//          znode.setStat(stat);
//
//          if (oldStat.getCzxid() != stat.getCzxid())
//          {
//            fireEvents(path, EventType.NodeDeleted);
//            fireEvents(path, EventType.NodeCreated);
//          }
//          else if (oldStat.getVersion() != stat.getVersion())
//          {
//            System.out.println("\t--fireNodeChanged: " + path + ", oldVersion: " + oldStat.getVersion() + ", newVersion: " + stat.getVersion());
//            fireEvents(path, EventType.NodeDataChanged);
//          }
//        }
      }
      catch (ZkNoNodeException e)
      {
        // OK. znode not exists
        // we still need to subscribe child change
      }

      // recursively update children nodes if not exists
      // System.out.println("subcribeChildChange: " + path);
      ZNode znode = _cache.get(path);
      List<String> childNames = _accessor.subscribeChildChanges(path, this);
      if (childNames != null && !childNames.isEmpty())
      {
        for (String childName : childNames)
        {
          if (!znode.hasChild(childName))
          {
            String childPath = path + "/" + childName;
            znode.addChild(childName);
            updateRecursive(childPath);
          }
        }
      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
  {
    // System.out.println("handleChildChange: " + parentPath + ", " + currentChilds);

    // this is invoked if subscribed for childChange and node gets deleted
    if (currentChilds == null)
    {
      return;
    }

    updateRecursive(parentPath);
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception
  {
    // System.out.println("handleDataChange: " + dataPath);
    try
    {
      _lock.writeLock().lock();

      // TODO: optimize it by get stat from callback
      Stat stat = new Stat();
      Object readData = _accessor.get(dataPath, stat, 0);

      ZNode znode = _cache.get(dataPath);
      if (znode != null)
      {
        Stat oldStat = znode.getStat();

        // System.out.println("handleDataChange: " + dataPath + ", data: " + data);
//        System.out.println("handleDataChange: " + dataPath + ", oldCzxid: " + oldStat.getCzxid() + ", newCzxid: " + stat.getCzxid()
//                           + ", oldVersion: " + oldStat.getVersion() + ", newVersion: " + stat.getVersion());
        znode.setData(readData);
        znode.setStat(stat);

        // if create right after delete, and zkCallback comes after create
        // no DataDelete() will be fired, instead will fire 2 DataChange()
        // see ZkClient.fireDataChangedEvents()
        if (oldStat.getCzxid() != stat.getCzxid())
        {
          fireEvents(dataPath, EventType.NodeDeleted);
          fireEvents(dataPath, EventType.NodeCreated);
        }
        else if (oldStat.getVersion() != stat.getVersion())
        {
//          System.out.println("\t--fireNodeChanged: " + dataPath + ", oldVersion: " + oldStat.getVersion() + ", newVersion: " + stat.getVersion());
          fireEvents(dataPath, EventType.NodeDataChanged);
        }
      }
      else
      {
        // we may see dataChange on child before childChange on parent
        // in this case, let childChange update cache
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
//    System.out.println("handleDataDeleted: " + dataPath);

    try
    {
      _lock.writeLock().lock();
      _accessor.unsubscribeDataChanges(dataPath, this);
      _accessor.unsubscribeChildChanges(dataPath, this);

      String parentPath = new File(dataPath).getParent();
      String name = new File(dataPath).getName();
      removeFromParentChildSet(parentPath, name);
      _cache.remove(dataPath);

      fireEvents(dataPath, EventType.NodeDeleted);
    }
    finally
    {
      _lock.writeLock().unlock();
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

  public void subscribe(String path, HelixPropertyListener listener)
  {
    synchronized (_listener)
    {
      Set<HelixPropertyListener> listeners = _listener.get(path);
      if (listeners == null)
      {
        listeners = new CopyOnWriteArraySet<HelixPropertyListener>();
        _listener.put(path, listeners);
      }
      listeners.add(listener);
    }
  }

  public void unsubscribe(String path, HelixPropertyListener childListener)
  {
    synchronized (_listener)
    {
      final Set<HelixPropertyListener> listeners = _listener.get(path);
      if (listeners != null)
      {
        listeners.remove(childListener);
      }
    }
  }

  private void fireEvents(final String path, EventType type)
  {
    String tmpPath = path;
    final String clientPath =
        (_chrootPath == null ? path : (_chrootPath.equals(path) ? "/"
            : path.substring(_chrootPath.length())));

    while (tmpPath != null)
    {
      Set<HelixPropertyListener> listeners = _listener.get(tmpPath);

      if (listeners != null && !listeners.isEmpty())
      {
        for (final HelixPropertyListener listener : listeners)
        {
          try
          {
            switch (type)
            {
            case NodeDataChanged:
              // listener.onDataChange(path);
              _eventThread.send(new ZkCacheEvent("dataChange on " + path + " send to "
                  + listener)
              {
                @Override
                public void run() throws Exception
                {
                  listener.onDataChange(clientPath);
                }
              });
              break;
            case NodeCreated:
              // listener.onDataCreate(path);
              _eventThread.send(new ZkCacheEvent("dataCreate on " + path + " send to "
                  + listener)
              {
                @Override
                public void run() throws Exception
                {
                  listener.onDataCreate(clientPath);
                }
              });
              break;
            case NodeDeleted:
              // listener.onDataDelete(path);
              _eventThread.send(new ZkCacheEvent("dataDelete on " + path + " send to "
                  + listener)
              {
                @Override
                public void run() throws Exception
                {
                  listener.onDataDelete(clientPath);
                }
              });
              break;
            default:
              break;
            }
          }
          catch (Exception e)
          {
            LOG.error("Exception in handle events.", e);
          }
        }
      }

      tmpPath = new File(tmpPath).getParent();
    }
  }

}
