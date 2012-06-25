package com.linkedin.helix.manager.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.BaseDataAccessor.Option;
import com.linkedin.helix.IZkListener;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.store.zk.ZNode;

public class ZkCachedDataAccessor implements IZkListener
{
  private static final Logger               LOG        =
                                                           Logger.getLogger(ZkCachedDataAccessor.class);

  ConcurrentMap<String, ZNode>              _map       =
                                                           new ConcurrentHashMap<String, ZNode>();
  ReadWriteLock                             _lock      = new ReentrantReadWriteLock();

  final String                              _root;
  final ZkBaseDataAccessor                  _accessor;
  final List<String>                        _subscribedPaths;

  final Map<String, Set<HelixDataListener>> _listeners =
                                                           new ConcurrentHashMap<String, Set<HelixDataListener>>();

  public ZkCachedDataAccessor(ZkBaseDataAccessor accessor, List<String> subscribedPaths)
  {
    _root = accessor._root;
    _accessor = accessor;
    _subscribedPaths = subscribedPaths;

    for (String path : subscribedPaths)
    {
      updateCacheRecursive(path);
    }
  }

  boolean isSubscribed(String path)
  {
    boolean ret = false;
    while (path.length() >= _root.length())
    {
      if (_subscribedPaths.contains(path))
      {
        ret = true;
        break;
      }
      path = path.substring(0, path.lastIndexOf('/'));
    }

    return ret;
  }

  void updateCacheRecursive(String path)
  {
    if (!isSubscribed(path))
    {
      return;
    }

    try
    {
      _lock.writeLock().lock();

      _accessor.subscribe(path, this);

      // update parent's childSet
      String parentPath = new File(path).getParent();
      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        String name = new File(path).getName();
        zNode.addChild(name);
      }

      // update this node
      Stat stat = new Stat();
      ZNRecord readData = _accessor.get(path, stat, 0);
      if (!_map.containsKey(path))
      {
        _map.put(path, new ZNode(path, readData, stat));
//        System.out.println("fire NodeCreated:" + path);
        fireListeners(path, EventType.NodeCreated);
      }
      else
      {
        // if in cache, and create timestamp is different
        // that indicates at least 1 delete and 1 create
        Stat oldStat = _map.get(path).getStat();
        if (oldStat.getCzxid() != stat.getCzxid())
        {
          fireListeners(path, EventType.NodeDeleted);
          fireListeners(path, EventType.NodeCreated);
        }
      }

      zNode = _map.get(path);
      zNode.setData(readData);
      zNode.setStat(stat);

      // recursively update children nodes
      List<String> childNames = _accessor.getChildNames(path, 0);
      for (String childName : childNames)
      {
        String childPath = path + "/" + childName;
        if (!zNode.hasChild(childName))
        {
          zNode.addChild(childName);
          updateCacheRecursive(childPath);
        }
      }

    }
    catch (ZkNoNodeException e)
    {
      // OK
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  private void updateCacheAlongPath(String path, boolean success)
  {
    int idx = _root.length() + 1;
    while (idx > 0)
    {
      if (!_map.containsKey(path.substring(0, idx)))
      {
        break;
      }
      idx = path.indexOf('/', idx + 1);
    }

    if (idx > 0)
    {
      updateCacheRecursive(path.substring(0, idx));
    }
    else
    {
      if (success)
      {
        updateCacheRecursive(path);
      }
    }
  }

  private void purgeCacheRecursive(String path)
  {
    try
    {
      _lock.writeLock().lock();

      // remove from parent's childSet
      String parentPath = new File(path).getParent();
      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        String name = new File(path).getName();
        zNode.removeChild(name);
      }

      // recursively remove children nodes
      zNode = _map.remove(path);
      if (zNode != null)
      {
        Set<String> childNames = zNode.getChild();
        for (String childName : childNames)
        {
          String childPath = path + "/" + childName;
          purgeCacheRecursive(childPath);
        }
      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }

  }

  public boolean create(String path, ZNRecord record, int options)
  {
    try
    {
      _lock.writeLock().lock();

      boolean success = _accessor.create(path, record, options);
      updateCacheAlongPath(path, success);
      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean set(String path, ZNRecord record, int options)
  {
    try
    {
      _lock.writeLock().lock();

      boolean success = _accessor.set(path, record, options);
      updateCacheAlongPath(path, success);
      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean update(String path, ZNRecord record, int options)
  {
    try
    {
      _lock.writeLock().lock();

      boolean success = _accessor.update(path, record, options);
      updateCacheAlongPath(path, success);
      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean remove(String path)
  {
    try
    {
      _lock.writeLock().lock();

      boolean success = _accessor.remove(path);
      if (isSubscribed(path))
      {
        purgeCacheRecursive(path);
      }
      return success;
    }
    finally
    {
      _lock.writeLock().unlock();

    }
  }

  private void updateCacheAlongPath(String parentPath,
                                    List<ZNRecord> records,
                                    boolean[] success)
  {
    int idx = _root.length() + 1;
    while (idx > 0)
    {
      if (!_map.containsKey(parentPath.substring(0, idx)))
      {
        break;
      }
      idx = parentPath.indexOf('/', idx + 1);
    }

    if (idx > 0)
    {
      updateCacheRecursive(parentPath.substring(0, idx));
    }
    else
    {
      for (int i = 0; i < records.size(); i++)
      {
        if (success[i])
        {
          String path = parentPath + "/" + records.get(i).getId();
          updateCacheRecursive(path);
        }
      }
    }
  }

  public boolean[] createChildren(String parentPath, List<ZNRecord> records, int options)
  {
    try
    {
      _lock.writeLock().lock();

      boolean[] success = _accessor.createChildren(parentPath, records, options);
      updateCacheAlongPath(parentPath, records, success);
      return success;
    }
    finally
    {
      _lock.writeLock().unlock();

    }
  }

  public boolean[] setChildren(String parentPath, List<ZNRecord> records, int options)
  {
    try
    {
      _lock.writeLock().lock();
      boolean[] success = _accessor.setChildren(parentPath, records, options);
      updateCacheAlongPath(parentPath, records, success);

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean[] updateChildren(String parentPath, List<ZNRecord> records, int options)
  {
    try
    {
      _lock.writeLock().lock();
      boolean[] success = _accessor.updateChildren(parentPath, records, options);

      updateCacheAlongPath(parentPath, records, success);
      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean[] remove(List<String> paths)
  {
    try
    {
      _lock.writeLock().lock();

      boolean[] success = _accessor.remove(paths);

      for (String path : paths)
      {
        if (isSubscribed(path))
        {
          purgeCacheRecursive(path);
        }
      }
      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public ZNRecord get(String path, Stat stat, int options)
  {
    if (isSubscribed(path))
    {
      ZNRecord record = null;
      ZNode zNode = _map.get(path);
      if (zNode != null)
      {
        // TODO: shall return a deep copy instead of reference
        record = new ZNRecord((ZNRecord) zNode.getData());
        if (stat != null)
        {
          // TODO: copy zNode.getStat() to stat
        }
      }
      return record;
    }
    else
    {
      return _accessor.get(path, stat, options);
    }

  }

  public List<ZNRecord> get(List<String> paths, int options)
  {
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (String path : paths)
    {
      records.add(_accessor.get(path, null, options));
    }

    return records;
  }

  public List<ZNRecord> getChildren(String parentPath, int options)
  {
    List<String> childNames = getChildNames(parentPath, options);
    List<String> paths = new ArrayList<String>();
    for (String childName : childNames)
    {
      String path = parentPath + "/" + childName;
      paths.add(path);
    }
    return get(paths, options);
  }

  public String getPath(PropertyType type, String... keys)
  {
    return _accessor.getPath(type, keys);
  }

  public List<String> getChildNames(String parentPath, int options)
  {
    if (isSubscribed(parentPath))
    {
      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        return new ArrayList<String>(zNode.getChild());
      }
      else
      {
        return Collections.emptyList();
      }
    }
    else
    {
      return _accessor.getChildNames(parentPath, options);
    }
  }

  public boolean exists(String path)
  {
    if (isSubscribed(path))
    {
      return _map.containsKey(path);
    }
    else
    {
      return _accessor.exists(path);
    }
  }

  public boolean[] exists(List<String> paths)
  {
    boolean[] exists = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      String path = paths.get(i);
      exists[i] = exists(path);
    }
    return exists;
  }

  public Stat[] getStats(List<String> paths)
  {
    Stat[] stats = new Stat[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      String path = paths.get(i);
      stats[i] = getStat(path);
    }
    return stats;
  }

  public Stat getStat(String path)
  {
    if (isSubscribed(path))
    {
      ZNode zNode = _map.get(path);
      if (zNode != null)
      {
        // TODO: return a copy
        return zNode.getStat();
      }
      else
      {
        return null;
      }
    }
    else
    {
      return _accessor.getStat(path);
    }
  }

  public void subscribe(String parentPath, final HelixDataListener listener)
  {
    synchronized (_listeners)
    {
      if (!isSubscribed(parentPath))
      {
        LOG.debug("Subscribed changes for parentPath: " + parentPath);
        updateCacheRecursive(parentPath);
      }

      if (!_listeners.containsKey(parentPath))
      {
        _listeners.put(parentPath, new CopyOnWriteArraySet<HelixDataListener>());
      }
      Set<HelixDataListener> listenerSet = _listeners.get(parentPath);
      listenerSet.add(listener);
    }
  }

  public void unsubscribe(String parentPath, HelixDataListener listener)
  {
    synchronized (_listeners)
    {
      Set<HelixDataListener> listenerSet = _listeners.get(parentPath);
      if (listenerSet != null)
      {
        listenerSet.remove(listener);
        if (listenerSet.isEmpty())
        {
          _listeners.remove(parentPath);
        }

      }

      if (!isSubscribed(parentPath))
      {
        LOG.debug("Unsubscribed changes for pathPrefix: " + parentPath);
        purgeCacheRecursive(parentPath);
      }
    }
  }

  private void fireListeners(String path, EventType type)
  {
    synchronized (_listeners)
    {
      String tmpPath = new String(path);
      while (tmpPath != null)
      {
        Set<HelixDataListener> listenerSet = _listeners.get(tmpPath);
        if (listenerSet != null)
        {
          for (HelixDataListener listener : listenerSet)
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
        tmpPath = new File(tmpPath).getParent();
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
    try
    {
      _lock.writeLock().lock();

      ZNode zNode = _map.get(dataPath);
      if (zNode != null)
      {
        // TODO: optimize it by get stat from callback
        Stat stat = new Stat();
        Object readData = _accessor.get(dataPath, stat, 0);

        zNode.setData(readData);
        Stat oldStat = zNode.getStat();
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
    try
    {
      _lock.writeLock().lock();
      _map.remove(dataPath);

      _accessor.unsubscribe(dataPath, this);

      String parentPath = new File(dataPath).getParent();

      // remove child from parent's childSet
      // parent stat will also be changed. parent's stat will be updated in
      // parent's
      // childChange callback
      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        String child = new File(dataPath).getName();
        zNode.removeChild(child);
        fireListeners(dataPath, EventType.NodeDeleted);
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
//    System.out.println(this + ":" + "ChildChange: " + currentChilds);
    if (currentChilds == null)
    {
      return;
    }

    updateCacheRecursive(parentPath);
  }

  public static void main(String[] args) throws Exception
  {
    String zkAddr = "localhost:2191";
    String root = "TestCDA";
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.deleteRecursive("/" + root);

    List<String> list = new ArrayList<String>();
    list.add("/" + root);

    ZkCachedDataAccessor accessor =
        new ZkCachedDataAccessor(new ZkBaseDataAccessor(root, zkClient), list);
    System.out.println("accessor1:" + accessor);

    ZkClient zkClient2 = new ZkClient(zkAddr);
    zkClient2.setZkSerializer(new ZNRecordSerializer());
    ZkCachedDataAccessor accessor2 =
        new ZkCachedDataAccessor(new ZkBaseDataAccessor(root, zkClient2), list);
    System.out.println("accessor2:" + accessor2);
    
    accessor2.subscribe("/" + root, new HelixDataListener()
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

    System.out.println("cache: " + accessor._map);
    System.out.println("cache2: " + accessor2._map);

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
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);

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
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);

    // test sync update will update cache
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = "/" + root + "/host_0/" + msgId;
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key2", "value2");
      boolean success = accessor.update(path, newRecord, Option.PERSISTENT);
      System.out.println("update:" + success + ":" + msgId);
    }
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);

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
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);
    System.out.println("cache: " + accessor._map);

    // test createChildren()
    List<ZNRecord> records = new ArrayList<ZNRecord>();

    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      records.add(new ZNRecord(msgId));
    }
    String parentPath = "/" + root + "/host_1";
    boolean[] success =
        accessor.createChildren(parentPath, records, BaseDataAccessor.Option.PERSISTENT);
    System.out.println("create:" + Arrays.toString(success));
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);

    parentPath = "/" + root + "/host_1";
    records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key1", "value1");
      records.add(newRecord);
    }
    success = accessor.setChildren(parentPath, records, Option.PERSISTENT);

    System.out.println("set:" + Arrays.toString(success));
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);

    // test async updateChildren
    parentPath = "/" + root + "/host_1";
    records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key2", "value2");
      records.add(newRecord);
    }
    success = accessor.updateChildren(parentPath, records, Option.PERSISTENT);
    System.out.println("update:" + Arrays.toString(success));
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);

    // test get (from cache)
    parentPath = "/" + root + "/host_1";
    records = accessor2.getChildren(parentPath, 0);
    System.out.println("get:" + records);

    zkClient.close();
  }
}
