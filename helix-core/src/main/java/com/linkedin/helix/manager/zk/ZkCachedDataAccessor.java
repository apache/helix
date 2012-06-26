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

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.BaseDataAccessor.Option;
import com.linkedin.helix.IZkListener;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.store.HelixPropertyListener;
import com.linkedin.helix.store.zk.ZNode;

public class ZkCachedDataAccessor implements IZkListener
{
  private static final Logger                   LOG        =
                                                               Logger.getLogger(ZkCachedDataAccessor.class);

  protected ConcurrentMap<String, ZNode>        _map       =
                                                               new ConcurrentHashMap<String, ZNode>();
  ReadWriteLock                                 _lock      = new ReentrantReadWriteLock();

  final String                                  _root;
  final ZkBaseDataAccessor                      _accessor;
  final List<String>                            _subscribedPaths;

  final Map<String, Set<HelixPropertyListener>> _listeners =
                                                               new ConcurrentHashMap<String, Set<HelixPropertyListener>>();

  public ZkCachedDataAccessor(ZkBaseDataAccessor accessor, List<String> subscribedPaths)
  {
    _root = "";
    _accessor = accessor;
    _subscribedPaths = subscribedPaths;

    for (String path : subscribedPaths)
    {
      updateCacheRecursive(path);
    }
  }

  public ZkCachedDataAccessor(ZkBaseDataAccessor accessor,
                              String root,
                              List<String> subscribedPaths)
  {
    _root = root;
    _accessor = accessor;
    _subscribedPaths = subscribedPaths;

    for (String absPath : subscribedPaths)
    {
      updateCacheRecursive(absPath);
    }
  }

  String getAbsolutePath(String path)
  {
    return path.equals("/")? _root : new File(_root, path).getAbsolutePath();
  }

  List<String> getAbsolutePath(List<String> paths)
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
    if (!absPath.startsWith(_root))
    {
      throw new IllegalArgumentException("Illegal absPath: " + absPath
          + ", did NOT start with " + _root);
    }
    return absPath.equals(_root)? "/" : absPath.substring(_root.length());
  }

  List<String> getAbsolutPath(List<String> paths)
  {
    List<String> absPaths = new ArrayList<String>();
    for (String path : paths)
    {
      absPaths.add(getAbsolutePath(path));
    }
    return absPaths;
  }

  boolean isSubscribed(String absPath)
  {
    boolean ret = false;

    while (absPath != null && absPath.startsWith(_root))
    {
      if (_subscribedPaths.contains(absPath))
      {
        ret = true;
        break;
      }
      absPath = new File(absPath).getParent();
    }

    return ret;
  }

  void updateCacheRecursive(String absPath)
  {
    if (!isSubscribed(absPath))
    {
      return;
    }

    try
    {
      _lock.writeLock().lock();

      _accessor.subscribe(absPath, this);

      // update parent's childSet
      String parentPath = new File(absPath).getParent();
      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        String name = new File(absPath).getName();
        zNode.addChild(name);
      }

      // update this node
      Stat stat = new Stat();
      ZNRecord readData = _accessor.get(absPath, stat, 0);
      if (!_map.containsKey(absPath))
      {
        _map.put(absPath, new ZNode(absPath, readData, stat));
        // System.out.println("fire NodeCreated:" + path);
        fireListeners(absPath, EventType.NodeCreated);
      }
      else
      {
        // if in cache, and create timestamp is different
        // that indicates at least 1 delete and 1 create
        Stat oldStat = _map.get(absPath).getStat();
        if (oldStat.getCzxid() != stat.getCzxid())
        {
          fireListeners(absPath, EventType.NodeDeleted);
          fireListeners(absPath, EventType.NodeCreated);
        } else if (oldStat.getVersion() != stat.getVersion())
        {
          fireListeners(absPath, EventType.NodeDataChanged);
        }
      }

      zNode = _map.get(absPath);
      zNode.setData(readData);
      zNode.setStat(stat);

      // recursively update children nodes
      List<String> childNames = _accessor.getChildNames(absPath, 0);
      for (String childName : childNames)
      {
        String childPath = absPath + "/" + childName;
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

  private void updateCacheAlongPath(String absPath, boolean success)
  {
    int idx = absPath.indexOf('/', _root.length());
    while (idx > 0)
    {
      String tmpAbsPath = absPath.substring(0, idx);
      if (isSubscribed(tmpAbsPath) && !_map.containsKey(tmpAbsPath))
      {
        break;
      }
      idx = absPath.indexOf('/', idx + 1);
    }

    if (idx > 0)
    {
      updateCacheRecursive(absPath.substring(0, idx));
    }
    else
    {
      if (success)
      {
        updateCacheRecursive(absPath);
      }
    }
  }

  private void purgeCacheRecursive(String absPath)
  {
    try
    {
      _lock.writeLock().lock();

      // remove from parent's childSet
      String parentPath = new File(absPath).getParent();
      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        String name = new File(absPath).getName();
        zNode.removeChild(name);
      }

      // recursively remove children nodes
      zNode = _map.remove(absPath);
      if (zNode != null)
      {
        fireListeners(absPath, EventType.NodeDeleted);
        Set<String> childNames = zNode.getChild();
        for (String childName : childNames)
        {
          String childPath = absPath + "/" + childName;
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

      String absPath = getAbsolutePath(path);
      boolean success = _accessor.create(absPath, record, options);
      updateCacheAlongPath(absPath, success);
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

      String absPath = getAbsolutePath(path);
      boolean success = _accessor.set(absPath, record, options);
      updateCacheAlongPath(absPath, success);
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
      String absPath = getAbsolutePath(path);
      boolean success = _accessor.update(absPath, record, options);
      updateCacheAlongPath(absPath, success);
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
      String absPath = getAbsolutePath(path);
      boolean success = _accessor.remove(absPath);
      if (isSubscribed(absPath))
      {
        purgeCacheRecursive(absPath);
      }
      return success;
    }
    finally
    {
      _lock.writeLock().unlock();

    }
  }

//  private void updateCacheAlongPath(String absParentPath,
//                                    List<ZNRecord> records,
//                                    boolean[] success)
//  {
//    int idx = absParentPath.indexOf('/', _root.length());
//    while (idx > 0)
//    {
//      String tmpAbsPath = absParentPath.substring(0, idx);
//      if (isSubscribed(tmpAbsPath) && !_map.containsKey(tmpAbsPath))
//      {
//        break;
//      }
//      idx = absParentPath.indexOf('/', idx + 1);
//    }
//
//    if (idx > 0)
//    {
//      updateCacheRecursive(absParentPath.substring(0, idx));
//    }
//    else
//    {
//      for (int i = 0; i < records.size(); i++)
//      {
//        if (success[i])
//        {
//          String path = absParentPath + "/" + records.get(i).getId();
//          updateCacheRecursive(path);
//        }
//      }
//    }
//  }

  private void updateCacheAlongPath(List<String> absPaths,
                                  boolean[] success)
  {
    for (int i = 0; i < absPaths.size(); i++)
    {
      updateCacheAlongPath(absPaths.get(i), success[i]);
    }
  }
  
  public boolean[] createChildren(List<String> paths, List<ZNRecord> records, int options)
  {
    try
    {
      _lock.writeLock().lock();
      List<String> absPaths = getAbsolutePath(paths);
      boolean[] success = _accessor.createChildren(absPaths, records, options);
      updateCacheAlongPath(absPaths, success);
      return success;
    }
    finally
    {
      _lock.writeLock().unlock();

    }
  }

  public boolean[] setChildren(List<String> paths, List<ZNRecord> records, int options)
  {
    try
    {
      _lock.writeLock().lock();
      List<String> absPaths = getAbsolutePath(paths);
      boolean[] success = _accessor.setChildren(absPaths, records, options);
      updateCacheAlongPath(absPaths, success);

      return success;
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean[] updateChildren(List<String> paths, List<ZNRecord> records, int options)
  {
    try
    {
      _lock.writeLock().lock();
      List<String> absPaths = getAbsolutePath(paths);
      boolean[] success = _accessor.updateChildren(absPaths, records, options);

      updateCacheAlongPath(absPaths, success);
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
      List<String> absPaths = getAbsolutPath(paths);
      boolean[] success = _accessor.remove(absPaths);

      for (String absPath : absPaths)
      {
        if (isSubscribed(absPath))
        {
          purgeCacheRecursive(absPath);
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
    String absPath = getAbsolutePath(path);
    if (isSubscribed(absPath))
    {
      ZNRecord record = null;
      ZNode zNode = _map.get(absPath);
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
      return _accessor.get(absPath, stat, options);
    }

  }

  public List<ZNRecord> get(List<String> paths, int options)
  {
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    List<String> absPaths = getAbsolutPath(paths);
    for (String absPath : absPaths)
    {
      records.add(_accessor.get(absPath, null, options));
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

    // remove null children
    List<ZNRecord> records = get(paths, options);
    Iterator<ZNRecord> iter = records.iterator();
    while (iter.hasNext())
    {
      if (iter.next() == null)
      {
        iter.remove();
      }
    }
    return records;
  }

  public List<String> getChildNames(String parentPath, int options)
  {
    String absParentPath = getAbsolutePath(parentPath);
    if (isSubscribed(absParentPath))
    {
      ZNode zNode = _map.get(absParentPath);
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
      return _accessor.getChildNames(absParentPath, options);
    }
  }

  public boolean exists(String path)
  {
    String absPath = getAbsolutePath(path);
    if (isSubscribed(absPath))
    {
      return _map.containsKey(absPath);
    }
    else
    {
      return _accessor.exists(absPath);
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

  public Stat getStat(String path)
  {
    String absPath = getAbsolutePath(path);
    if (isSubscribed(absPath))
    {
      ZNode zNode = _map.get(absPath);
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
      return _accessor.getStat(absPath);
    }
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

  public void subscribe(String parentPath, final HelixPropertyListener listener)
  {
    synchronized (_listeners)
    {
      String absParentPath = getAbsolutePath(parentPath);

      if (!isSubscribed(absParentPath))
      {
        LOG.debug("Subscribed changes for parentPath: " + absParentPath);
        updateCacheRecursive(absParentPath);
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

      if (!isSubscribed(absParentPath))
      {
        LOG.debug("Unsubscribed changes for pathPrefix: " + absParentPath);
        purgeCacheRecursive(absParentPath);
      }
    }
  }

  private void fireListeners(String absPath, EventType type)
  {
    synchronized (_listeners)
    {
      String path = getRelativePath(absPath);
      String tmpAbsPath = new String(absPath);
      while (tmpAbsPath != null && tmpAbsPath.startsWith(_root))
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
//    System.out.println("DataChange:" + dataPath);
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
//    System.out.println("DataDelete:" + dataPath);

    try
    {
      _lock.writeLock().lock();
      _map.remove(dataPath);

      _accessor.unsubscribe(dataPath, this);

      String parentPath = new File(dataPath).getParent();

      // remove child from parent's childSet
      // parent stat will also be changed. parent's stat will be updated in
      // parent's childChange callback
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
    // System.out.println(this + ":" + "ChildChange: " + currentChilds);
//    System.out.println("ChildChange:" + parentPath);

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
        new ZkCachedDataAccessor(new ZkBaseDataAccessor(zkClient), list);
    System.out.println("accessor1:" + accessor);

    ZkClient zkClient2 = new ZkClient(zkAddr);
    zkClient2.setZkSerializer(new ZNRecordSerializer());
    ZkCachedDataAccessor accessor2 =
        new ZkCachedDataAccessor(new ZkBaseDataAccessor(zkClient2), list);
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
    List<String> paths = new ArrayList<String>();
    String parentPath = "/" + root + "/host_1";

    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      paths.add(parentPath + "/" + msgId);
      records.add(new ZNRecord(msgId));
    }
    
    boolean[] success =
        accessor.createChildren(paths, records, BaseDataAccessor.Option.PERSISTENT);
    System.out.println("create:" + Arrays.toString(success));
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);

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
    success = accessor.setChildren(paths, records, Option.PERSISTENT);

    System.out.println("set:" + Arrays.toString(success));
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);

    // test async updateChildren
    paths = new ArrayList<String>();
    parentPath = "/" + root + "/host_1";
    records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key2", "value2");
      paths.add(parentPath + "/" + msgId);
      records.add(newRecord);
    }
    success = accessor.updateChildren(paths, records, Option.PERSISTENT);
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
