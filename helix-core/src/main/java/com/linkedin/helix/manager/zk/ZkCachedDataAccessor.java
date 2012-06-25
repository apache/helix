package com.linkedin.helix.manager.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;
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
  private static final Logger LOG = Logger
      .getLogger(ZkCachedDataAccessor.class);

  ConcurrentMap<String, ZNode> _map = new ConcurrentHashMap<String, ZNode>();
  ReadWriteLock _lock = new ReentrantReadWriteLock();

  final String _root;
  final ZkBaseDataAccessor _accessor;
  final List<String> _subscribedPaths;

  public ZkCachedDataAccessor(ZkBaseDataAccessor accessor,
      List<String> subscribedPaths)
  {
    _root = "/";
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

  // private boolean accept(String path)
  // {
  // String dir = new File(path).getParent();
  // String name = new File(path).getName();
  // return _filter.accept(new File(dir), name);
  // }

  // private void updateCache(String path)
  // {
  // if (!accept(path))
  // {
  // return;
  // }
  //
  // try
  // {
  // _lock.writeLock().lock();
  //
  // _accessor.subscribe(path, this);
  //
  // Stat stat = new Stat();
  // ZNRecord readData = _accessor.get(path, stat, 0);
  //
  // _map.putIfAbsent(path, new ZNode(path, readData, stat));
  //
  // // only update parent's child set
  // // parent's stat will be updated by parent's childchange callback
  // String parentDir = path.substring(0, path.lastIndexOf('/'));
  // ZNode parent = _map.get(parentDir);
  // if (parent != null)
  // {
  // parent.addChild(path.substring(path.lastIndexOf('/') + 1));
  // }
  // }
  // finally
  // {
  // _lock.writeLock().unlock();
  // }
  //
  // }

  void updateCacheRecursive(String path)
  {
    if (!isSubscribed(path))
    {
      return;
    }

    try
    {
      Stat stat = new Stat();
      _lock.writeLock().lock();

      _accessor.subscribe(path, this);

      ZNRecord readData = _accessor.get(path, stat, 0);
      if (!_map.containsKey(path))
      {
        _map.put(path, new ZNode(path, readData, stat));
      }

      ZNode zNode = _map.get(path);
      zNode.setData(readData);
      zNode.setStat(stat);

      List<String> children = _accessor.getChildNames(path, 0);
      for (String child : children)
      {
        String childPath = path + "/" + child;
        if (!zNode.hasChild(child))
        {
          zNode.addChild(child);
          updateCacheRecursive(childPath);
        }
      }

    } catch (ZkNoNodeException e)
    {
      // OK
    } finally
    {
      _lock.writeLock().unlock();
    }
  }

  // private void createRecursive(String path)
  // {
  // try
  // {
  // _zkClient.create(path, null, CreateMode.PERSISTENT);
  // updateCache(path);
  // }
  // catch (ZkNodeExistsException e)
  // {
  // // OK
  // }
  // catch (ZkNoNodeException e)
  // {
  // String parentDir = path.substring(0, path.lastIndexOf('/'));
  // createRecursive(parentDir);
  // createRecursive(path);
  // }
  //
  // }

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
    } else
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
      ZNode zNode = _map.remove(path);
      if (zNode != null)
      {
        Set<String> childNames = zNode.getChild();
        for (String childName : childNames)
        {
          String childPath = path + "/" + childName;
          purgeCacheRecursive(childPath);
        }
      }
    } finally
    {
      _lock.writeLock().unlock();
    }

  }

  private void purgeCahce(String path)
  {
    try
    {
      _lock.writeLock().lock();

      // remove from parent's childSet
      String parentPath = new File(path).getParent();
      String child = new File(path).getName();
      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        zNode.removeChild(child);
      }

      // remove node and all children recursively
      purgeCacheRecursive(path);
    } finally
    {
      _lock.writeLock().unlock();
    }

  }

  public boolean create(String path, ZNRecord record, int options)
  {
    boolean success = _accessor.create(path, record, options);

    // update cache
    updateCacheAlongPath(path, success);

    return success;
  }

  public boolean set(String path, ZNRecord record, int options)
  {
    boolean success = _accessor.set(path, record, options);

    updateCacheAlongPath(path, success);

    return success;
  }

  public boolean update(String path, ZNRecord record, int options)
  {
    boolean success = _accessor.update(path, record, options);
    updateCacheAlongPath(path, success);

    return success;
  }

  public boolean remove(String path)
  {
    boolean success = _accessor.remove(path);

    if (isSubscribed(path))
    {
      purgeCahce(path);
    }

    return success;
  }

  // /**
  // * sync create parent and async create child. used internally when fail on
  // NoNode
  // *
  // * @param parentPath
  // * @param records
  // * @param success
  // * @param mode
  // * @param cbList
  // */
  // private void createChildren(String parentPath,
  // List<ZNRecord> records,
  // boolean[] success,
  // CreateMode mode,
  // DefaultCallback[] cbList)
  // {
  // createRecursive(parentPath);
  //
  // CreateCallbackHandler[] createCbList = new
  // CreateCallbackHandler[records.size()];
  // for (int i = 0; i < records.size(); i++)
  // {
  // DefaultCallback cb = cbList[i];
  // if (Code.get(cb.getRc()) != Code.NONODE)
  // {
  // continue;
  // }
  //
  // ZNRecord record = records.get(i);
  // String path = parentPath + "/" + record.getId();
  // createCbList[i] = new CreateCallbackHandler();
  // _zkClient.asyncCreate(path, record, mode, createCbList[i]);
  // }
  //
  // for (int i = 0; i < createCbList.length; i++)
  // {
  // CreateCallbackHandler createCb = createCbList[i];
  // if (createCb != null)
  // {
  // createCb.waitForSuccess();
  // success[i] = (createCb.getRc() == 0);
  // switch (Code.get(createCb.getRc()))
  // {
  // case OK:
  // String path = parentPath + "/" + records.get(i).getId();
  // updateCache(path);
  // break;
  // default:
  // break;
  // }
  // }
  // }
  // }

  private void updateCacheAlongPath(String parentPath, List<ZNRecord> records,
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
    } else
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

  public boolean[] createChildren(String parentPath, List<ZNRecord> records,
      int options)
  {
    boolean[] success = _accessor.createChildren(parentPath, records, options);

    // update cache
    updateCacheAlongPath(parentPath, records, success);

    return success;
  }

  public boolean[] setChildren(String parentPath, List<ZNRecord> records,
      int options)
  {
    boolean[] success = _accessor.setChildren(parentPath, records, options);

    updateCacheAlongPath(parentPath, records, success);

    return success;
  }

  public boolean[] updateChildren(String parentPath, List<ZNRecord> records,
      int options)
  {
    boolean[] success = _accessor.updateChildren(parentPath, records, options);

    updateCacheAlongPath(parentPath, records, success);
    return success;
  }

  public boolean[] remove(List<String> paths)
  {
    boolean[] success = _accessor.remove(paths);

    for (String path : paths)
    {
      if (isSubscribed(path))
      {
        purgeCahce(path);
      }
    }

    return success;
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
        record = (ZNRecord) zNode.getData();
        if (stat != null)
        {
          // TODO: copy zNode.getStat() to stat
        }
      }
      return record;
    } else
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

  public List<String> getChildNames(String parentPath, int options)
  {
    if (isSubscribed(parentPath))
    {
      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        return new ArrayList<String>(zNode.getChild());
      } else
      {
        return Collections.emptyList();
      }
    } else
    {
      return _accessor.getChildNames(parentPath, options);
    }
  }

  public boolean exists(String path)
  {
    if (isSubscribed(path))
    {
      return _map.containsKey(path);
    } else
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
      } else
      {
        return null;
      }
    } else
    {
      return _accessor.getStat(path);
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
        zNode.setStat(stat);
      }
    } finally
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
      // parent stat will also be changed. stat will be updated in parent's
      // childChange
      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        String child = new File(dataPath).getName();
        zNode.removeChild(child);
      }
    } finally
    {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds)
      throws Exception
  {
    // System.out.println("ChildChange: " + currentChilds);
    if (currentChilds == null)
    {
      return;
    }

    updateCacheRecursive(parentPath);
  }

  // /**
  // * Start from path and go up to _root, return true if any path is subscribed
  // *
  // * @param path
  // * : path shall always start with _root
  // * @return
  // */
  // boolean isSubscribed(String path)
  // {
  // boolean ret = false;
  // synchronized (_subscribedPaths)
  // {
  // while (path.length() >= _root.length())
  // {
  // if (_subscribedPaths.contains(path))
  // {
  // ret = true;
  // break;
  // }
  // path = path.substring(0, path.lastIndexOf('/'));
  // }
  // }
  // return ret;
  // }

  // /**
  // * subscribe will put the path and all its children in cache
  // *
  // * @param parentPath
  // */
  // public void subscribe(String parentPath)
  // {
  // synchronized (_subscribedPaths)
  // {
  // boolean needUpdateCache = !isSubscribed(parentPath);
  // if (!_subscribedPaths.contains(parentPath))
  // {
  // _subscribedPaths.add(parentPath);
  // }
  //
  // if (needUpdateCache)
  // {
  // LOG.debug("Subscribe for parentPath: " + parentPath);
  // updateCacheRecursive(parentPath);
  // }
  // }
  // }

  public static void main(String[] args) throws Exception
  {
    String zkAddr = "localhost:2191";
    String root = "TestCDA";
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.deleteRecursive("/" + root);

    List<String> list = new ArrayList<String>();
    list.add("/" + root);

    ZkCachedDataAccessor accessor = new ZkCachedDataAccessor(
        new ZkBaseDataAccessor(zkClient), list);

    ZkClient zkClient2 = new ZkClient(zkAddr);
    zkClient2.setZkSerializer(new ZNRecordSerializer());
    ZkCachedDataAccessor accessor2 = new ZkCachedDataAccessor(
        new ZkBaseDataAccessor(zkClient2), list);

    System.out.println("cache: " + accessor._map);
    System.out.println("cache2: " + accessor2._map);

    // test sync create will update cache
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = "/" + root + "/host_0/" + msgId;
      boolean success = accessor.create(path, new ZNRecord(msgId),
          BaseDataAccessor.Option.PERSISTENT);
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

    // test createChildren()
    List<ZNRecord> records = new ArrayList<ZNRecord>();

    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      records.add(new ZNRecord(msgId));
    }
    String parentPath = "/" + root + "/host_1";
    boolean[] success = accessor.createChildren(parentPath, records,
        BaseDataAccessor.Option.PERSISTENT);
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
