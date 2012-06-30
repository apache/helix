package com.linkedin.helix.store.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;

import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.util.ZKClientPool;

public class ZkCache implements IZkChildListener, IZkDataListener, IZkStateListener
{
  private static final Logger                LOG     = Logger.getLogger(ZkCache.class);

  String                                     _rootPath;
  ZkClient                                   _client;
  ConcurrentMap<String, ZNode>               _map    =
                                                         new ConcurrentHashMap<String, ZNode>();
  ReadWriteLock                              _lock   = new ReentrantReadWriteLock();

  // TODO: debug code, remove it later
  final static ConcurrentLinkedQueue<String> changes =
                                                         new ConcurrentLinkedQueue<String>();
  final ZkListener                           _listener;

  public ZkCache(String path, ZkClient client, ZkListener listener)
  {
    super();
    _rootPath = path;
    _client = client;
    _listener = listener;
    // init();
  }

  public void init()
  {
    updateCache(_rootPath);
  }

  boolean purgeCache(String path)
  {
    try
    {
      _lock.writeLock().lock();

      changes.add(path + "-" + "cachepurge" + "-" + System.currentTimeMillis());

      List<String> children;
      try
      {
        // TODO optimize it by using cache
        children = _client.getChildren(path);
      }
      catch (ZkNoNodeException e)
      {
        return true;
      }

      // purge cache recursively
      for (String subPath : children)
      {
        if (!purgeCache(path + "/" + subPath))
        {
          return false;
        }
      }

      _map.remove(path);
      _client.unsubscribeChildChanges(path, this);
      _client.unsubscribeDataChanges(path, this);

      // if a node gets purged, none of its parents should be in cache
      return true;
    }
    finally
    {
      _lock.writeLock().unlock();
    }

  }

  public void updateCache(String path)
  {
    try
    {
      boolean isCreate = false;
      boolean isDelete = false;

      Stat stat = new Stat();
      _lock.writeLock().lock();

      changes.add(path + "-" + "updatecache" + "-");

      _client.subscribeChildChanges(path, this);
      _client.subscribeDataChanges(path, this);
      Object readData = _client.readData(path, stat);
      if (!_map.containsKey(path))
      {
        _map.put(path, new ZNode(path, readData, stat));
        changes.add(path + "-" + "updatecache2-" + "setcid" + stat.getCzxid());
        isCreate = true;
      }
      else
      {

        // if in cache, and create timestamp is different
        // that indicates at least 1 delete before a create
        Stat oldStat = _map.get(path).getStat();
        changes.add(path + "-" + "updatecache3-" + "oldcid" + oldStat.getCzxid()
            + "-newcid" + stat.getCzxid());
        if (oldStat.getCzxid() != stat.getCzxid())
        {
          changes.add(path + "-" + "updatecache4");
          isDelete = true;
          isCreate = true;
        }
      }

      ZNode zNode = _map.get(path);
      zNode.setData(readData);

      // changes.add(path + "-" + "updatecache5-" + "setcid" +
      // stat.getCzxid());
      zNode.setStat(stat);

      List<String> children = _client.getChildren(path);
      for (String child : children)
      {
        String childPath = path + "/" + child;
        if (!zNode.hasChild(child))
        {
          zNode.addChild(child);
          updateCache(childPath);
        }
      }

      // fire listeners
      if (isDelete)
      {
        changes.add(path + "-deletecallback");
        if (_listener != null)
        {
          _listener.handleNodeDelete(path);
        }
      }

      if (isCreate)
      {
        changes.add(path + "-createcallback");
        if (_listener != null)
        {
          _listener.handleNodeCreate(path);
        }
      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception
  {
    System.out.println("Handle handleDataChange:" + dataPath);
    // TODO: sync on path
    try
    {
      boolean isChange = false;
      boolean isDelete = false;

      _lock.writeLock().lock();

      changes.add(dataPath + "-" + "handledatachange" + "-" + data + "-"
          + System.currentTimeMillis());

      ZNode zNode = _map.get(dataPath);
      if (zNode != null)
      {
        // TODO: optimize it by get stat from callback
        Stat stat = new Stat();
        Object readData = _client.readData(dataPath, stat);

        zNode.setData(readData);
        Stat oldStat = zNode.getStat();

        changes.add(dataPath + "-" + "handledatachange1-" + "setcid" + stat.getCzxid());
        zNode.setStat(stat);

        changes.add(dataPath + "-" + "handledatachange2" + "-oldcid" + oldStat.getCzxid()
            + "-newcid" + stat.getCzxid());

        isChange = true;
        if (oldStat.getCzxid() != stat.getCzxid())
        {
          isDelete = true;
        }
      }
      else
      {
        // we may see dataChange on child before childChange on parent
        // in this case, let childChange update cache
        System.out.println("null in handleDataChange");
        printChangesFor(dataPath);
        String parent = dataPath.substring(0, dataPath.lastIndexOf('/'));
        printChangesFor(parent);
      }

      // fire listeners
      if (isDelete)
      {
        changes.add(dataPath + "-deletecallback");
        if (_listener != null)
        {
          _listener.handleNodeDelete(dataPath);
        }
      }

      if (isChange)
      {
        changes.add(dataPath + "-datachangecallback");
        if (_listener != null)
        {
          _listener.handleDataChange(dataPath);
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
    System.out.println("Handle handleDataDeleted:" + dataPath);
    // TODO: sync on path
    try
    {
      _lock.writeLock().lock();

      changes.add(dataPath + "-" + "handledatadeleted" + "-" + System.currentTimeMillis());

      _map.remove(dataPath);

      _client.unsubscribeChildChanges(dataPath, this);
      _client.unsubscribeDataChanges(dataPath, this);

      String parentPath = dataPath.substring(0, dataPath.lastIndexOf('/'));

      ZNode zNode = _map.get(parentPath);
      if (zNode != null)
      {
        String name = dataPath.substring(dataPath.lastIndexOf('/') + 1);
        System.out.println("Removing child:" + name + " from parent:" + parentPath
            + " childSet:" + zNode._childSet);
        changes.add(parentPath + "-removechild-" + name + "-" + zNode._childSet);
        zNode._childSet.remove(name);
      }
      else
      {
        // we may see dataDeleted change on child before on parent
        System.out.println("null in handleDataDelete");
        printChangesFor(dataPath);
        printChangesFor(parentPath);
      }
    }
    catch (ZkNoNodeException e)
    {
      // OK
    }
    finally
    {
      // fire listeners
      changes.add(dataPath + "-deletecallback");
      if (_listener != null)
      {
        _listener.handleNodeDelete(dataPath);
      }
      changes.add(dataPath + "-" + "unsubscribe" + "-" + System.currentTimeMillis());

      _lock.writeLock().unlock();
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChildsOnZk) throws Exception
  {
    System.out.println("Handle handleChildChange:" + parentPath + "-" + currentChildsOnZk);
    if (currentChildsOnZk == null)
    {
      return;
    }

    changes.add(parentPath + "-" + "handlechildchange" + "-" + currentChildsOnZk + "-"
        + System.currentTimeMillis());

    try
    {
      _lock.writeLock().lock();

      ZNode zNode = _map.get(parentPath);
      if (zNode == null)
      {
        // no subscription available
        return;
      }
      updateCache(parentPath);
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

  void createRecursive(String path)
  {
    try
    {
      _client.subscribeChildChanges(path, this);
      _client.subscribeDataChanges(path, this);
      _client.create(path, null, CreateMode.PERSISTENT);

      // TODO: optimize it by get stat from create()
      Stat stat = new Stat();
      Object readData = _client.readData(path, stat);

      _map.putIfAbsent(path, new ZNode(path, readData, stat));

      String parentDir = path.substring(0, path.lastIndexOf('/'));
      ZNode parent = _map.get(parentDir);
      if (parent != null)
      {
        parent.addChild(path.substring(path.lastIndexOf('/') + 1));
      }
    }
    catch (ZkNodeExistsException e)
    {
      // OK
    }
    catch (ZkNoNodeException e)
    {
      String parentDir = path.substring(0, path.lastIndexOf('/'));
      createRecursive(parentDir);
      createRecursive(path);
    }

  }

  public boolean set(String path, Object data)
  {
    // TODO sync on key
    try
    {
      _lock.writeLock().lock();
      System.out.println("Writing key: " + path);

      // TODO: there may be a delete between create() and write()
      createRecursive(path);
      _client.writeData(path, data);

      // TODO: optimize it by get stat from write()
      Stat stat = new Stat();
      Object readData = _client.readData(path, stat);

      ZNode zNode = _map.get(path);
      if (zNode != null)
      {
        zNode.setData(readData);
        zNode.setStat(stat);
      }
      else
      {
        _map.put(path, new ZNode(path, readData, stat));
      }

      String parentPath = path.substring(0, path.lastIndexOf('/'));
      ZNode parentNode = _map.get(parentPath);
      if (parentNode != null)
      {
        // TODO: optimize it by using cache
        Stat parentStat = new Stat();
        Object readParentData = _client.readData(parentPath, parentStat);
        parentNode.setData(readParentData);
        parentNode.setStat(parentStat);
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
    return true;
  }

  public Object get(String path, Stat stat)
  {
    try
    {
      _lock.readLock().lock();
      ZNode zNode = _map.get(path);
      if (zNode == null)
      {
        return null;
      }

      if (stat != null)
      {
        DataTree.copyStat(zNode.getStat(), stat);
      }
      // TODO: we should return a copy of data instead of ref in cache
      return zNode._data;
    }
    finally
    {
      _lock.readLock().unlock();
    }
  }

  void getKeysRecursive(String path, List<String> keys)
  {
    ZNode node = _map.get(path);
    if (node != null)
    {
      keys.add(path);
      Set<String> childs = node._childSet;
      for (String child : childs)
      {
        String childPath = path.equals("/") ? path + child : path + "/" + child;
        getKeysRecursive(childPath, keys);
      }

    }
  }

  public List<String> getKeys(String path)
  {
    List<String> keys = new ArrayList<String>();
    try
    {
      _lock.readLock().lock();
      getKeysRecursive(path, keys);

      return keys;
    }
    finally
    {
      _lock.readLock().unlock();
    }
  }

  boolean removeRecursive(String path)
  {
    List<String> children;
    try
    {
      children = _client.getChildren(path);
    }
    catch (ZkNoNodeException e)
    {
      return true;
    }

    for (String subPath : children)
    {
      if (!removeRecursive(path + "/" + subPath))
      {
        return false;
      }
    }

    boolean succeed = _client.delete(path);
    _map.remove(path);
    String parentPath = path.substring(0, path.lastIndexOf('/'));

    ZNode zNode = _map.get(parentPath);
    if (zNode != null)
    {
      String name = path.substring(path.lastIndexOf('/') + 1);
      System.out.println("Removing child:" + name + " from parent:"
          + parentPath + ", child set:" + zNode._childSet);
      changes.add(parentPath + "-cacheremovechild-" + name + "-" + zNode._childSet);
      zNode._childSet.remove(name);
    }

    return succeed;
  }

  public boolean remove(String key)
  {
    try
    {
      _lock.writeLock().lock();

      // unsubscribe from zk will be done in dataDelete callback
      removeRecursive(key);

      String parentPath = key.substring(0, key.lastIndexOf('/'));
      ZNode parentNode = _map.get(parentPath);
      if (parentNode != null)
      {
        // TODO: optimize it
        Stat parentStat = new Stat();
        Object readParentData = _client.readData(parentPath, parentStat);
        parentNode.setData(readParentData);
        parentNode.setStat(parentStat);
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
    return true;
  }

  public void updateSerialized(String path,
                               DataUpdater<Object> updater,
                               boolean createIfAbsent)
  {
    Stat stat = new Stat();
    boolean retry;
    Object oldData = null;

    try
    {
      _lock.writeLock().lock();

      do
      {
        retry = false;
        try
        {
          oldData = _client.readData(path, stat);
          Object newData = updater.update(oldData);
          _client.writeData(path, newData, stat.getVersion());
        }
        catch (ZkBadVersionException e)
        {
          retry = true;
        }
        catch (ZkNoNodeException e)
        {
          if (createIfAbsent)
          {
            retry = true;
            _client.createPersistent(path, true);
          }
          else
          {
            throw e;
          }
        }
      }
      while (retry);

      // optimize it
      try
      {
        Object readData = _client.readData(path, stat);
        ZNode zNode = _map.get(path);
        zNode.setData(readData);
        zNode.setStat(stat);
      } catch (ZkNoNodeException e)
      {
        // someone may delete it after we update
        // OK
      }
    }
    finally
    {
      _lock.writeLock().unlock();
    }
  }

  public boolean exists(String path)
  {
    return _map.containsKey(path);
  }
  
  public static void main(String[] args) throws Exception
  {
    final String rootNamespace = "/testZkCache";
    String zkAddress = "localhost:2191";
    // ZkServer server = startZkSever(zkAddress, rootNamespace);
    ZkClient client = new ZkClient(zkAddress);
    client.deleteRecursive(rootNamespace);

    int count = 0;
    int maxDepth = 10;
    String delim = "/";
    while (count < 100)
    {
      int depth = ((int) (Math.random() * 10000)) % maxDepth + 1;
      StringBuilder sb = new StringBuilder(rootNamespace);
      for (int i = 0; i < depth; i++)
      {
        int childId = ((int) (Math.random() * 10000)) % 5;
        sb.append(delim).append("child-" + childId);
      }
      String key = sb.toString();
      String val = key;

      String keyToCreate = key;
      while (keyToCreate.startsWith(rootNamespace))
      {
        if (client.exists(keyToCreate))
        {
          break;
        }
        changes.add(keyToCreate + "-" + "create" + "-" + System.currentTimeMillis());
        keyToCreate = keyToCreate.substring(0, keyToCreate.lastIndexOf('/'));
      }

      client.createPersistent(key, true);

      System.out.println("Writing key:" + key);
      client.writeData(key, val);
      count = count + 1;
      changes.add(key + "-" + "write" + "-" + System.currentTimeMillis());
    }

    ZkCache cache = new ZkCache(rootNamespace, client, null);
    cache.init();
    verify(cache, client, rootNamespace);
    System.out.println("init verification done. changes: " + changes.size());

    count = 0;
    int newWrites = 0;
    int updates = 0;
    int deletion = 0;
    while (count < 100)
    {
      int depth = ((int) (Math.random() * 10000)) % maxDepth + 1;
      StringBuilder sb = new StringBuilder(rootNamespace);
      for (int i = 0; i < depth; i++)
      {
        int childId = ((int) (Math.random() * 10000)) % 5;
        sb.append(delim).append("child-" + childId);
      }
      String key = sb.toString();
      String val = key;
      if (!client.exists(key))
      {
        String keyToCreate = key;
        while (keyToCreate.startsWith(rootNamespace))
        {
          if (client.exists(keyToCreate))
          {
            break;
          }
          changes.add(keyToCreate + "-" + "create" + "-" + System.currentTimeMillis());
          keyToCreate = keyToCreate.substring(0, keyToCreate.lastIndexOf('/'));
        }

        client.createPersistent(key, true);

        System.out.println("Writing key:" + key);
        client.writeData(key, val);
        changes.add(key + "-" + "write" + "-" + System.currentTimeMillis());

        newWrites++;
      }
      else
      {
        int op = ((int) (Math.random() * 10000)) % 2;
        if (op == 0)
        {
          System.out.println("Deleting key:" + key);

          Map<String, ZNode> toDelete = new HashMap<String, ZNode>();
          read(toDelete, client, key);
          for (String child : toDelete.keySet())
          {
            changes.add(child + "-" + "delete" + "-" + System.currentTimeMillis());
          }

          client.deleteRecursive(key);
          deletion++;
        }
        else
        {
          System.out.println("Updating key:" + key);
          Object data = client.readData(key);

          String object = (data != null ? data.toString() : key) + "-updated";
          client.writeData(key, object);
          changes.add(key + "-" + "write" + "-" + System.currentTimeMillis());
          updates++;
        }
      }
      count = count + 1;
    }

    System.out.println("newWrites:" + newWrites + " updates:" + updates + " deletions:"
        + deletion);
    Thread.sleep(5000);

    verify(cache, client, rootNamespace);

    count = 0;
    newWrites = 0;
    updates = 0;
    deletion = 0;
    while (count < 100)
    {
      int depth = ((int) (Math.random() * 10000)) % maxDepth + 1;
      StringBuilder sb = new StringBuilder(rootNamespace);
      for (int i = 0; i < depth; i++)
      {
        int childId = ((int) (Math.random() * 10000)) % 5;
        sb.append(delim).append("child-" + childId);
      }
      final String key = sb.toString();
      String val = key;
      if (!client.exists(key))
      {
        String keyToCreate = key;
        while (keyToCreate.startsWith(rootNamespace))
        {
          if (client.exists(keyToCreate))
          {
            break;
          }
          changes.add(keyToCreate + "-cachecreate-" + System.currentTimeMillis());
          keyToCreate = keyToCreate.substring(0, keyToCreate.lastIndexOf('/'));
        }
        
        cache.set(key, val);
        newWrites++;
      }
      else
      {
        int op = ((int) (Math.random() * 10000)) % 2;
        if (op == 0)
        {
          System.out.println("Deleting key:" + key);

          Map<String, ZNode> toDelete = new HashMap<String, ZNode>();
          read(toDelete, client, key);
          for (String child : toDelete.keySet())
          {
            changes.add(child + "-cachedelete-" + System.currentTimeMillis());
          }

          cache.remove(key);
          deletion++;
        }
        else
        {
          System.out.println("Updating key:" + key);
          Object data = client.readData(key);

//          String str = (data != null ? data.toString() : key) + "-updated";
//          cache.set(key, str);
          cache.updateSerialized(key, new DataUpdater<Object>()
          {
            
            @Override
            public Object update(Object currentData)
            {
              String curStr = (String)currentData;
              String updateStr = (curStr != null ? curStr : key) + "-updated";
              return updateStr;
            }
          }, false);
          changes.add(key + "-cachewrite-" + System.currentTimeMillis());
          updates++;
        }
      }
      count++;
    }

    System.out.println("newWrites:" + newWrites + " updates:" + updates + " deletions:"
        + deletion);
    Thread.sleep(5000);

    verify(cache, client, rootNamespace);
    // verifyCallbacks();

    System.out.println("Verification passed");
    client.close();
    // stopZkServer(server);
  }

  private static void printChangesFor(String path)
  {
    System.out.println("START:Changes detected for child:" + path);
    int id = 0;
    for (String entry : changes)
    {
      if (entry.startsWith(path + "-"))
      {
        System.out.println(id + ": " + entry);
      }
      id++;
    }

    System.out.println("END:Changes detected for child:" + path);
  }

  static void verifyCallbacks() throws Exception
  {
    Set<String> subscribedSet = new HashSet<String>();
    Map<String, Integer> callbackVerifyMap = new HashMap<String, Integer>();

    System.out.println("START:Verify callback");
    for (String entry : changes)
    {
      int idx;
      idx = entry.indexOf("-updatecache-");
      if (idx != -1)
      {
        String path = entry.substring(0, idx);
        subscribedSet.add(path);
      }

      idx = entry.indexOf("-unsubsribe-");
      if (idx != -1)
      {
        String path = entry.substring(0, idx);
        subscribedSet.remove(path);
      }

      idx = entry.indexOf("-create-");
      if (idx != -1)
      {
        // String key = entry.substring(0, idx) + "-create-";
        // if (!callbackVerifyMap.containsKey(key)) {
        // callbackVerifyMap.put(key, 0);
        // }
        // int count = callbackVerifyMap.get(key);
        // callbackVerifyMap.put(key, count+1);
      }
      idx = entry.indexOf("-delete-");
      if (idx != -1)
      {
        String path = entry.substring(0, idx);
        if (subscribedSet.contains(path))
        {
          String key = path + "-delete-";
          if (!callbackVerifyMap.containsKey(key))
          {
            callbackVerifyMap.put(key, 0);
          }
          int count = callbackVerifyMap.get(key);
          callbackVerifyMap.put(key, count + 1);
        }
      }
      idx = entry.indexOf("-deletecallback");
      if (idx != -1)
      {
        String path = entry.substring(0, idx);
        String key = path + "-delete-";
        if (!callbackVerifyMap.containsKey(key))
        {
          printChangesFor(path);
          // throw new Exception(key +
          // ": deletecallback without delete");
          callbackVerifyMap.put(key, 0);
        }
        int count = callbackVerifyMap.get(key);
        callbackVerifyMap.put(key, count - 1);
      }

      idx = entry.indexOf("-createcallback");
      if (idx != -1)
      {
        // String key = entry.substring(0, idx) + "-create-";
        // if (!callbackVerifyMap.containsKey(key)) {
        // throw new Exception(key + ": createcallback without create");
        // }
        // int count = callbackVerifyMap.get(key);
        // callbackVerifyMap.put(key, count-1);
      }
    }

    for (String key : callbackVerifyMap.keySet())
    {
      int count = callbackVerifyMap.get(key);
      if (count > 0)
      {
        int idx = key.indexOf("-create-");
        if (idx == -1)
        {
          idx = key.indexOf("-delete-");
        }
        String path = key.substring(0, idx);
        // if (!subscribedSet.contains(path)) {
        // continue;
        // }
        printChangesFor(path);
        throw new Exception(key + ": miss callbacks");
      }
    }

    System.out.println("END:Verify callback");

  }

  static void verify(ZkCache cache, ZkClient client, String root) throws Exception
  {
    Map<String, ZNode> zkMap = new HashMap<String, ZNode>();

    read(zkMap, client, root);
    System.out.println("actual size: " + zkMap.size() + ", cached size: "
        + cache._map.size());
    if (cache._map.size() != zkMap.size())
    {
      throw new Exception("size not same. actual: " + zkMap.size() + ", cache: "
          + cache._map.size());
    }
    for (String key : zkMap.keySet())
    {
      String actual = (String) (zkMap.get(key)._data);
      Stat actualStat = zkMap.get(key).getStat();

      Stat cachedStat = new Stat();
      String cached = (String) cache.get(key, cachedStat);

      // verify value
      if (actual == null)
      {
        if (cached != null)
        {
          throw new Exception(key + " not equal value. actual: " + actual + ", cached: "
              + cached);
        }
      }
      else
      {
        if (!actual.equals(cached))
        {
          throw new Exception(key + " not equal value. actual: " + actual + ", cached: "
              + cached);
        }
      }

      // verify stat
      if (!actualStat.equals(cachedStat))
      {
        printChangesFor(key);
        throw new Exception(key + " not equal stat. actual: " + actualStat + ", cached: "
            + cachedStat);
      }

      // verify childs
      Set<String> actualChilds = zkMap.get(key)._childSet;
      Set<String> cachedChilds = cache._map.get(key)._childSet;
      if (!actualChilds.equals(cachedChilds))
      {
        printChangesFor(key);

        throw new Exception(key + " childs not equal. actualChilds: " + actualChilds
            + ", cachedChilds: " + cachedChilds);
      }
    }
  }

  static void read(Map<String, ZNode> map, ZkClient client, String root)
  {
    List<String> childs = client.getChildren(root);
    if (childs != null)
    {
      Stat stat = new Stat();
      String value = client.readData(root, stat);
      ZNode node = new ZNode(root, value, stat);
      node._childSet.addAll(childs);
      map.put(root, node);

      for (String child : childs)
      {
        String childPath = root + "/" + child;
        read(map, client, childPath);
      }
    }

  }

  // move from TestHelper
  static public ZkServer startZkSever(final String zkAddress, final String rootNamespace) throws Exception
  {
    List<String> rootNamespaces = new ArrayList<String>();
    rootNamespaces.add(rootNamespace);
    return startZkSever(zkAddress, rootNamespaces);
  }

  static public ZkServer startZkSever(final String zkAddress,
                                      final List<String> rootNamespaces) throws Exception
  {
    System.out.println("Start zookeeper at " + zkAddress + " in thread "
        + Thread.currentThread().getName());

    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";
    FileUtils.deleteDirectory(new File(dataDir));
    FileUtils.deleteDirectory(new File(logDir));
    ZKClientPool.reset();

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
      {
        for (String rootNamespace : rootNamespaces)
        {
          try
          {
            zkClient.deleteRecursive(rootNamespace);
          }
          catch (Exception e)
          {
            LOG.error("fail to deleteRecursive path:" + rootNamespace, e);
          }
        }
      }
    };

    int port = Integer.parseInt(zkAddress.substring(zkAddress.lastIndexOf(':') + 1));
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();

    return zkServer;
  }

  static public void stopZkServer(ZkServer zkServer)
  {
    if (zkServer != null)
    {
      zkServer.shutdown();
      System.out.println("Shut down zookeeper at port " + zkServer.getPort()
          + " in thread " + Thread.currentThread().getName());
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
    // reinstall all zk listeners
    // try
    // {
    // _lock.writeLock().lock();
    // for (String path : _map.keySet())
    // {
    // updateCache(path);
    // }
    // }
    // finally
    // {
    // _lock.writeLock().unlock();
    //
    // }

  }
}
