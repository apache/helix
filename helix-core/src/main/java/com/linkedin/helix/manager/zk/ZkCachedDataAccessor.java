package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.IZkListener;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.store.zk.ZNode;

public class ZkCachedDataAccessor implements IZkListener
{
  private static final Logger  LOG   = Logger.getLogger(ZkCachedDataAccessor.class);

  ConcurrentMap<String, ZNode> _map  = new ConcurrentHashMap<String, ZNode>();
  ReadWriteLock                _lock = new ReentrantReadWriteLock();

  // final Set<String> _subscribedPaths =
  // Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  final String                 _root;
  // final ZkClient _zkClient;
  final ZkBaseDataAccessor     _accessor;
  // final FilenameFilter _filter;
  final List<String>           _subscribedPaths;

  public ZkCachedDataAccessor(ZkBaseDataAccessor accessor, List<String> subscribedPaths)
  {
    // _root = root;
    // _zkClient = zkClient;
    _root = accessor._root;
    _accessor = accessor;
    // _filter = filter;
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

  public boolean create(String path, ZNRecord record, int options)
  {
    // CreateMode mode = Option.getMode(options);
    // if (mode == null)
    // {
    // LOG.error("invalid create mode. options: " + options);
    // return false;
    // }
    //
    // try
    // {
    // _zkClient.create(path, record, mode);
    // updateCache(path);
    //
    // return true;
    // }
    // catch (ZkNoNodeException e)
    // {
    // String parentPath = new File(path).getParent();
    // createRecursive(parentPath);
    // _zkClient.create(path, record, mode);
    // updateCache(path);
    // return true;
    // }
    // catch (ZkNodeExistsException e)
    // {
    // LOG.warn("node already exists. path: " + path);
    // return false;
    // }

    boolean success = _accessor.create(path, record, options);

    // update cache
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

    return success;
  }

  public boolean set(String path, ZNRecord record, int options)
  {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean update(String path, ZNRecord record, int options)
  {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean remove(String path)
  {
    // TODO Auto-generated method stub
    return false;
  }

  // /**
  // * sync create parent and async create child. used internally when fail on NoNode
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
  // CreateCallbackHandler[] createCbList = new CreateCallbackHandler[records.size()];
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

  public boolean[] createChildren(String parentPath, List<ZNRecord> records, int options)
  {
    // boolean[] success = new boolean[records.size()];
    //
    // CreateMode mode = Option.getMode(options);
    // if (mode == null)
    // {
    // LOG.error("invalid create mode. options: " + options);
    // return success;
    // }
    //
    // CreateCallbackHandler[] cbList = new CreateCallbackHandler[records.size()];
    // for (int i = 0; i < records.size(); i++)
    // {
    // ZNRecord record = records.get(i);
    // String path = parentPath + "/" + record.getId();
    // cbList[i] = new CreateCallbackHandler();
    // _zkClient.asyncCreate(path, record, mode, cbList[i]);
    // }
    //
    // boolean failOnNoNode = false;
    // for (int i = 0; i < cbList.length; i++)
    // {
    // CreateCallbackHandler cb = cbList[i];
    // cb.waitForSuccess();
    // success[i] = (cb.getRc() == 0);
    // switch (Code.get(cb.getRc()))
    // {
    // case OK:
    // String path = parentPath + "/" + records.get(i).getId();
    // updateCache(path);
    // break;
    // case NONODE:
    // failOnNoNode = true;
    // break;
    // default:
    // break;
    // }
    // }
    //
    // // if fail on NO_NODE, sync create parent and do async create child nodes again
    // if (failOnNoNode)
    // {
    // createChildren(parentPath, records, success, mode, cbList);
    // }

    boolean[] success = _accessor.createChildren(parentPath, records, options);

    // update cache
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
      updateCacheRecursive(parentPath);
    }

    return success;
  }

  public boolean[] setChildren(String parentPath, List<ZNRecord> records, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean[] updateChildren(String parentPath, List<ZNRecord> records, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean[] remove(List<String> paths)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public ZNRecord get(String path, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public List<ZNRecord> get(List<String> paths, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public List<ZNRecord> getChildren(String parentPath, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public String getPath(PropertyType type, String... keys)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> getChildNames(String parentPath, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean exists(String path)
  {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean[] exists(List<String> paths)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public Stat[] getStats(List<String> paths)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public Stat getStat(String path)
  {
    // TODO Auto-generated method stub
    return null;
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
    // TODO Auto-generated method stub

  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
  {
//    System.out.println("ChildChange: " + currentChilds);
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
    String root = "TestCDA";
    ZkClient zkClient = new ZkClient("localhost:2191");
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.deleteRecursive("/" + root);

//    FilenameFilter filter = new FilenameFilter()
//    {
//
//      @Override
//      public boolean accept(File dir, String name)
//      {
//        String path = new File(dir.getAbsolutePath() + "/" + name).getAbsolutePath();
//        return path.startsWith("/TestCDA");
//      }
//    };

    List<String> list = new ArrayList<String>();
    list.add("/" + root);
    
    ZkCachedDataAccessor accessor =
        new ZkCachedDataAccessor(new ZkBaseDataAccessor(root, zkClient), list);

    ZkClient zkClient2 = new ZkClient("localhost:2191");
    zkClient2.setZkSerializer(new ZNRecordSerializer());
    ZkCachedDataAccessor accessor2 =
        new ZkCachedDataAccessor(new ZkBaseDataAccessor(root, zkClient2), list);

    System.out.println("cache: " + accessor._map);
    System.out.println("cache2: " + accessor2._map);

    // accessor2.subscribe("/" + root);

    // // test subscribe will pull in all existing znode to cache
    // for (int i = 0; i < 5; i++)
    // {
    // String msgId = "msg_" + i;
    // String path = "/" + root + "/host_0/" + msgId;
    // boolean success = accessor.create(path, new ZNRecord(msgId), Option.PERSISTENT);
    // Assert.assertTrue(success, "Should succeed in create");
    // }
    //
    // accessor.subscribe("/" + root);
    // System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    // Thread.sleep(500);
    // System.out.println("cache2: " + accessor2._map);

    // test sync create will update cache
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = "/" + root + "/host_0/" + msgId;
      boolean success =
          accessor.create(path, new ZNRecord(msgId), BaseDataAccessor.Option.PERSISTENT);
      System.out.println(success + ":" + msgId);
      // Assert.assertTrue(success, "Should succeed in create");
    }
    System.out.println("cache: " + accessor._map);

    // wait for cache2 to be updated by zk callback
    Thread.sleep(500);
    System.out.println("cache2: " + accessor2._map);

    // // test createChildren()
    // List<ZNRecord> records = new ArrayList<ZNRecord>();
    //
    // for (int i = 0; i < 10; i++)
    // {
    // String msgId = "msg_" + i;
    // records.add(new ZNRecord(msgId));
    // }
    // String parentPath = "/" + root + "/host_1";
    // boolean[] success =
    // accessor.createChildren(parentPath, records, BaseDataAccessor.Option.PERSISTENT);
    // System.out.println(Arrays.toString(success));
    // System.out.println("cache: " + accessor._map);
    //
    // // wait for cache2 to be updated by zk callback
    // Thread.sleep(1000);
    // System.out.println("cache2: " + accessor2._map);

    zkClient.close();
  }
}
