package com.linkedin.helix.manager.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.IZkListener;
import com.linkedin.helix.Updatable;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.CreateCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.DefaultCallback;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.DeleteCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.ExistsCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.GetDataCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.SetDataCallbackHandler;

public class ZkBaseDataAccessor<T> implements
    BaseDataAccessor<T>
{
  private static Logger LOG = Logger.getLogger(ZkBaseDataAccessor.class);

  private final ZkClient _zkClient;

  class ZkDataUpdater implements DataUpdater<Updatable>
  {
    final Updatable _record;

    public ZkDataUpdater(Updatable record)
    {
      _record = record;
    }

    @Override
    public Updatable update(Updatable current)
    {
      if (current != null)
      {
        current.update(_record);
        return current;
      } 
        return _record;
      
    }
  }

  public ZkBaseDataAccessor(ZkClient zkClient)
  {
    _zkClient = zkClient;
  }


  @Override
  public boolean create(String path, T record, int options)
  {
    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return false;
    }

    try
    {
      _zkClient.create(path, record, mode);
      return true;
    } catch (ZkNoNodeException e)
    {
      // this will happen if parent does not exist
      String parentPath = new File(path).getParent();
      try
      {
        _zkClient.createPersistent(parentPath, true);
        _zkClient.create(path, record, mode);
        return true;
      } catch (Exception e1)
      {
        return false;
      }
    } catch (ZkNodeExistsException e)
    {
      LOG.warn("node already exists. path: " + path);
      return false;
    } catch (Exception e)
    {
      LOG.error("Exception while creating path: " + path + ". "
          + e.getMessage());
      return false;
    }
  }

  // TODO: have retry in this and make sure we handle various exceptions
  // appropriately. This applies to create, update,set
  @Override
  public boolean set(String path, T record, int options)
  {
    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid set mode. options: " + options);
      return false;
    }

    try
    {
      _zkClient.writeData(path, record);
    } catch (ZkNoNodeException e)
    {
      String parentPath = new File(path).getParent();
      _zkClient.createPersistent(parentPath, true);
      _zkClient.create(path, record, mode);
    } catch (Exception e)
    {
      LOG.error("Exception while setting path: " + path + ". " + e.getMessage());
      return false;
    }
    return true;
  }

  


  @Override
  public boolean update(String path, final T record, int options)
  {
    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return false;
    }
    if (! (record instanceof Updatable))
    {
      throw new IllegalArgumentException("record should be instance of Updatable");
    }
    ZkDataUpdater updater = new ZkDataUpdater((Updatable)record);
    try
    {
      _zkClient.updateDataSerialized(path, updater);
    } catch (ZkNoNodeException e)
    {
      String parentPath = new File(path).getParent();
      _zkClient.createPersistent(parentPath, true);
      _zkClient.create(path, record, mode);
    } catch (Exception e)
    {
      LOG.error("Exception while updating path: " + path + ". "
          + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * sync create parent and async create child. used internally when fail on
   * NoNode
   * 
   * @param parentPath
   * @param records
   * @param success
   * @param mode
   * @param cbList
   */
  private void createChildren(
      // String parentPath,
      List<String> paths, List<T> records, boolean[] success, CreateMode mode,
      DefaultCallback[] cbList)
  {
    // _zkClient.createPersistent(parentPath, true);

    CreateCallbackHandler[] createCbList = new CreateCallbackHandler[records
        .size()];
    for (int i = 0; i < records.size(); i++)
    {
      DefaultCallback cb = cbList[i];
      if (Code.get(cb.getRc()) != Code.NONODE)
      {
        continue;
      }

      T record = records.get(i);
      String path = paths.get(i); // parentPath + "/" + record.getId();
      String parentPath = new File(path).getParent();
      _zkClient.createPersistent(parentPath, true);

      createCbList[i] = new CreateCallbackHandler();
      _zkClient.asyncCreate(path, record, mode, createCbList[i]);
    }

    for (int i = 0; i < createCbList.length; i++)
    {
      CreateCallbackHandler createCb = createCbList[i];
      if (createCb != null)
      {
        createCb.waitForSuccess();
        success[i] = (createCb.getRc() == 0);
      }
    }
  }

  @Override
  public T get(String path, Stat stat, int options)
  {
    // throw ZkNoNodeException to distinguish NoNode and NodeWithEmptyValue
    return _zkClient.readData(path, stat);
  }

  @Override
  public List<T> get(List<String> paths, int options)
  {
    GetDataCallbackHandler[] cbList = new GetDataCallbackHandler[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      String path = paths.get(i);
      cbList[i] = new GetDataCallbackHandler();
      _zkClient.asyncGetData(path, cbList[i]);
    }

    List<T> records = new ArrayList<T>();
    for (int i = 0; i < cbList.length; i++)
    {
      GetDataCallbackHandler cb = cbList[i];
      cb.waitForSuccess();
      if (cb.getRc() == 0)
      {
        @SuppressWarnings("unchecked")
        T record = (T) _zkClient.getZkSerializer().deserialize(cb._data);
        records.add(record);
      } else
      {
        records.add(null);
      }
    }

    return records;
  }

  @Override
  public List<T> getChildren(String parentPath, int options)
  {
    try
    {
      List<String> childNames = getChildNames(parentPath, options);
      List<String> paths = new ArrayList<String>();
      for (String childName : childNames)
      {
        String path = parentPath + "/" + childName;
        paths.add(path);
      }

      // remove null children
      List<T> records = get(paths, options);
      Iterator<T> iter = records.iterator();
      while (iter.hasNext())
      {
        if (iter.next() == null)
        {
          iter.remove();
        }
      }
      return records;
    } catch (ZkNoNodeException e)
    {
      return Collections.emptyList();
    }
  }

  @Override
  public List<String> getChildNames(String parentPath, int options)
  {
    try
    {
      List<String> childNames = _zkClient.getChildren(parentPath);
      Collections.sort(childNames);
      return childNames;
    } catch (ZkNoNodeException e)
    {
      return Collections.emptyList();
    }
  }

  @Override
  public boolean exists(String path)
  {
    return _zkClient.exists(path);
  }

  @Override
  public boolean[] exists(List<String> paths)
  {
    boolean[] exists = new boolean[paths.size()];

    ExistsCallbackHandler[] cbList = new ExistsCallbackHandler[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      String path = paths.get(i);
      cbList[i] = new ExistsCallbackHandler();
      _zkClient.asyncExists(path, cbList[i]);
    }

    for (int i = 0; i < cbList.length; i++)
    {
      ExistsCallbackHandler cb = cbList[i];
      cb.waitForSuccess();
      exists[i] = (cb._stat != null);
    }

    return exists;
  }

  @Override
  public Stat[] getStats(List<String> paths)
  {
    Stat[] stats = new Stat[paths.size()];

    ExistsCallbackHandler[] cbList = new ExistsCallbackHandler[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      String path = paths.get(i);
      cbList[i] = new ExistsCallbackHandler();
      _zkClient.asyncExists(path, cbList[i]);
    }

    for (int i = 0; i < cbList.length; i++)
    {
      ExistsCallbackHandler cb = cbList[i];
      cb.waitForSuccess();
      stats[i] = cb._stat;
    }

    return stats;
  }

  @Override
  public Stat getStat(String path)
  {
    return _zkClient.getStat(path);
  }

  @Override
  public boolean remove(String path)
  {
    _zkClient.deleteRecursive(path);
    return true;
  }

  @Override
  public boolean[] remove(List<String> paths)
  {
    boolean[] success = new boolean[paths.size()];

    DeleteCallbackHandler[] cbList = new DeleteCallbackHandler[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      String path = paths.get(i);
      cbList[i] = new DeleteCallbackHandler();
      _zkClient.asyncDelete(path, cbList[i]);
    }

    for (int i = 0; i < cbList.length; i++)
    {
      DeleteCallbackHandler cb = cbList[i];
      cb.waitForSuccess();
      success[i] = (cb.getRc() == 0);
    }

    return success;
  }

  @Override
  public boolean subscribe(String path, IZkListener listener)
  {
    _zkClient.subscribeChildChanges(path, listener);
    _zkClient.subscribeDataChanges(path, listener);

    return true;
  }

  @Override
  public boolean unsubscribe(String path, IZkListener listener)
  {
    _zkClient.unsubscribeChildChanges(path, listener);
    _zkClient.unsubscribeDataChanges(path, listener);

    return true;
  }

  @Override
  public boolean[] createChildren(List<String> paths, List<T> records,
      int options)
  {
    boolean[] success = new boolean[records.size()];

    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return success;
    }

    CreateCallbackHandler[] cbList = new CreateCallbackHandler[records.size()];
    for (int i = 0; i < records.size(); i++)
    {
      T record = records.get(i);
      String path = paths.get(i);
      // String path = parentPath + "/" + record.getId();
      cbList[i] = new CreateCallbackHandler();
      _zkClient.asyncCreate(path, record, mode, cbList[i]);
    }

    boolean failOnNoNode = false;
    for (int i = 0; i < cbList.length; i++)
    {
      CreateCallbackHandler cb = cbList[i];
      cb.waitForSuccess();
      success[i] = (cb.getRc() == 0);
      if (Code.get(cb.getRc()) == Code.NONODE)
      {
        failOnNoNode = true;
      }
    }

    // if fail on NO_NODE, sync create parent and do async create child nodes
    // again
    if (failOnNoNode)
    {
      createChildren(paths, records, success, mode, cbList);
    }

    return success;
  }

  @Override
  public boolean[] setChildren(List<String> paths, List<T> records, int options)
  {
    boolean[] success = new boolean[records.size()];

    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return success;
    }

    SetDataCallbackHandler[] cbList = new SetDataCallbackHandler[records.size()];
    for (int i = 0; i < records.size(); i++)
    {
      T record = records.get(i);
      String path = paths.get(i); // parentPath + "/" + record.getId();
      cbList[i] = new SetDataCallbackHandler();
      _zkClient.asyncSetData(path, record, -1, cbList[i]);
    }

    boolean failOnNoNode = false;
    for (int i = 0; i < cbList.length; i++)
    {
      SetDataCallbackHandler cb = cbList[i];
      cb.waitForSuccess();
      success[i] = (cb.getRc() == 0);
      if (Code.get(cb.getRc()) == Code.NONODE)
      {
        failOnNoNode = true;
      }
    }

    // if fail on NO_NODE, sync create parent node and do async create child
    // nodes
    if (failOnNoNode)
    {
      createChildren(paths, records, success, mode, cbList);
    }

    return success;
  }

  @Override
  public boolean[] updateChildren(List<String> paths, List<T> records,
      int options)
  {
    boolean[] success = new boolean[records.size()];
    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return success;
    }

    SetDataCallbackHandler[] cbList = new SetDataCallbackHandler[records.size()];

    boolean failOnBadVersion = false;
    boolean failOnNoNode = false;
    do
    {
      for (int i = 0; i < records.size(); i++)
      {
        if (success[i])
          continue;

        T record = records.get(i);
        String path = paths.get(i); // parentPath + "/" + record.getId();
        cbList[i] = new SetDataCallbackHandler();
        if (! (record instanceof Updatable))
        {
          throw new IllegalArgumentException("record should be instance of Updatable");
        }
        ZkDataUpdater updater = new ZkDataUpdater((Updatable)record);

        Stat stat = new Stat();
        T oldData = _zkClient.readData(path, stat);
        @SuppressWarnings("unchecked")
        T newData = (T) updater.update((Updatable)oldData);
        _zkClient.asyncSetData(path, newData, stat.getVersion(), cbList[i]);
      }

      for (int i = 0; i < cbList.length; i++)
      {
        SetDataCallbackHandler cb = cbList[i];
        cb.waitForSuccess();
        success[i] = (cb.getRc() == 0);
        if (success[i] == false)
        {
          switch (Code.get(cb.getRc()))
          {
          case NONODE:
            failOnNoNode = true;
            break;
          case BADVERSION:
            failOnBadVersion = true;
            break;
          default:
            break;
          }
        }

      }
    } while (failOnBadVersion);

    // if fail on NO_NODE, create parent node and do async create child nodes
    if (failOnNoNode)
    {
      createChildren(paths, records, success, mode, cbList);
    }

    return success;
  }

}
