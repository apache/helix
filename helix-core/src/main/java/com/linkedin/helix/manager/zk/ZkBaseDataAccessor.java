package com.linkedin.helix.manager.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.CreateCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.SetDataCallbackHandler;

public class ZkBaseDataAccessor implements BaseDataAccessor
{
  private static Logger    LOG = Logger.getLogger(ZkBaseDataAccessor.class);

  protected final String   _clusterName;
  protected final ZkClient _zkClient;

  class ZkDataUpdater implements DataUpdater<ZNRecord>
  {
    final ZNRecord _record;

    ZkDataUpdater(ZNRecord record)
    {
      _record = record;
    }

    @Override
    public ZNRecord update(ZNRecord current)
    {
      if (current != null)
      {
        current.merge(_record);
        return current;
      }
      return _record;
    }
  }

  public ZkBaseDataAccessor(String clusterName, ZkClient zkClient)
  {
    _clusterName = clusterName;
    _zkClient = zkClient;
  }

  @Override
  public boolean create(String path, ZNRecord record, int options)
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
    }
    catch (ZkNoNodeException e)
    {
      String parentPath = new File(path).getParent();
      _zkClient.createPersistent(parentPath, true);
      _zkClient.create(path, record, mode);
      return true;
    }
    catch (ZkNodeExistsException e)
    {
      LOG.warn("node already exists. path: " + path);
      return false;
    }
  }

  @Override
  public boolean set(String path, ZNRecord record, int options)
  {
    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return false;
    }

    try
    {
      _zkClient.writeData(path, record);
    }
    catch (ZkNoNodeException e)
    {
      String parentPath = new File(path).getParent();
      _zkClient.createPersistent(parentPath, true);
      _zkClient.create(path, record, mode);
    }
    return true;
  }

  @Override
  public boolean update(String path, final ZNRecord record, int options)
  {
    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return false;
    }

    ZkDataUpdater updater = new ZkDataUpdater(record);

    try
    {
      _zkClient.updateDataSerialized(path, updater);
    }
    catch (ZkNoNodeException e)
    {
      String parentPath = new File(path).getParent();
      _zkClient.createPersistent(parentPath, true);
      _zkClient.create(path, record, mode);
    }
    return true;
  }

  @Override
  public boolean[] createChildren(String parentPath, List<ZNRecord> records, int options)
  {
    boolean[] succeeds = new boolean[records.size()];

    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return succeeds;
    }

    List<CreateCallbackHandler> cbList = new ArrayList<CreateCallbackHandler>();
    for (ZNRecord record : records)
    {
      String path = parentPath + "/" + record.getId();
      CreateCallbackHandler cb = new CreateCallbackHandler();
      cbList.add(cb);
      // TODO: handle parent not exist
      _zkClient.asyncCreate(path, record, mode, cb);
    }

    for (int i = 0; i < cbList.size(); i++)
    {
      CreateCallbackHandler cb = cbList.get(i);
      cb.waitForSuccess();
      succeeds[i] = cb.getSuccess();
    }

    return succeeds;
  }

  @Override
  public boolean[] setChildren(String parentPath, List<ZNRecord> records, int options)
  {
    boolean[] succeeds = new boolean[records.size()];

    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return succeeds;
    }

    List<SetDataCallbackHandler> cbList = new ArrayList<SetDataCallbackHandler>();
    for (ZNRecord record : records)
    {
      String path = parentPath + "/" + record.getId();
      // TODO: handle parent/node not exist
      SetDataCallbackHandler cb = new SetDataCallbackHandler();
      cbList.add(cb);
      _zkClient.asyncSetData(path, record, -1, cb);
    }

    for (int i = 0; i < cbList.size(); i++)
    {
      SetDataCallbackHandler cb = cbList.get(i);
      cb.waitForSuccess();
      succeeds[i] = cb.getSuccess();
    }

    return succeeds;
  }

  @Override
  public boolean[] updateChildren(String parentPath, List<ZNRecord> records, int options)
  {
    boolean[] succeeds = new boolean[records.size()];

    CreateMode mode = Option.getMode(options);
    if (mode == null)
    {
      LOG.error("invalid create mode. options: " + options);
      return succeeds;
    }

    List<SetDataCallbackHandler> cbList = new ArrayList<SetDataCallbackHandler>();
    for (ZNRecord record : records)
    {
      String path = parentPath + "/" + record.getId();
      SetDataCallbackHandler cb = new SetDataCallbackHandler();
      cbList.add(cb);
      ZkDataUpdater updater = new ZkDataUpdater(record);

      Stat stat = new Stat();
      ZNRecord oldData = _zkClient.readData(path, stat);
      ZNRecord newData = updater.update(oldData);
      _zkClient.asyncSetData(path, newData, stat.getVersion(), cb);
    }

    // TODO: add retry
    for (int i = 0; i < cbList.size(); i++)
    {
      SetDataCallbackHandler cb = cbList.get(i);
      cb.waitForSuccess();
      succeeds[i] = cb.getSuccess();
    }

    return succeeds;
  }

  @Override
  public ZNRecord get(String path, int options)
  {
    return _zkClient.readData(path, true);
  }

  @Override
  public List<ZNRecord> get(List<String> paths, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ZNRecord> getChildren(String parentPath, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getPath(PropertyType type, String... keys)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getChildNames(String parentPath, int options)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean exists(String path)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean[] exists(List<String> paths)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Stat[] getStats(List<String> paths)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Stat getStat(String path)
  {
    // TODO Auto-generated method stub
    return null;
  }

}
