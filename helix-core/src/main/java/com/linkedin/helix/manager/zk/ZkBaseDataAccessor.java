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
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.CreateCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.DefaultCallback;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.DeleteCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.ExistsCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.GetDataCallbackHandler;
import com.linkedin.helix.manager.zk.ZkAsyncCallbacks.SetDataCallbackHandler;

public class ZkBaseDataAccessor implements BaseDataAccessor
{
  private static Logger LOG = Logger.getLogger(ZkBaseDataAccessor.class);

  private final ZkClient _zkClient;

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

  public ZkBaseDataAccessor(ZkClient zkClient)
  {
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
    }catch (Exception e) {
      LOG.error("Exception while creating path: " + path +". " +e.getMessage());
      return false;
    }
  }

  // TODO: have retry in this and make sure we handle various exceptions
  // appropriately. This applies to create, update,set
  @Override
  public boolean set(String path, ZNRecord record, int options)
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
    }catch (Exception e) {
      LOG.error("Exception while setting path: " + path +". " +e.getMessage());
      return false;
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
    } catch (ZkNoNodeException e)
    {
      String parentPath = new File(path).getParent();
      _zkClient.createPersistent(parentPath, true);
      _zkClient.create(path, record, mode);
    }catch (Exception e) {
      LOG.error("Exception while updating path: " + path +". " +e.getMessage());
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
      List<String> paths, List<ZNRecord> records, boolean[] success,
      CreateMode mode, DefaultCallback[] cbList)
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

      ZNRecord record = records.get(i);
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

  // // @Override
  // public boolean[] createChildren(String parentPath, List<ZNRecord> records,
  // int
  // options)
  // {
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
  // if (Code.get(cb.getRc()) == Code.NONODE)
  // {
  // failOnNoNode = true;
  // }
  // }
  //
  // // if fail on NO_NODE, sync create parent and do async create child nodes
  // // again
  // if (failOnNoNode)
  // {
  // createChildren(parentPath, records, success, mode, cbList);
  // }
  //
  // return success;
  // }

  // // @Override
  // public boolean[] setChildren(String parentPath, List<ZNRecord> records, int
  // options)
  // {
  // boolean[] success = new boolean[records.size()];
  //
  // CreateMode mode = Option.getMode(options);
  // if (mode == null)
  // {
  // LOG.error("invalid create mode. options: " + options);
  // return success;
  // }
  //
  // SetDataCallbackHandler[] cbList = new
  // SetDataCallbackHandler[records.size()];
  // for (int i = 0; i < records.size(); i++)
  // {
  // ZNRecord record = records.get(i);
  // String path = parentPath + "/" + record.getId();
  // cbList[i] = new SetDataCallbackHandler();
  // _zkClient.asyncSetData(path, record, -1, cbList[i]);
  // }
  //
  // boolean failOnNoNode = false;
  // for (int i = 0; i < cbList.length; i++)
  // {
  // SetDataCallbackHandler cb = cbList[i];
  // cb.waitForSuccess();
  // success[i] = (cb.getRc() == 0);
  // if (Code.get(cb.getRc()) == Code.NONODE)
  // {
  // failOnNoNode = true;
  // }
  // }
  //
  // // if fail on NO_NODE, sync create parent node and do async create child
  // // nodes
  // if (failOnNoNode)
  // {
  // createChildren(parentPath, records, success, mode, cbList);
  // }
  //
  // return success;
  // }

  // // @Override
  // public boolean[] updateChildren(String parentPath, List<ZNRecord> records,
  // int options)
  // {
  // boolean[] success = new boolean[records.size()];
  // CreateMode mode = Option.getMode(options);
  // if (mode == null)
  // {
  // LOG.error("invalid create mode. options: " + options);
  // return success;
  // }
  //
  // SetDataCallbackHandler[] cbList = new
  // SetDataCallbackHandler[records.size()];
  //
  // boolean failOnBadVersion = false;
  // boolean failOnNoNode = false;
  // do
  // {
  // for (int i = 0; i < records.size(); i++)
  // {
  // if (success[i])
  // continue;
  //
  // ZNRecord record = records.get(i);
  // String path = parentPath + "/" + record.getId();
  // cbList[i] = new SetDataCallbackHandler();
  // ZkDataUpdater updater = new ZkDataUpdater(record);
  //
  // Stat stat = new Stat();
  // ZNRecord oldData = _zkClient.readData(path, stat);
  // ZNRecord newData = updater.update(oldData);
  // _zkClient.asyncSetData(path, newData, stat.getVersion(), cbList[i]);
  // }
  //
  // for (int i = 0; i < cbList.length; i++)
  // {
  // SetDataCallbackHandler cb = cbList[i];
  // cb.waitForSuccess();
  // success[i] = (cb.getRc() == 0);
  // if (success[i] == false)
  // {
  // switch (Code.get(cb.getRc()))
  // {
  // case NONODE:
  // failOnNoNode = true;
  // break;
  // case BADVERSION:
  // failOnBadVersion = true;
  // break;
  // default:
  // break;
  // }
  // }
  //
  // }
  // }
  // while (failOnBadVersion);
  //
  // // if fail on NO_NODE, create parent node and do async create child nodes
  // if (failOnNoNode)
  // {
  // createChildren(parentPath, records, success, mode, cbList);
  // }
  //
  // return success;
  // }

  @Override
  public ZNRecord get(String path, Stat stat, int options)
  {
    // throw ZkNoNodeException to distinguish NoNode and NodeWithEmptyValue
    return _zkClient.readData(path, stat);
  }

  @Override
  public List<ZNRecord> get(List<String> paths, int options)
  {
    GetDataCallbackHandler[] cbList = new GetDataCallbackHandler[paths.size()];
    for (int i = 0; i < paths.size(); i++)
    {
      String path = paths.get(i);
      cbList[i] = new GetDataCallbackHandler();
      _zkClient.asyncGetData(path, cbList[i]);
    }

    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < cbList.length; i++)
    {
      GetDataCallbackHandler cb = cbList[i];
      cb.waitForSuccess();
      if (cb.getRc() == 0)
      {
        ZNRecord record = (ZNRecord) _zkClient.getZkSerializer().deserialize(
            cb._data);
        records.add(record);
      } else
      {
        records.add(null);
      }
    }

    return records;
  }

  @Override
  public List<ZNRecord> getChildren(String parentPath, int options)
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
  public boolean[] createChildren(List<String> paths, List<ZNRecord> records,
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
      ZNRecord record = records.get(i);
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
  public boolean[] setChildren(List<String> paths, List<ZNRecord> records,
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
    for (int i = 0; i < records.size(); i++)
    {
      ZNRecord record = records.get(i);
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
  public boolean[] updateChildren(List<String> paths, List<ZNRecord> records,
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

        ZNRecord record = records.get(i);
        String path = paths.get(i); // parentPath + "/" + record.getId();
        cbList[i] = new SetDataCallbackHandler();
        ZkDataUpdater updater = new ZkDataUpdater(record);

        Stat stat = new Stat();
        ZNRecord oldData = _zkClient.readData(path, stat);
        ZNRecord newData = updater.update(oldData);
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

  /*
   * public static void main(String[] args) { ZkClient zkClient = new
   * ZkClient("localhost:2191"); zkClient.setZkSerializer(new
   * ZNRecordSerializer()); zkClient.deleteRecursive("/TestBDA");
   * 
   * BaseDataAccessor accessor = new ZkBaseDataAccessor( zkClient);
   * 
   * // test create() for (int i = 0; i < 10; i++) { String msgId = "msg_" + i;
   * String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_0",
   * msgId); boolean succeed = accessor.create(path, new ZNRecord(msgId),
   * Option.PERSISTENT); System.out.println(succeed + ":" + path); }
   * 
   * // test set() for (int i = 0; i < 10; i++) { String msgId = "msg_" + i;
   * String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_0",
   * msgId); ZNRecord newRecord = new ZNRecord(msgId);
   * newRecord.setSimpleField("key1", "value1"); boolean succeed =
   * accessor.set(path, newRecord, Option.PERSISTENT);
   * System.out.println(succeed + ":" + path); }
   * 
   * // test update() for (int i = 0; i < 10; i++) { String msgId = "msg_" + i;
   * String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_0",
   * msgId); ZNRecord newRecord = new ZNRecord(msgId);
   * newRecord.setSimpleField("key2", "value2"); boolean succeed =
   * accessor.update(path, newRecord, Option.PERSISTENT);
   * System.out.println(succeed + ":" + path); }
   * 
   * // test get() for (int i = 0; i < 10; i++) { String msgId = "msg_" + i;
   * String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_0",
   * msgId); ZNRecord record = accessor.get(path, null, 0);
   * System.out.println(record); }
   * 
   * // test exist() for (int i = 0; i < 10; i++) { String msgId = "msg_" + i;
   * String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_0",
   * msgId); boolean exists = accessor.exists(path); System.out.println(exists);
   * }
   * 
   * // test getStat() for (int i = 0; i < 10; i++) { String msgId = "msg_" + i;
   * String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_0",
   * msgId); Stat stat = accessor.getStat(path); System.out.println(stat); }
   * 
   * // test remove() for (int i = 0; i < 10; i++) { String msgId = "msg_" + i;
   * String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_0",
   * msgId); boolean success = accessor.remove(path);
   * System.out.println(success); }
   * 
   * // test createChildren() String parentPath =
   * PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1"); //
   * zkClient.createPersistent(parentPath, true); List<ZNRecord> records = new
   * ArrayList<ZNRecord>(); List<String> paths = new ArrayList<String>(); for
   * (int i = 10; i < 20; i++) { String msgId = "msg_" + i;
   * paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES,
   * "host_1",msgId)); records.add(new ZNRecord(msgId)); }
   * 
   * boolean[] success = accessor.createChildren(paths, records,
   * Option.PERSISTENT); System.out.println(Arrays.toString(success));
   * 
   * // test setChildren()
   * 
   * parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1");
   * records = new ArrayList<ZNRecord>(); paths = new ArrayList<String>(); for
   * (int i = 10; i < 20; i++) { String msgId = "msg_" + i;
   * paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES,
   * "host_1",msgId)); ZNRecord newRecord = new ZNRecord(msgId);
   * newRecord.setSimpleField("key1", "value1"); records.add(newRecord); }
   * success = accessor.setChildren(paths, records, Option.PERSISTENT);
   * System.out.println(Arrays.toString(success));
   * 
   * // test updateChildren() parentPath =
   * PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1"); records = new
   * ArrayList<ZNRecord>(); paths = new ArrayList<String>(); for (int i = 10; i
   * < 20; i++) { String msgId = "msg_" + i;
   * paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES,
   * "host_1",msgId)); ZNRecord newRecord = new ZNRecord(msgId);
   * newRecord.setSimpleField("key2", "value2"); records.add(newRecord); }
   * success = accessor.updateChildren(paths, records, Option.PERSISTENT);
   * System.out.println(Arrays.toString(success));
   * 
   * // test getChildren() parentPath =
   * PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1"); records =
   * accessor.getChildren(parentPath, 0); System.out.println(records);
   * 
   * // test exists() parentPath =
   * PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1"); paths = new
   * ArrayList<String>(); for (int i = 10; i < 20; i++) { String msgId = "msg_"
   * + i; paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1",
   * msgId)); } boolean[] exists = accessor.exists(paths);
   * System.out.println(Arrays.toString(exists));
   * 
   * // test getStats() parentPath =
   * PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1"); paths = new
   * ArrayList<String>(); for (int i = 10; i < 20; i++) { String msgId = "msg_"
   * + i; paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1",
   * msgId)); } Stat[] stats = accessor.getStats(paths);
   * System.out.println(Arrays.toString(stats));
   * 
   * // test remove() parentPath =
   * PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1"); paths = new
   * ArrayList<String>(); for (int i = 10; i < 20; i++) { String msgId = "msg_"
   * + i; paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, "host_1",
   * msgId)); } success = accessor.remove(paths);
   * System.out.println(Arrays.toString(success)); }
   */

}
