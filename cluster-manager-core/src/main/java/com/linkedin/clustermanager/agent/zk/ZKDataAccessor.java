package com.linkedin.clustermanager.agent.zk;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertySerializer;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.store.zk.ZKPropertyStore;

public class ZKDataAccessor implements ClusterDataAccessor
{
  private static Logger logger = Logger.getLogger(ZKDataAccessor.class);
  private final String _clusterName;
//  private final ClusterView _clusterView;
  private final ZkClient _zkClient;

  public ZKDataAccessor(String clusterName, ZkClient zkClient)
  {
    this._clusterName = clusterName;
    this._zkClient = zkClient;
//    this._clusterView = new ClusterView();
  }

  @Override
  public boolean setProperty(PropertyType type, final ZNRecord value, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    String parent = new File(path).getParent();
    if (!_zkClient.exists(parent))
    {
      _zkClient.createPersistent(parent, true);
    }
    if (_zkClient.exists(path))
    {
      if (type.isCreateIfAbsent())
      {
        return false;
      }
      else
      {
        ZKUtil.createOrUpdate(_zkClient, path, value, type.isPersistent(), false);
      }
    }
    else
    {
      try
      {
        if (type.isPersistent())
        {
          _zkClient.createPersistent(path, value);
        }
        else
        {
          _zkClient.createEphemeral(path, value);
        }
      }
      catch (Exception e)
      {
        logger.warn("Exception while creating path:" + path
            + " Most likely due to race condition(Ignorable).", e);
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean updateProperty(PropertyType type, ZNRecord value, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    if (type.isUpdateOnlyOnExists())
    {
      ZKUtil.updateIfExists(_zkClient, path, value, type.isMergeOnUpdate());
    }
    else
    {
      String parent = new File(path).getParent();

      if (!_zkClient.exists(parent))
      {
        _zkClient.createPersistent(parent, true);
      }
      ZKUtil.createOrUpdate(_zkClient,
                            path,
                            value,
                            type.isPersistent(),
                            type.isMergeOnUpdate());
    }

    return true;
  }

  @Override
  public ZNRecord getProperty(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    ZNRecord record = _zkClient.readData(path, true);
    return record;
  }

  @Override
  public boolean removeProperty(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    return _zkClient.delete(path);
  }

  @Override
  public List<String> getChildNames(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    return _zkClient.getChildren(path);
  }

  @Override
  public List<ZNRecord> getChildValues(PropertyType type, String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    if (_zkClient.exists(path))
    {
      return ZKUtil.getChildren(_zkClient, path);
    }
    else
    {
      return Collections.emptyList();
    }
  }

  @Override
  public PropertyStore<ZNRecord> getStore()
  {
    String path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, _clusterName);
    if (!_zkClient.exists(path))
    {
      _zkClient.createPersistent(path);
    }

    String zkAddr = _zkClient.getConnection().getServers();
    PropertySerializer<ZNRecord> serializer =
        new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    return new ZKPropertyStore<ZNRecord>(new ZkConnection(zkAddr), serializer, path);

  }
}
