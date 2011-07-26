package com.linkedin.clustermanager.agent.zk;

import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterView;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.util.CMUtil;

public class ZKDataAccessor implements ClusterDataAccessor
{
  private static Logger logger = Logger.getLogger(ZKDataAccessor.class);
  private final String _clusterName;
  private final ClusterView _clusterView;
  private final ZkClient _zkClient;

  public ZKDataAccessor(String clusterName, ZkClient zkClient)
  {
    this._clusterName = clusterName;
    this._zkClient = zkClient;
    this._clusterView = new ClusterView();
  }

  @Override
  public void setClusterProperty(ClusterPropertyType clusterProperty,
      String key, final ZNRecord value)
  {
    String zkPropertyPath = CMUtil.getClusterPropertyPath(_clusterName,
        clusterProperty);
    String targetValuePath = zkPropertyPath + "/" + key;

    ZKUtil.createOrReplace(_zkClient, targetValuePath, value,
        clusterProperty.isPersistent());
  }

  @Override
  public void updateClusterProperty(ClusterPropertyType clusterProperty,
      String key, final ZNRecord value)
  {
    String clusterPropertyPath = CMUtil.getClusterPropertyPath(_clusterName,
        clusterProperty);
    String targetValuePath = clusterPropertyPath + "/" + key;
    if (clusterProperty.isUpdateOnlyOnExists())
    {
      ZKUtil.updateIfExists(_zkClient, targetValuePath, value,
          clusterProperty.isMergeOnUpdate());
    } else
    {
      ZKUtil.createOrUpdate(_zkClient, targetValuePath, value,
          clusterProperty.isPersistent(), clusterProperty.isMergeOnUpdate());
    }
  }

  @Override
  public ZNRecord getClusterProperty(ClusterPropertyType clusterProperty,
      String key)
  {
    String clusterPropertyPath = CMUtil.getClusterPropertyPath(_clusterName,
        clusterProperty);
    String targetPath = clusterPropertyPath + "/" + key;
    ZNRecord nodeRecord = _zkClient.readData(targetPath);
    return nodeRecord;
  }

  @Override
  public List<ZNRecord> getClusterPropertyList(
      ClusterPropertyType clusterProperty)
  {
    String clusterPropertyPath = CMUtil.getClusterPropertyPath(_clusterName,
        clusterProperty);
    List<ZNRecord> children;
    children = ZKUtil.getChildren(_zkClient, clusterPropertyPath);
    _clusterView.setClusterPropertyList(clusterProperty, children);
    return children;
  }

  @Override
  public void setInstanceProperty(String instanceName,
      InstancePropertyType type, String key, final ZNRecord value)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        type);
    String propertyPath = path + "/" + key;
    ZKUtil.createOrReplace(_zkClient, propertyPath, value,
        type.isPersistent());
  }

  @Override
  public ZNRecord getInstanceProperty(String instanceName,
      InstancePropertyType clusterProperty, String key)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        clusterProperty);
    String propertyPath = path + "/" + key;
    ZNRecord record = _zkClient.readData(propertyPath, true);
    return record;
  }

  @Override
  public List<ZNRecord> getInstancePropertyList(String instanceName,
      InstancePropertyType clusterProperty)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        clusterProperty);
    return ZKUtil.getChildren(_zkClient, path);
  }

  @Override
  public void removeInstanceProperty(String instanceName,
      InstancePropertyType type, String key)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        type) + "/" + key;
    if (_zkClient.exists(path))
    {
      boolean b = _zkClient.delete(path);
      if (!b)
      {
        logger.warn("Unable to remove property at path:" + path);
      }
    } else
    {
      logger.warn("No property to remove at path:" + path);
    }
  }

  @Override
  public void updateInstanceProperty(String instanceName,
      InstancePropertyType type, String key, ZNRecord value)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        type) + "/" + key;
    if (type.isUpdateOnlyOnExists())
    {
      ZKUtil.updateIfExists(_zkClient, path, value,
          type.isMergeOnUpdate());
    } else
    {
      ZKUtil.createOrUpdate(_zkClient, path, value,
          type.isPersistent(), type.isMergeOnUpdate());
    }

  }
  public ClusterView getClusterView()
  {
    return _clusterView;
  }

  @Override
  public void setClusterProperty(ClusterPropertyType clusterProperty,
      String key, final ZNRecord value, CreateMode mode)
  {
    String zkPropertyPath = CMUtil.getClusterPropertyPath(_clusterName,
        clusterProperty);
    String targetValuePath = zkPropertyPath + "/" + key;

    if (_zkClient.exists(targetValuePath))
    {
      DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>()
      {
        @Override
        public ZNRecord update(ZNRecord currentData)
        {
          return value;
        }
      };
      _zkClient.updateDataSerialized(targetValuePath, updater);
    } else
    {
      _zkClient.create(targetValuePath, value, mode);
    }
  }

  @Override
  public void removeClusterProperty(ClusterPropertyType clusterProperty,
      String key)
  {
    String path = CMUtil.getClusterPropertyPath(_clusterName, clusterProperty)
        + "/" + key;
    if (_zkClient.exists(path))
    {
      boolean b = _zkClient.delete(path);
      if (!b)
      {
        logger.warn("Unable to remove property at path:" + path);
      }
    } else
    {
      logger.warn("No property to remove at path:" + path);
    }
  }
}
