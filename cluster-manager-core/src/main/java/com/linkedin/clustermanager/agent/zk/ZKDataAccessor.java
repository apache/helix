package com.linkedin.clustermanager.agent.zk;

import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterView;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.store.PropertyStore;
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
    ZNRecord nodeRecord = _zkClient.readData(targetPath,true);
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

  @Override
  public void setInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key,
      ZNRecord value)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        instanceProperty);
    String parentPath = path + "/" + subPath;
    if(!_zkClient.exists(parentPath))
    {
      String[] subPaths = subPath.split("/");
      String tempPath = path;
      for(int i = 0;i < subPaths.length; i++)
      {
        tempPath = tempPath + "/" + subPaths[i];
        if(!_zkClient.exists(tempPath))
        {
          if(instanceProperty.isPersistent()) 
          {
            _zkClient.createPersistent(tempPath);
          }
          else
          {
            _zkClient.createEphemeral(tempPath);
          }
        }
      }
    }
      
    String propertyPath = parentPath + "/" + key;
    ZKUtil.createOrReplace(_zkClient, propertyPath, value,
        instanceProperty.isPersistent());
    
  }

  @Override
  public void updateInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key,
      ZNRecord value)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        instanceProperty);
    String parentPath = path + "/" + subPath;
    if(!_zkClient.exists(parentPath))
    {
      String[] subPaths = subPath.split("/");
      String tempPath = path;
      for(int i = 0;i < subPaths.length; i++)
      {
        tempPath = tempPath + "/" + subPaths[i];
        if(!_zkClient.exists(tempPath))
        {
          if(instanceProperty.isPersistent()) 
          {
            _zkClient.createPersistent(tempPath);
          }
          else
          {
            _zkClient.createEphemeral(tempPath);
          }
        }
      }
    }
      
    String propertyPath = parentPath + "/" + key;
    ZKUtil.createOrUpdate(_zkClient, propertyPath, value,
        instanceProperty.isPersistent(), instanceProperty.isMergeOnUpdate());
  }

  @Override
  public List<ZNRecord> getInstancePropertyList(String instanceName,
      String subPath, InstancePropertyType instanceProperty)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        instanceProperty);
    path = path + "/" + subPath;
    return ZKUtil.getChildren(_zkClient, path);
  }

  @Override
  public ZNRecord getInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        instanceProperty);
    String propertyPath = path + "/" + subPath + "/" + key;
    if(_zkClient.exists(propertyPath))
    {
      ZNRecord record = _zkClient.readData(propertyPath, true);
      return record;
    }
    return null;
  }

  @Override
  public List<String> getInstancePropertySubPaths(String instanceName,
      InstancePropertyType instanceProperty)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        instanceProperty);
    return _zkClient.getChildren(path);
  }

  @Override
  public void substractInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key,
      ZNRecord value)
  {
    String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
        instanceProperty);
    String propertyPath = path + "/" + subPath + "/" + key;
    if(_zkClient.exists(propertyPath))
    {
      ZKUtil.substract(_zkClient, propertyPath, value);
    }
  }
  
  // distributed cluster controller
  @Override
  public void createControllerProperty(ControllerPropertyType controllerProperty, 
                                       ZNRecord value, CreateMode mode)
  {
    final String path = CMUtil.getControllerPropertyPath(_clusterName, controllerProperty);
    _zkClient.create(path, value, mode);
    // ZKUtil.createOrReplace(_zkClient, path, value, controllerProperty.isPersistent());
  }

  @Override
  public void removeControllerProperty(ControllerPropertyType controllerProperty)
  {
    final String path = CMUtil.getControllerPropertyPath(_clusterName, controllerProperty);
    if (_zkClient.exists(path))
    {
      boolean b = _zkClient.delete(path);
      if (!b)
      {
        logger.warn("Unable to remove property at path:" + path);
      }
    } 
    else
    {
      logger.warn("No controller property to remove at path:" + path);
    }
  }
  
  @Override
  public void setControllerProperty(ControllerPropertyType controllerProperty, 
                                    ZNRecord value, CreateMode mode)
  {
    final String path = CMUtil.getControllerPropertyPath(_clusterName, controllerProperty);
    // _zkClient.create(path, value, mode);
    ZKUtil.createOrReplace(_zkClient, path, value, controllerProperty.isPersistent());
  }
  
  @Override
  public ZNRecord getControllerProperty(ControllerPropertyType controllerProperty)
  {
    final String path = CMUtil.getControllerPropertyPath(_clusterName, controllerProperty);
    ZNRecord record = _zkClient.<ZNRecord>readData(path,true);
    return record;
  }

  @Override
  public PropertyStore<ZNRecord> getStore()
  {
    // TODO Auto-generated method stub
    return null;
  }
}
