package com.linkedin.clustermanager.agent.zk;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterView;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
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

  /*
   * @Override public void setClusterProperty(PropertyType clusterProperty,
   * String key, final ZNRecord value) { String zkPropertyPath =
   * CMUtil.getPropertyPath(_clusterName, clusterProperty); String
   * targetValuePath = zkPropertyPath + "/" + key;
   * 
   * ZKUtil.createOrReplace(_zkClient, targetValuePath, value,
   * clusterProperty.isPersistent()); }
   * 
   * @Override public void updateClusterProperty(PropertyType clusterProperty,
   * String key, final ZNRecord value) { String clusterPropertyPath =
   * CMUtil.getPropertyPath(_clusterName, clusterProperty); String
   * targetValuePath = clusterPropertyPath + "/" + key; if
   * (clusterProperty.isUpdateOnlyOnExists()) { ZKUtil.updateIfExists(_zkClient,
   * targetValuePath, value, clusterProperty.isMergeOnUpdate()); } else {
   * ZKUtil.createOrUpdate(_zkClient, targetValuePath, value,
   * clusterProperty.isPersistent(), clusterProperty.isMergeOnUpdate()); } }
   * 
   * @Override public ZNRecord getProperty(PropertyType clusterProperty, String
   * key) { String clusterPropertyPath = CMUtil.getPropertyPath(_clusterName,
   * clusterProperty); String targetPath = clusterPropertyPath + "/" + key;
   * ZNRecord nodeRecord = _zkClient.readData(targetPath, true); return
   * nodeRecord; }
   * 
   * @Override public List<ZNRecord> getPropertyList( PropertyType
   * clusterProperty) { String clusterPropertyPath =
   * CMUtil.getPropertyPath(_clusterName, clusterProperty); List<ZNRecord>
   * children; children = ZKUtil.getChildren(_zkClient, clusterPropertyPath);
   * _clusterView.setClusterPropertyList(clusterProperty, children); return
   * children; }
   * 
   * @Override public void setInstanceProperty(String instanceName,
   * InstancePropertyType type, String key, final ZNRecord value) { String path
   * = CMUtil.getInstancePropertyPath(_clusterName, instanceName, type); String
   * propertyPath = path + "/" + key; ZKUtil.createOrReplace(_zkClient,
   * propertyPath, value, type.isPersistent()); }
   * 
   * @Override public ZNRecord getInstanceProperty(String instanceName,
   * InstancePropertyType clusterProperty, String key) { String path =
   * CMUtil.getInstancePropertyPath(_clusterName, instanceName,
   * clusterProperty); String propertyPath = path + "/" + key; ZNRecord record =
   * _zkClient.readData(propertyPath, true); return record; }
   * 
   * @Override public List<ZNRecord> getInstancePropertyList(String
   * instanceName, InstancePropertyType clusterProperty) { String path =
   * CMUtil.getInstancePropertyPath(_clusterName, instanceName,
   * clusterProperty); return ZKUtil.getChildren(_zkClient, path); }
   * 
   * @Override public void removeInstanceProperty(String instanceName,
   * InstancePropertyType type, String key) { String path =
   * CMUtil.getInstancePropertyPath(_clusterName, instanceName, type) + "/" +
   * key; if (_zkClient.exists(path)) { boolean b = _zkClient.delete(path); if
   * (!b) { logger.warn("Unable to remove property at path:" + path); } } else {
   * logger.warn("No property to remove at path:" + path); } }
   * 
   * @Override public void updateInstanceProperty(String instanceName,
   * InstancePropertyType type, String key, ZNRecord value) { String path =
   * CMUtil.getInstancePropertyPath(_clusterName, instanceName, type) + "/" +
   * key; if (type.isUpdateOnlyOnExists()) { ZKUtil.updateIfExists(_zkClient,
   * path, value, type.isMergeOnUpdate()); } else {
   * ZKUtil.createOrUpdate(_zkClient, path, value, type.isPersistent(),
   * type.isMergeOnUpdate()); }
   * 
   * }
   * 
   * public ClusterView getClusterView() { return _clusterView; }
   * 
   * @Override public void setClusterProperty(PropertyType clusterProperty,
   * String key, final ZNRecord value, CreateMode mode) { String zkPropertyPath
   * = CMUtil.getPropertyPath(_clusterName, clusterProperty); String
   * targetValuePath = zkPropertyPath + "/" + key;
   * 
   * if (_zkClient.exists(targetValuePath)) { DataUpdater<ZNRecord> updater =
   * new DataUpdater<ZNRecord>() {
   * 
   * @Override public ZNRecord update(ZNRecord currentData) { return value; } };
   * _zkClient.updateDataSerialized(targetValuePath, updater); } else {
   * _zkClient.create(targetValuePath, value, mode); } }
   * 
   * @Override public void removeClusterProperty(PropertyType clusterProperty,
   * String key) { String path = CMUtil.getPropertyPath(_clusterName,
   * clusterProperty) + "/" + key; if (_zkClient.exists(path)) { boolean b =
   * _zkClient.delete(path); if (!b) {
   * logger.warn("Unable to remove property at path:" + path); } } else {
   * logger.warn("No property to remove at path:" + path); } }
   * 
   * @Override public void setInstanceProperty(String instanceName,
   * InstancePropertyType instanceProperty, String subPath, String key, ZNRecord
   * value) { String path = CMUtil.getInstancePropertyPath(_clusterName,
   * instanceName, instanceProperty); String parentPath = path + "/" + subPath;
   * if (!_zkClient.exists(parentPath)) { String[] subPaths =
   * subPath.split("/"); String tempPath = path; for (int i = 0; i <
   * subPaths.length; i++) { tempPath = tempPath + "/" + subPaths[i]; if
   * (!_zkClient.exists(tempPath)) { if (instanceProperty.isPersistent()) {
   * _zkClient.createPersistent(tempPath); } else {
   * _zkClient.createEphemeral(tempPath); } } } }
   * 
   * String propertyPath = parentPath + "/" + key;
   * ZKUtil.createOrReplace(_zkClient, propertyPath, value,
   * instanceProperty.isPersistent());
   * 
   * }
   * 
   * @Override public void updateInstanceProperty(String instanceName,
   * InstancePropertyType instanceProperty, String subPath, String key, ZNRecord
   * value) { String path = CMUtil.getInstancePropertyPath(_clusterName,
   * instanceName, instanceProperty); String parentPath = path + "/" + subPath;
   * if (!_zkClient.exists(parentPath)) { String[] subPaths =
   * subPath.split("/"); String tempPath = path; for (int i = 0; i <
   * subPaths.length; i++) { tempPath = tempPath + "/" + subPaths[i]; if
   * (!_zkClient.exists(tempPath)) { if (instanceProperty.isPersistent()) {
   * _zkClient.createPersistent(tempPath); } else {
   * _zkClient.createEphemeral(tempPath); } } } }
   * 
   * String propertyPath = parentPath + "/" + key;
   * ZKUtil.createOrUpdate(_zkClient, propertyPath, value,
   * instanceProperty.isPersistent(), instanceProperty.isMergeOnUpdate()); }
   * 
   * @Override public List<ZNRecord> getInstancePropertyList(String
   * instanceName, String subPath, InstancePropertyType instanceProperty) {
   * String path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
   * instanceProperty); path = path + "/" + subPath; if (_zkClient.exists(path))
   * { return ZKUtil.getChildren(_zkClient, path); } else { return
   * Collections.emptyList(); } }
   * 
   * @Override public ZNRecord getInstanceProperty(String instanceName,
   * InstancePropertyType instanceProperty, String subPath, String key) { String
   * path = CMUtil.getInstancePropertyPath(_clusterName, instanceName,
   * instanceProperty); String propertyPath = path + "/" + subPath + "/" + key;
   * if (_zkClient.exists(propertyPath)) { ZNRecord record =
   * _zkClient.readData(propertyPath, true); return record; } return null; }
   * 
   * @Override public List<String> getInstancePropertySubPaths(String
   * instanceName, InstancePropertyType instanceProperty) { String path =
   * CMUtil.getInstancePropertyPath(_clusterName, instanceName,
   * instanceProperty); return _zkClient.getChildren(path); }
   * 
   * @Override public void substractInstanceProperty(String instanceName,
   * InstancePropertyType instanceProperty, String subPath, String key, ZNRecord
   * value) { String path = CMUtil.getInstancePropertyPath(_clusterName,
   * instanceName, instanceProperty); String propertyPath = path + "/" + subPath
   * + "/" + key; if (_zkClient.exists(propertyPath)) {
   * ZKUtil.substract(_zkClient, propertyPath, value); } }
   * 
   * // distributed cluster controller
   * 
   * @Override public void createControllerProperty( ControllerPropertyType
   * controllerProperty, ZNRecord value, CreateMode mode) { final String path =
   * CMUtil.getControllerPropertyPath(_clusterName, controllerProperty);
   * _zkClient.create(path, value, mode); // ZKUtil.createOrReplace(_zkClient,
   * path, value, // controllerProperty.isPersistent()); }
   * 
   * @Override public void removeControllerProperty(ControllerPropertyType
   * controllerProperty) { final String path =
   * CMUtil.getControllerPropertyPath(_clusterName, controllerProperty); if
   * (_zkClient.exists(path)) { boolean b = _zkClient.delete(path); if (!b) {
   * logger.warn("Unable to remove property at path:" + path); } } else {
   * logger.warn("No controller property to remove at path:" + path); } }
   * 
   * @Override public void setControllerProperty(ControllerPropertyType
   * controllerProperty, ZNRecord value, CreateMode mode) { final String path =
   * CMUtil.getControllerPropertyPath(_clusterName, controllerProperty); //
   * _zkClient.create(path, mode); ZKUtil.createOrReplace(_zkClient, path,
   * value, controllerProperty.isPersistent()); }
   * 
   * @Override public ZNRecord getControllerProperty( ControllerPropertyType
   * controllerProperty) { final String path =
   * CMUtil.getControllerPropertyPath(_clusterName, controllerProperty);
   * ZNRecord record = _zkClient.<ZNRecord> readData(path, true); return record;
   * }
   * 
   * @Override public ZNRecord getControllerProperty( ControllerPropertyType
   * controllerProperty, String subPath) { final String path =
   * CMUtil.getControllerPropertyPath(_clusterName, controllerProperty) + "/" +
   * subPath; ZNRecord record = _zkClient.<ZNRecord> readData(path, true);
   * return record; }
   * 
   * @Override public PropertyStore<ZNRecord> getStore() { // TODO
   * Auto-generated method stub return null; }
   * 
   * @Override public void removeControllerProperty(ControllerPropertyType type,
   * String id) { String path = CMUtil.getControllerPropertyPath(_clusterName,
   * type) + "/" + id; if (_zkClient.exists(path)) { boolean b =
   * _zkClient.delete(path); if (!b) {
   * logger.warn("Unable to remove property at path:" + path); } } else {
   * logger.warn("No property to remove at path:" + path); }
   * 
   * }
   * 
   * @Override public void setControllerProperty(ControllerPropertyType
   * controllerProperty, String subPath, ZNRecord value, CreateMode mode) {
   * String path = CMUtil.getControllerPropertyPath(_clusterName,
   * controllerProperty); String parentPath =
   * CMUtil.getControllerPropertyPath(_clusterName, controllerProperty) + "/" +
   * subPath; if (!_zkClient.exists(parentPath)) { String[] subPaths =
   * subPath.split("/"); String tempPath = path; for (int i = 0; i <
   * subPaths.length; i++) { tempPath = tempPath + "/" + subPaths[i]; if
   * (!_zkClient.exists(tempPath)) { if (controllerProperty.isPersistent()) {
   * _zkClient.createPersistent(tempPath); } else {
   * _zkClient.createEphemeral(tempPath); } } } } path =
   * CMUtil.getControllerPropertyPath(_clusterName, controllerProperty) + "/" +
   * subPath; ZKUtil .createOrUpdate(_zkClient, path, value,
   * controllerProperty.isPersistent(), controllerProperty.isMergeOnUpdate());
   * 
   * }
   */
  @Override
  public boolean setProperty(PropertyType type, final ZNRecord value,
      String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    String parent = new File(path).getParent();
    if (!_zkClient.exists(parent))
    {
      _zkClient.createPersistent(parent, true);
    }
    if (_zkClient.exists(path))
    {
      DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>()
      {
        @Override
        public ZNRecord update(ZNRecord currentData)
        {
          return value;
        }
      };
      _zkClient.updateDataSerialized(path, updater);
    } else
    {
      if (type.isPersistent())
      {
        _zkClient.createPersistent(path, value);
      } else
      {
        _zkClient.createEphemeral(path, value);
      }
    }
    return true;
  }

  @Override
  public boolean updateProperty(PropertyType type, ZNRecord value,
      String... keys)
  {
    String path = PropertyPathConfig.getPath(type, _clusterName, keys);
    if (type.isUpdateOnlyOnExists())
    {
      ZKUtil.updateIfExists(_zkClient, path, value, type.isMergeOnUpdate());
    } else
    {
      String parent = new File(path).getParent();

      if (!_zkClient.exists(parent))
      {
        _zkClient.createPersistent(parent, true);
      }
      ZKUtil.createOrUpdate(_zkClient, path, value, type.isPersistent(),
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
    } else
    {
      return Collections.emptyList();
    }
  }

  @Override
  public PropertyStore<ZNRecord> getStore()
  {
    // TODO Auto-generated method stub
    return null;

  }
}
