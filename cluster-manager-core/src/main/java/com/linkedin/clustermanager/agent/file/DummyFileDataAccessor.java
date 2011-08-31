package com.linkedin.clustermanager.agent.file;

import java.util.List;

import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.store.PropertyStore;

public class DummyFileDataAccessor implements ClusterDataAccessor
{

  @Override
  public ZNRecord getClusterProperty(ClusterPropertyType clusterProperty, String key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setClusterProperty(ClusterPropertyType clusterProperty,
                                 String key,
                                 ZNRecord value)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateClusterProperty(ClusterPropertyType clusterProperty,
                                    String key,
                                    ZNRecord value)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeClusterProperty(ClusterPropertyType clusterProperty, String key)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public List<ZNRecord> getClusterPropertyList(ClusterPropertyType clusterProperty)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setInstanceProperty(String instanceName,
                                  InstancePropertyType instanceProperty,
                                  String key,
                                  ZNRecord value)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void setInstanceProperty(String instanceName,
                                  InstancePropertyType instanceProperty,
                                  String subPath,
                                  String key,
                                  ZNRecord value)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateInstanceProperty(String instanceName,
                                     InstancePropertyType instanceProperty,
                                     String subPath,
                                     String key,
                                     ZNRecord value)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void substractInstanceProperty(String instanceName,
                                        InstancePropertyType instanceProperty,
                                        String subPath,
                                        String key,
                                        ZNRecord value)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public ZNRecord getInstanceProperty(String instanceName,
                                      InstancePropertyType instanceProperty,
                                      String key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ZNRecord getInstanceProperty(String instanceName,
                                      InstancePropertyType instanceProperty,
                                      String subPath,
                                      String key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ZNRecord> getInstancePropertyList(String instanceName,
                                                InstancePropertyType instanceProperty)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ZNRecord> getInstancePropertyList(String instanceName,
                                                String subPath,
                                                InstancePropertyType instanceProperty)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void removeInstanceProperty(String instanceName,
                                     InstancePropertyType instanceProperty,
                                     String key)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateInstanceProperty(String instanceName,
                                     InstancePropertyType instanceProperty,
                                     String key,
                                     ZNRecord value)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void setClusterProperty(ClusterPropertyType clusterProperty,
                                 String key,
                                 ZNRecord value,
                                 CreateMode mode)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public List<String> getInstancePropertySubPaths(String instanceName,
                                                  InstancePropertyType instanceProperty)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void createControllerProperty(ControllerPropertyType controllerProperty,
                                       ZNRecord value,
                                       CreateMode mode)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeControllerProperty(ControllerPropertyType controllerProperty)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void setControllerProperty(ControllerPropertyType controllerProperty,
                                    ZNRecord value,
                                    CreateMode mode)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public ZNRecord getControllerProperty(ControllerPropertyType controllerProperty)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PropertyStore<ZNRecord> getStore()
  {
    // TODO Auto-generated method stub
    return null;
  }

}
