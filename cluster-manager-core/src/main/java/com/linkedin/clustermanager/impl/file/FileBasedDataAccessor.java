package com.linkedin.clustermanager.impl.file;

import java.util.List;

import com.linkedin.clustermanager.core.ClusterDataAccessor;
import com.linkedin.clustermanager.model.ClusterView;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ZNRecord;

public class FileBasedDataAccessor implements ClusterDataAccessor
{

  // private final ClusterView _clusterView;

  public FileBasedDataAccessor()
  {
    // this._clusterView = new ClusterView();
  }

  @Override
  public void setClusterProperty(ClusterPropertyType clusterProperty,
      String key, ZNRecord value)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void updateClusterProperty(ClusterPropertyType clusterProperty,
      String key, ZNRecord value)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setClusterPropertyList(ClusterPropertyType clusterProperty,
      List<ZNRecord> values)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public ZNRecord getClusterProperty(ClusterPropertyType clusterProperty,
      String key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ZNRecord> getClusterPropertyList(
      ClusterPropertyType clusterProperty)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setInstanceProperty(String instanceName,
      InstancePropertyType clusterProperty, String key, ZNRecord value)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setInstancePropertyList(String instanceName,
      InstancePropertyType clusterProperty, List<ZNRecord> values)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public ZNRecord getInstanceProperty(String instanceName,
      InstancePropertyType clusterProperty, String key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ZNRecord> getInstancePropertyList(String instanceName,
      InstancePropertyType clusterProperty)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void removeInstanceProperty(String instanceName,
      InstancePropertyType type, String key)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void updateInstanceProperty(String instanceName,
      InstancePropertyType type, String hey, ZNRecord value)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public ClusterView getClusterView()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setEphemeralClusterProperty(ClusterPropertyType clusterProperty,
      String key, ZNRecord value)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void removeClusterProperty(ClusterPropertyType clusterProperty,
      String key)
  {
    // TODO Auto-generated method stub
    
  }

 
}
