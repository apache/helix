package com.linkedin.clustermanager.core;

import java.util.List;

import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.model.ClusterView;
import com.linkedin.clustermanager.model.ZNRecord;

public interface ClusterDataAccessor
{
  public enum ClusterPropertyType
  {
    CONFIGS, LIVEINSTANCES, INSTANCES, IDEALSTATES, EXTERNALVIEW
  }
  
  public enum InstancePropertyType
  {
    MESSAGES, CURRENTSTATES, STATUSUPDATES, ERRORS
  }
  
  public interface PropertyCreateModeCalculator
  {
    public CreateMode getClusterPropertyCreateMode();
    
    public CreateMode getInstancePropertyCreateMode();
  }
  
  void setClusterProperty(ClusterPropertyType clusterProperty, String key, ZNRecord value);
  
  void setEphemeralClusterProperty(ClusterPropertyType clusterProperty, String key, ZNRecord value);
  
  void updateClusterProperty(ClusterPropertyType clusterProperty, String key, ZNRecord value);
  
  void removeClusterProperty(ClusterPropertyType clusterProperty, String key);
  
  void setClusterPropertyList(ClusterPropertyType clusterProperty, List<ZNRecord> values);
  
  ZNRecord getClusterProperty(ClusterPropertyType clusterProperty, String key);
  
  List<ZNRecord> getClusterPropertyList(ClusterPropertyType clusterProperty);
  
  // instance values
  void setInstanceProperty(String instanceName, InstancePropertyType instanceProperty, String key, final ZNRecord value);
  
  void setInstancePropertyList(String instanceName, InstancePropertyType instanceProperty, List<ZNRecord> values);
  
  ZNRecord getInstanceProperty(String instanceName, InstancePropertyType instanceProperty, String key);
  
  List<ZNRecord> getInstancePropertyList(String instanceName, InstancePropertyType instanceProperty);
  
  void removeInstanceProperty(String instanceName, InstancePropertyType instanceProperty, String key);
  
  void updateInstanceProperty(String instanceName, InstancePropertyType instanceProperty, String key, ZNRecord value);
  
  ClusterView getClusterView();
  
}
