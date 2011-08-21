package com.linkedin.clustermanager;

import java.util.List;

import org.apache.zookeeper.CreateMode;

public interface ClusterDataAccessor
{
  public enum ClusterPropertyType
  {
    CONFIGS(true, false), LIVEINSTANCES(false, false), INSTANCES(true, false), 
    IDEALSTATES(true, false), EXTERNALVIEW(true, false), STATEMODELDEFS(true, false),
    CONTROLLER(true, false);

    boolean isPersistent;

    boolean mergeOnUpdate;

    boolean updateOnlyOnExists;

    private ClusterPropertyType(boolean isPersistent, boolean mergeOnUpdate)
    {
      this(isPersistent, mergeOnUpdate, false);
    }

    private ClusterPropertyType(boolean isPersistent, boolean mergeOnUpdate,
        boolean updateOnlyOnExists)
    {
      this.isPersistent = isPersistent;
      this.mergeOnUpdate = mergeOnUpdate;
      this.updateOnlyOnExists = updateOnlyOnExists;
    }

    public boolean isPersistent()
    {
      return isPersistent;
    }

    public void setPersistent(boolean isPersistent)
    {
      this.isPersistent = isPersistent;
    }

    public boolean isMergeOnUpdate()
    {
      return mergeOnUpdate;
    }

    public void setMergeOnUpdate(boolean mergeOnUpdate)
    {
      this.mergeOnUpdate = mergeOnUpdate;
    }

    public boolean isUpdateOnlyOnExists()
    {
      return updateOnlyOnExists;
    }

    public void setUpdateOnlyOnExists(boolean updateOnlyOnExists)
    {
      this.updateOnlyOnExists = updateOnlyOnExists;
    }

  }

  public enum InstanceConfigProperty
  {
    HOST, PORT, ENABLED
  }

  public enum InstancePropertyType
  {
    MESSAGES(true, true, true), CURRENTSTATES(true, true, false), STATUSUPDATES(
        true, true, false), ERRORS(true, true);

    boolean isPersistent;

    boolean mergeOnUpdate;

    boolean updateOnlyOnExists;

    private InstancePropertyType(boolean isPersistent, boolean mergeOnUpdate)
    {
      this(isPersistent, mergeOnUpdate, false);
    }

    private InstancePropertyType(boolean isPersistent, boolean mergeOnUpdate,
        boolean updateOnlyOnExists)
    {
      this.isPersistent = isPersistent;
      this.mergeOnUpdate = mergeOnUpdate;
      this.updateOnlyOnExists = updateOnlyOnExists;
    }

    public boolean isPersistent()
    {
      return isPersistent;
    }

    public void setPersistent(boolean isPersistent)
    {
      this.isPersistent = isPersistent;
    }

    public boolean isMergeOnUpdate()
    {
      return mergeOnUpdate;
    }

    public void setMergeOnUpdate(boolean mergeOnUpdate)
    {
      this.mergeOnUpdate = mergeOnUpdate;
    }

    public boolean isUpdateOnlyOnExists()
    {
      return updateOnlyOnExists;
    }

    public void setUpdateOnlyOnExists(boolean updateOnlyOnExists)
    {
      this.updateOnlyOnExists = updateOnlyOnExists;
    }
  }

  public enum ControllerPropertyType
  {
    LAEDER(false, false, true), HISTORY(true, true, true);
    
    boolean isPersistent;

    boolean mergeOnUpdate;

    boolean updateOnlyOnExists;

    private ControllerPropertyType(boolean isPersistent, boolean mergeOnUpdate)
    {
      this(isPersistent, mergeOnUpdate, false);
    }

    private ControllerPropertyType(boolean isPersistent, boolean mergeOnUpdate,
        boolean updateOnlyOnExists)
    {
      this.isPersistent = isPersistent;
      this.mergeOnUpdate = mergeOnUpdate;
      this.updateOnlyOnExists = updateOnlyOnExists;
    }

    public boolean isPersistent()
    {
      return isPersistent;
    }

    public void setPersistent(boolean isPersistent)
    {
      this.isPersistent = isPersistent;
    }

    public boolean isMergeOnUpdate()
    {
      return mergeOnUpdate;
    }

    public void setMergeOnUpdate(boolean mergeOnUpdate)
    {
      this.mergeOnUpdate = mergeOnUpdate;
    }

    public boolean isUpdateOnlyOnExists()
    {
      return updateOnlyOnExists;
    }

    public void setUpdateOnlyOnExists(boolean updateOnlyOnExists)
    {
      this.updateOnlyOnExists = updateOnlyOnExists;
    }
  }
  
  ZNRecord getClusterProperty(ClusterPropertyType clusterProperty, String key);

  void setClusterProperty(ClusterPropertyType clusterProperty, String key,
      ZNRecord value);

  void updateClusterProperty(ClusterPropertyType clusterProperty, String key,
      ZNRecord value);

  void removeClusterProperty(ClusterPropertyType clusterProperty, String key);

  List<ZNRecord> getClusterPropertyList(ClusterPropertyType clusterProperty);

  // instance values
  void setInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String key, final ZNRecord value);
  
  void setInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key, final ZNRecord value);
  
  void updateInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key, final ZNRecord value);
  
  void substractInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key, final ZNRecord value);
  
  ZNRecord getInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String key);
  
  ZNRecord getInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key);

  List<ZNRecord> getInstancePropertyList(String instanceName,
      InstancePropertyType instanceProperty);
  
  List<ZNRecord> getInstancePropertyList(String instanceName, String subPath,
      InstancePropertyType instanceProperty);

  void removeInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String key);

  void updateInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String key, ZNRecord value);

  void setClusterProperty(ClusterPropertyType clusterProperty, String key,
      ZNRecord value, CreateMode mode);
  
  List<String> getInstancePropertySubPaths(String instanceName,
      InstancePropertyType instanceProperty);

  // distributed cluster controller
  void createControllerProperty(ControllerPropertyType controllerProperty, 
                                ZNRecord value, CreateMode mode);
  
  void removeControllerProperty(ControllerPropertyType controllerProperty);
  
  void setControllerProperty(ControllerPropertyType controllerProperty, 
                             ZNRecord value, CreateMode mode);
  
  ZNRecord getControllerProperty(ControllerPropertyType controllerProperty);

}
