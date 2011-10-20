package com.linkedin.clustermanager;

import java.util.List;


import com.linkedin.clustermanager.store.PropertyStore;

public interface ClusterDataAccessor
{
  public enum Type
  {
    CLUSTER, INSTANCE, CONTROLLER;
  }

  /**
   * Set a property, overwrite if it exists and creates if not exists
   * 
   * @param type
   * @param value
   * @param keys
   * @true if the operation was successful
   */
   boolean setProperty(PropertyType type, ZNRecord value, String... keys);

  /**
   * Updates a property, either overwrite or merge based on the
   * propertyType.mergeOnUpdate, fails to update if
   * propertyType.updateOnlyOnExists and does not exist
   * 
   * @param type
   * @param value
   * @param keys
   * @return true if the update was successful
   */
   boolean updateProperty(PropertyType type, ZNRecord value,
      String... keys);

  /**
   * Return the property value, it must be a leaf
   * 
   * @param type
   * @param keys
   *          one or more keys used to get the path of znode
   * @return value, Null if absent or on error
   */
   ZNRecord getProperty(PropertyType type, String... keys);

  /**
   * Removes the property
   * 
   * @param type
   * @param keys
   * @return
   */
   boolean removeProperty(PropertyType type, String... keys);

  /**
   * Return the child names of the property
   * 
   * @param type
   * @param keys
   * @return SubPropertyNames
   */
   List<String> getChildNames(PropertyType type, String... keys);

  /**
   * 
   * @param type
   * @param keys
   *          must point to parent of leaf znodes
   * @return subPropertyValues
   */
   List<ZNRecord> getChildValues(PropertyType type, String... keys);
  
  
  PropertyStore<ZNRecord> getStore();
  
  public enum InstanceConfigProperty
  {
    HOST, PORT, ENABLED
  }

  public enum IdealStateConfigProperty
  {
    AUTO, CUSTOMIZED
  }
  
/*
  public enum PropertyType
  {
    CONFIGS(true, false), LIVEINSTANCES(false, false), INSTANCES(true, false), IDEALSTATES(
        false, false), EXTERNALVIEW(true, false), STATEMODELDEFS(true, false), CONTROLLER(
        true, false);

    boolean isPersistent;

    boolean mergeOnUpdate;

    boolean updateOnlyOnExists;

    private PropertyType(boolean isPersistent, boolean mergeOnUpdate)
    {
      this(isPersistent, mergeOnUpdate, false);
    }

    private PropertyType(boolean isPersistent, boolean mergeOnUpdate,
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



  public enum InstancePropertyType
  {
    MESSAGES(true, true, true), CURRENTSTATES(true, true, false), STATUSUPDATES(
        true, true, false), ERRORS(true, true), HEALTHREPORT(true, true, false);

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
    LEADER(false, false, true), HISTORY(true, true, true), PAUSE(false, false,
        true), MESSAGES(true, false, true), STATUSUPDATES(true, true, true), ERRORS(
        true, true, true);

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

  ZNRecord getProperty(PropertyType clusterProperty, String key);

  void setClusterProperty(PropertyType clusterProperty, String key,
      ZNRecord value);

  void updateClusterProperty(PropertyType clusterProperty, String key,
      ZNRecord value);

  void removeClusterProperty(PropertyType clusterProperty, String key);

  List<ZNRecord> getPropertyList(PropertyType clusterProperty);

  // instance values
  void setInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String key, final ZNRecord value);

  void setInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key,
      final ZNRecord value);

  void updateInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key,
      final ZNRecord value);

  void substractInstanceProperty(String instanceName,
      InstancePropertyType instanceProperty, String subPath, String key,
      final ZNRecord value);

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

  void setClusterProperty(PropertyType clusterProperty, String key,
      ZNRecord value, CreateMode mode);

  List<String> getInstancePropertySubPaths(String instanceName,
      InstancePropertyType instanceProperty);

  // distributed cluster controller
  void createControllerProperty(ControllerPropertyType controllerProperty,
      ZNRecord value, CreateMode mode);

  void removeControllerProperty(ControllerPropertyType controllerProperty);

  void setControllerProperty(ControllerPropertyType controllerProperty,
      ZNRecord value, CreateMode mode);

  void setControllerProperty(ControllerPropertyType controllerProperty,
      String subPath, ZNRecord value, CreateMode mode);

  ZNRecord getControllerProperty(ControllerPropertyType controllerProperty);

  ZNRecord getControllerProperty(ControllerPropertyType controllerProperty,
      String subPath);



  void removeControllerProperty(ControllerPropertyType messages, String id);
  */
}
