package com.linkedin.clustermanager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.store.PropertyStore;

public interface ClusterDataAccessor
{
  public enum Type
  {
    CLUSTER, INSTANCE, CONTROLLER;
  }

  public enum PropertyType
  {
    // @formatter:off
    // CLUSTER PROPERTIES
    CONFIGS(Type.CLUSTER, true, false), 
    LIVEINSTANCES(Type.CLUSTER, false,false),
    INSTANCES(Type.CLUSTER, true, false),
    IDEALSTATES(Type.CLUSTER,false, false), 
    EXTERNALVIEW(Type.CLUSTER, true, false), 
    STATEMODELDEFS(Type.CLUSTER, true, false), 
    CONTROLLER(Type.CLUSTER, true, false), 
    //INSTANCE PROPERTIES
    MESSAGES(Type.INSTANCE, true, true, true), 
    CURRENTSTATES(Type.INSTANCE, true,true, false),
    STATUSUPDATES(Type.INSTANCE, true, true, false), 
    ERRORS(Type.INSTANCE, true, true), 
    HEALTHREPORT(Type.INSTANCE, true, true,false), 
    //CONTROLLER PROPERTY
    LEADER(Type.CONTROLLER, false, false, true), 
    HISTORY(Type.CONTROLLER, true, true, true),
    PAUSE(Type.CONTROLLER, false,false, true),
    MESSAGES_CONTROLLER(Type.CONTROLLER, true, false, true), 
    STATUSUPDATES_CONTROLLER(Type.CONTROLLER, true, true, true), 
    ERRORS_CONTROLLER(Type.CONTROLLER,true, true, true);
    // @formatter:on
    Type type;
    boolean isPersistent;

    boolean mergeOnUpdate;

    boolean updateOnlyOnExists;

    private PropertyType(Type type, boolean isPersistent, boolean mergeOnUpdate)
    {
      this(type, isPersistent, mergeOnUpdate, false);
    }

    private PropertyType(Type type, boolean isPersistent,
        boolean mergeOnUpdate, boolean updateOnlyOnExists)
    {
      this.type = type;
      this.isPersistent = isPersistent;
      this.mergeOnUpdate = mergeOnUpdate;
      this.updateOnlyOnExists = updateOnlyOnExists;
    }

    public Type getType()
    {
      return type;
    }

    public void setType(Type type)
    {
      this.type = type;
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

  class PropertyPathConfig
  {
    static Map<PropertyType, Map<Integer, String>> templateMap = new HashMap<ClusterDataAccessor.PropertyType, Map<Integer, String>>();
    static
    {
      //@formatter:off
      addEntry(PropertyType.CONFIGS,1,"/{clusterName}/CONFIGS");
      addEntry(PropertyType.CONFIGS,2,"/{clusterName}/CONFIGS/{instanceName}");
      addEntry(PropertyType.LIVEINSTANCES,1,"/{clusterName}/LIVEINSTANCES");
      addEntry(PropertyType.LIVEINSTANCES,2,"/{clusterName}/LIVEINSTANCES/{instanceName}");
      addEntry(PropertyType.INSTANCES,1,"/{clusterName}/INSTANCES");
      addEntry(PropertyType.INSTANCES,2,"/{clusterName}/INSTANCES/{instanceName}");
      addEntry(PropertyType.IDEALSTATES,1,"/{clusterName}/IDEALSTATES");
      addEntry(PropertyType.IDEALSTATES,2,"/{clusterName}/IDEALSTATES/{resourceGroupName}");
      addEntry(PropertyType.EXTERNALVIEW,1,"/{clusterName}/EXTERNALVIEW");
      addEntry(PropertyType.EXTERNALVIEW,2,"/{clusterName}/EXTERNALVIEW/{resourceGroupName}");
      addEntry(PropertyType.STATEMODELDEFS,1,"/{clusterName}/STATEMODELDEFS");
      addEntry(PropertyType.STATEMODELDEFS,2,"/{clusterName}/STATEMODELDEFS/{stateModelName}");
      addEntry(PropertyType.CONTROLLER,1,"/{clusterName}/CONTROLLER");
      //INSTANCE
      addEntry(PropertyType.MESSAGES,2,"/{clusterName}/INSTANCES/{instanceName}/MESSAGES");
      addEntry(PropertyType.MESSAGES,3,"/{clusterName}/INSTANCES/{instanceName}/MESSAGES/{msgId}");
      addEntry(PropertyType.CURRENTSTATES,2,"/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES");
      addEntry(PropertyType.CURRENTSTATES,3,"/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}");
      addEntry(PropertyType.CURRENTSTATES,4,"/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}/{resourceGroupName}");
      addEntry(PropertyType.STATUSUPDATES,2,"/{clusterName}/STATUSUPDATES/{instanceName}/STATUSUPDATES");
      addEntry(PropertyType.STATUSUPDATES,4,"/{clusterName}/STATUSUPDATES/{instanceName}/STATUSUPDATES/{subPath}/{recordName}");
      addEntry(PropertyType.ERRORS,2,"/{clusterName}/ERRORS/{instanceName}/ERRORS/");
      addEntry(PropertyType.ERRORS,4,"/{clusterName}/ERRORS/{instanceName}/ERRORS/{subPath}/{recordName}");
      addEntry(PropertyType.HEALTHREPORT,2,"/{clusterName}/HEALTHREPORT/{instanceName}/HEALTHREPORT");
      addEntry(PropertyType.HEALTHREPORT,3,"/{clusterName}/HEALTHREPORT/{instanceName}/HEALTHREPORT/{reportName}");
      //CONTROLLER
      addEntry(PropertyType.MESSAGES_CONTROLLER,1,"/{clusterName}/CONTROLLER/MESSAGES");
      addEntry(PropertyType.MESSAGES_CONTROLLER,2,"/{clusterName}/CONTROLLER/MESSAGES/{msgId}");
      addEntry(PropertyType.ERRORS_CONTROLLER,1,"/{clusterName}/CONTROLLER/ERRORS");
      addEntry(PropertyType.ERRORS_CONTROLLER,2,"/{clusterName}/CONTROLLER/ERRORS/{errorId}");
      addEntry(PropertyType.STATUSUPDATES_CONTROLLER,1,"/{clusterName}/CONTROLLER/STATUSUPDATES");
      addEntry(PropertyType.STATUSUPDATES_CONTROLLER,2,"/{clusterName}/CONTROLLER/STATUSUPDATES/{statusId}");
      addEntry(PropertyType.LEADER,2,"/{clusterName}/CONTROLLER/LEADER");
      addEntry(PropertyType.HISTORY,2,"/{clusterName}/CONTROLLER/HISTORY");
      addEntry(PropertyType.PAUSE,2,"/{clusterName}/CONTROLLER/PAUSE");
      
      //@formatter:on

    }
    static Pattern pattern = Pattern.compile("(\\{.+?\\})");

    private static void addEntry(PropertyType type, int numKeys, String template)
    {
      if (!templateMap.containsKey(type))
      {
        templateMap.put(type, new HashMap<Integer, String>());
      }
      templateMap.get(type).put(numKeys, template);
    }

    public static String getPath(PropertyType type, String clusterName,
        String... keys)
    {
      if (clusterName == null)
      {
        return null;
      }
      if (keys == null)
      {
        keys = new String[] {};
      }
      String template = null;
      if (templateMap.containsKey(type))
      {
        // keys.length+1 since we add clusterName
        template = templateMap.get(type).get(keys.length + 1);
      }

      String result = null;

      if (template != null)
      {
        result = template;
        Matcher matcher = pattern.matcher(template);
        int count = 0;
        while (matcher.find())
        {
          count = count + 1;
          String var = matcher.group();
          if (count == 1)
          {
            System.out.printf("Replacing %s with %s\n", var, clusterName);
            result = result.replace(var, clusterName);
          } else
          {
            System.out.printf("Replacing %s with %s\n", var, keys[count - 2]);
            result = result.replace(var, keys[count - 2]);
          }
        }
      }
      return result;
    }
  }

  /**
   * Set a property, overwrite if it exists and creates if not exists
   * 
   * @param type
   * @param value
   * @param keys
   * @true if the operation was successful
   */
  public boolean setProperty(PropertyType type, ZNRecord value, String... keys);

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
  public boolean updateProperty(PropertyType type, ZNRecord value,
      String... keys);

  /**
   * Return the property value, it must be a leaf
   * 
   * @param type
   * @param keys
   *          one or more keys used to get the path of znode
   * @return value, Null if absent or on error
   */
  public ZNRecord getProperty(PropertyType type, String... keys);

  /**
   * Removes the property
   * 
   * @param type
   * @param keys
   * @return
   */
  public boolean removeProperty(PropertyType type, String... keys);

  /**
   * Return the child names of the property
   * 
   * @param type
   * @param keys
   * @return SubPropertyNames
   */
  public List<String> getChildNames(PropertyType type, String... keys);

  /**
   * 
   * @param type
   * @param keys
   *          must point to parent of leaf znodes
   * @return subPropertyValues
   */
  public List<ZNRecord> getChildValues(PropertyType type, String... keys);

  public enum ClusterPropertyType
  {
    CONFIGS(true, false), LIVEINSTANCES(false, false), INSTANCES(true, false), IDEALSTATES(
        false, false), EXTERNALVIEW(true, false), STATEMODELDEFS(true, false), CONTROLLER(
        true, false);

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

  public enum IdealStateConfigProperty
  {
    AUTO, CUSTOMIZED
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

  void setControllerProperty(ControllerPropertyType controllerProperty,
      String subPath, ZNRecord value, CreateMode mode);

  ZNRecord getControllerProperty(ControllerPropertyType controllerProperty);

  ZNRecord getControllerProperty(ControllerPropertyType controllerProperty,
      String subPath);

  PropertyStore<ZNRecord> getStore();

  void removeControllerProperty(ControllerPropertyType messages, String id);
}
