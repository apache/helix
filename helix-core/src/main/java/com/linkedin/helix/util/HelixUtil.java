package com.linkedin.helix.util;

import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;

public final class HelixUtil
{
  private HelixUtil()
  {
  }

  public static String getPropertyPath(String clusterName, PropertyType type)
  {
    return "/" + clusterName + "/" + type.toString();
  }

  public static String getInstancePropertyPath(String clusterName, String instanceName,
      PropertyType type)
  {
    return getPropertyPath(clusterName, PropertyType.INSTANCES) + "/" + instanceName + "/"
        + type.toString();
  }

  public static String getInstancePath(String clusterName, String instanceName)
  {
    return getPropertyPath(clusterName, PropertyType.INSTANCES) + "/" + instanceName;
  }

  public static String getIdealStatePath(String clusterName, String resourceName)
  {
    return getPropertyPath(clusterName, PropertyType.IDEALSTATES) + "/" + resourceName;
  }

  public static String getIdealStatePath(String clusterName)
  {
    return getPropertyPath(clusterName, PropertyType.IDEALSTATES);
  }

  public static String getLiveInstancesPath(String clusterName)
  {
    return getPropertyPath(clusterName, PropertyType.LIVEINSTANCES);
  }

  // public static String getConfigPath(String clusterName)
  // {
  // return getPropertyPath(clusterName, PropertyType.PARTICIPANT_CONFIGS);
  // }

  // public static String getConfigPath(String clusterName, String instanceName)
  // {
  // return getConfigPath(clusterName) + "/" + instanceName;
  // }

  public static String getMessagePath(String clusterName, String instanceName)
  {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.MESSAGES);
  }

  public static String getCurrentStateBasePath(String clusterName, String instanceName)
  {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.CURRENTSTATES);
  }

  /**
   * Even though this is simple we want to have the mechanism of bucketing the
   * partitions. If we have P partitions and N nodes with K replication factor
   * and D databases. Then on each node we will have (P/N)*K*D partitions. And
   * cluster manager neeeds to maintain watch on each of these nodes for every
   * node. So over all cluster manager will have P*K*D watches which can be
   * quite large given that we over partition.
   * 
   * The other extreme is having one znode per storage per database. This will
   * result in N*D watches which is good. But data in every node might become
   * really big since it has to save partition
   * 
   * Ideally we want to balance between the two models
   * 
   */
  public static String getCurrentStatePath(String clusterName, String instanceName,
      String sessionId, String stateUnitKey)
  {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.CURRENTSTATES) + "/"
        + sessionId + "/" + stateUnitKey;
  }

  public static String getExternalViewPath(String clusterName)
  {
    return getPropertyPath(clusterName, PropertyType.EXTERNALVIEW);
  }

  public static String getStateModelDefinitionPath(String clusterName)
  {
    return getPropertyPath(clusterName, PropertyType.STATEMODELDEFS);
  }

  public static String getExternalViewPath(String clusterName, String resourceName)
  {
    return getPropertyPath(clusterName, PropertyType.EXTERNALVIEW) + "/" + resourceName;
  }

  public static String getLiveInstancePath(String clusterName, String instanceName)
  {
    return getPropertyPath(clusterName, PropertyType.LIVEINSTANCES) + "/" + instanceName;
  }

  public static String getMemberInstancesPath(String clusterName)
  {
    return getPropertyPath(clusterName, PropertyType.INSTANCES);
  }

  public static String getErrorsPath(String clusterName, String instanceName)
  {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.ERRORS);
  }

  public static String getStatusUpdatesPath(String clusterName, String instanceName)
  {
    return getInstancePropertyPath(clusterName, instanceName, PropertyType.STATUSUPDATES);
  }

  public static String getHealthPath(String clusterName, String instanceName)
  {
    return PropertyPathConfig.getPath(PropertyType.HEALTHREPORT, clusterName, instanceName);
  }

  public static String getPersistentStatsPath(String clusterName)
  {
    return PropertyPathConfig.getPath(PropertyType.PERSISTENTSTATS, clusterName);
  }

  public static String getAlertsPath(String clusterName)
  {
    return PropertyPathConfig.getPath(PropertyType.ALERTS, clusterName);
  }

  public static String getAlertStatusPath(String clusterName)
  {
    return PropertyPathConfig.getPath(PropertyType.ALERT_STATUS, clusterName);
  }

  public static String getInstanceNameFromPath(String path)
  {
    // path structure
    // /<cluster_name>/instances/<instance_name>/[currentStates/messages]
    if (path.contains("/" + PropertyType.INSTANCES + "/"))
    {
      String[] split = path.split("\\/");
      if (split.length > 3)
      {
        return split[3];
      }
    }
    return null;
  }

  // distributed cluster controller
  public static String getControllerPath(String clusterName)
  {
    return getPropertyPath(clusterName, PropertyType.CONTROLLER);
  }

  public static String getControllerPropertyPath(String clusterName, PropertyType type)
  {
    return PropertyPathConfig.getPath(type, clusterName);
  }
}
