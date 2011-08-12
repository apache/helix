package com.linkedin.clustermanager.util;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;

public final class CMUtil
{
  private CMUtil()
  {
  }

  public static String getClusterPropertyPath(String clusterName,
      ClusterPropertyType type)
  {
    return "/" + clusterName + "/" + type.toString();
  }

  public static String getInstancePropertyPath(String clusterName,
      String instanceName, InstancePropertyType type)
  {
    return getClusterPropertyPath(clusterName, ClusterPropertyType.INSTANCES)
        + "/" + instanceName + "/" + type.toString();
  }

  public static String getIdealStatePath(String clusterName,String stateUnitGroup)
  {
    return getClusterPropertyPath(clusterName, ClusterPropertyType.IDEALSTATES) + "/" + stateUnitGroup;
  }
  public static String getIdealStatePath(String clusterName)
  {
    return getClusterPropertyPath(clusterName, ClusterPropertyType.IDEALSTATES);
  }

  public static String getLiveInstancesPath(String clusterName)
  {
    return getClusterPropertyPath(clusterName,
        ClusterPropertyType.LIVEINSTANCES);
  }

  public static String getConfigPath(String clusterName)
  {
    return getClusterPropertyPath(clusterName, ClusterPropertyType.CONFIGS);
  }

  public static String getConfigPath(String clusterName, String instanceName)
  {
    return getConfigPath(clusterName) + "/" + instanceName;
  }

  public static String getMessagePath(String clusterName, String instanceName)
  {
    return getInstancePropertyPath(clusterName, instanceName,
        InstancePropertyType.MESSAGES);
  }

  public static String getCurrentStateBasePath(String clusterName,
      String instanceName)
  {
    return getInstancePropertyPath(clusterName, instanceName,
        InstancePropertyType.CURRENTSTATES);
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
  public static String getCurrentStatePath(String clusterName,
      String instanceName, String sessionId, String stateUnitKey)
  {
    return getInstancePropertyPath(clusterName, instanceName,
        InstancePropertyType.CURRENTSTATES) +"/" + sessionId + "/" + stateUnitKey;
  }

  public static String getExternalViewPath(String clusterName)
  {
    return getClusterPropertyPath(clusterName, ClusterPropertyType.EXTERNALVIEW);
  }
  
  public static String getStateModelDefinitionPath(String clusterName)
  {
    return getClusterPropertyPath(clusterName, ClusterPropertyType.STATEMODELDEFS);
  }

  public static String getExternalViewPath(String clusterName,
      String stateUnitGroup)
  {
    return getClusterPropertyPath(clusterName, ClusterPropertyType.EXTERNALVIEW)
        + "/" + stateUnitGroup;
  }

  public static String getLiveInstancePath(String clusterName,
      String instanceName)
  {
    return getClusterPropertyPath(clusterName,
        ClusterPropertyType.LIVEINSTANCES) + "/" + instanceName;
  }

  public static String getMemberInstancesPath(String clusterName)
  {
    return getClusterPropertyPath(clusterName, ClusterPropertyType.INSTANCES);
  }

  public static String getErrorsPath(String clusterName, String instanceName)
  {
    return getInstancePropertyPath(clusterName, instanceName,
        InstancePropertyType.ERRORS);
  }

  public static String getStatusUpdatesPath(String clusterName,
      String instanceName)
  {
    return getInstancePropertyPath(clusterName, instanceName,
        InstancePropertyType.STATUSUPDATES);
  }

  public static String getInstanceNameFromPath(String path)
  {
    // path structure
    // /<cluster_name>/instances/<instance_name>/[currentStates/messages]
    if (path.contains("/" + ClusterPropertyType.INSTANCES + "/"))
    {
      String[] split = path.split("\\/");
      if (split.length > 3)
      {
        return split[3];
      }
    }
    return null;
  }

}
