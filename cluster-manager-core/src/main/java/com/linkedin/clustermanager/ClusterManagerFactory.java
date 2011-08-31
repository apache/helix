package com.linkedin.clustermanager;

import com.linkedin.clustermanager.agent.file.FileBasedClusterManager;
import com.linkedin.clustermanager.agent.file.FileBasedDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZKClusterManager;

public final class ClusterManagerFactory
{
  private ClusterManagerFactory()
  {
  }

  public static ClusterManager getZKBasedManagerForParticipant(
      String clusterName, String instanceName, String zkConnectString)
      throws Exception
  {

    return new ZKClusterManager(clusterName, instanceName,
        InstanceType.PARTICIPANT, zkConnectString);
  }

  public static ClusterManager getZKBasedManagerForSpectator(
      String clusterName, String zkConnectString) throws Exception
  {
    return new ZKClusterManager(clusterName, InstanceType.SPECTATOR,
        zkConnectString);
  }

  public static ClusterManager getZKBasedManagerForController(
      String clusterName, String zkConnectString) throws Exception
  {

    return new ZKClusterManager(clusterName, InstanceType.CONTROLLER,
        zkConnectString);
  }
  
  
  // TODO remove this
  public static ClusterManager getFileBasedManagerForParticipant(
      String clusterName, String instanceName, String file) throws Exception
  {

    return new FileBasedClusterManager(clusterName, instanceName,
        InstanceType.PARTICIPANT, file, null);
  }

  public static ClusterManager getFileBasedManagerForParticipant(
    String clusterName, String instanceName, String file, FileBasedDataAccessor accessor) 
  throws Exception
  {

     return new FileBasedClusterManager(clusterName, instanceName,
       InstanceType.PARTICIPANT, file, accessor);
  }

  /**
  public static ClusterManager getFileBasedManagerForController(String clusterName, String file)
  {
    return new FileBasedClusterManager(clusterName, null, InstanceType.CONTROLLER, file, null);
  }
  **/
  
  public static ClusterManager getFileBasedManagerForController(String clusterName, 
      String file, FileBasedDataAccessor accessor)
  {
    return new FileBasedClusterManager(clusterName, null, InstanceType.CONTROLLER, 
        file, accessor);
  }
}
