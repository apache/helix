package com.linkedin.clustermanager.core;

import com.linkedin.clustermanager.impl.file.FileBasedClusterManager;
import com.linkedin.clustermanager.impl.zk.ZKClusterManager;

public final class ClusterManagerFactory {
  private ClusterManagerFactory() {
  }

  public static ClusterManager getZKBasedManagerForParticipant(String clusterName,
      String instanceName, String zkConnectString) throws Exception {

    return new ZKClusterManager(clusterName, instanceName, InstanceType.PARTICIPANT,
        zkConnectString);
  }

  public static ClusterManager getFileBasedManagerForParticipant(String clusterName,
      String instanceName, String file) throws Exception {

    return new FileBasedClusterManager(clusterName, instanceName, InstanceType.PARTICIPANT, file);
  }

  public static ClusterManager getZKBasedManagerForSpectator(String clusterName,
      String zkConnectString) throws Exception {
    return new ZKClusterManager(clusterName, InstanceType.SPECTATOR, zkConnectString);
  }

  public static ClusterManager getZKBasedManagerForController(String clusterName,
      String zkConnectString) throws Exception {

    return new ZKClusterManager(clusterName, InstanceType.CONTROLLER, zkConnectString);
  }

}
