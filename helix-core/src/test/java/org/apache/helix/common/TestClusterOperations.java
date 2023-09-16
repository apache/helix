package org.apache.helix.common;

import java.util.Optional;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.config.HelixConfigProperty;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.zookeeper.api.client.HelixZkClient;


public final class TestClusterOperations {

  public static String getCurrentLeader(HelixZkClient zkClient, String clusterName) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(zkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    return Optional.ofNullable(accessor.getProperty(keyBuilder.controllerLeader()))
        .map(LiveInstance.class::cast)
        .map(LiveInstance::getInstanceName)
        .orElse(null);
  }

  public static void enablePersistBestPossibleAssignment(HelixZkClient zkClient, String clusterName,
      Boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setPersistBestPossibleAssignment(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  public static void enablePersistIntermediateAssignment(HelixZkClient zkClient, String clusterName,
      Boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setPersistIntermediateAssignment(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  public static void enableTopologyAwareRebalance(HelixZkClient zkClient, String clusterName,
      Boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setTopologyAwareEnabled(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  public static void enableDelayRebalanceInCluster(HelixZkClient zkClient, String clusterName,
      boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setDelayRebalaceEnabled(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  public static void enableDelayRebalanceInInstance(HelixZkClient zkClient, String clusterName,
      String instanceName, boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    instanceConfig.setDelayRebalanceEnabled(enabled);
    configAccessor.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  public static void enableDelayRebalanceInCluster(HelixZkClient zkClient, String clusterName,
      boolean enabled, long delay) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setDelayRebalaceEnabled(enabled);
    clusterConfig.setRebalanceDelayTime(delay);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  public static void enableP2PInCluster(String clusterName, ConfigAccessor configAccessor,
      boolean enable) {
    // enable p2p message in cluster.
    if (enable) {
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
      clusterConfig.enableP2PMessage(true);
      configAccessor.setClusterConfig(clusterName, clusterConfig);
    } else {
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
      clusterConfig.getRecord().getSimpleFields()
          .remove(HelixConfigProperty.P2P_MESSAGE_ENABLED.name());
      configAccessor.setClusterConfig(clusterName, clusterConfig);
    }
  }

  public static void enableP2PInResource(String clusterName, ConfigAccessor configAccessor,
      String dbName, boolean enable) {
    if (enable) {
      ResourceConfig resourceConfig =
          new ResourceConfig.Builder(dbName).setP2PMessageEnabled(true).build();
      configAccessor.setResourceConfig(clusterName, dbName, resourceConfig);
    } else {
      // remove P2P Message in resource config
      ResourceConfig resourceConfig = configAccessor.getResourceConfig(clusterName, dbName);
      if (resourceConfig != null) {
        resourceConfig.getRecord().getSimpleFields()
            .remove(HelixConfigProperty.P2P_MESSAGE_ENABLED.name());
        configAccessor.setResourceConfig(clusterName, dbName, resourceConfig);
      }
    }
  }

  public static void setDelayTimeInCluster(HelixZkClient zkClient, String clusterName, long delay) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setRebalanceDelayTime(delay);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

}
