package com.linkedin.helix;

/**
 * factory that creates cluster managers
 *
 * for zk-based cluster managers, the getZKXXX(..zkClient) that takes a zkClient parameter
 *   are intended for session expiry test purpose
 */
import org.apache.log4j.Logger;

import com.linkedin.helix.agent.file.DynamicFileClusterManager;
import com.linkedin.helix.agent.file.StaticFileClusterManager;
import com.linkedin.helix.agent.zk.ZKClusterManager;
import com.linkedin.helix.store.file.FilePropertyStore;

public final class ClusterManagerFactory
{
  private static final Logger logger = Logger.getLogger(ClusterManagerFactory.class);

  /**
   * Construct a zk-based cluster manager
   *   enforce all types (PARTICIPANT, CONTROLLER, and SPECTATOR to have a name
   * @param clusterName
   * @param instanceName
   * @param type
   * @param zkAddr
   * @return
   * @throws Exception
   */
  public static ClusterManager getZKClusterManager(String clusterName,
                                                   String instanceName,
                                                   InstanceType type,
                                                   String zkAddr)
  throws Exception
  {
    return new ZKClusterManager(clusterName, instanceName, type, zkAddr);
  }

  /**
   * Construct a file-based cluster manager using a static cluster-view file
   *   the cluster-view file contains pre-computed state transition messages
   *   from initial OFFLINE states to ideal states
   * @param clusterName
   * @param instanceName
   * @param type
   * @param clusterViewFile
   * @return
   * @throws Exception
   */
  public static ClusterManager getStaticFileClusterManager(String clusterName,
                                                           String instanceName,
                                                           InstanceType type,
                                                           String clusterViewFile)
    throws Exception
  {
    if (type != InstanceType.PARTICIPANT)
    {
      throw new IllegalArgumentException("Static file-based cluster manager doesn't support type other than participant");
    }
    return new StaticFileClusterManager(clusterName,
                                        instanceName,
                                        type,
                                        clusterViewFile);
  }

  /**
   * Construct a dynamic file-based cluster manager
   * @param clusterName
   * @param instanceName
   * @param type
   * @param file property store: all dynamic-file based participants/controller
   *   shall use the same file property store to avoid race condition in updating files
   * @return
   * @throws Exception
   */
  public static ClusterManager getDynamicFileClusterManager(String clusterName,
                                                            String instanceName,
                                                            InstanceType type,
                                                            FilePropertyStore<ZNRecord> store)
    throws Exception
  {
    if (type != InstanceType.PARTICIPANT && type != InstanceType.CONTROLLER)
    {
      throw new IllegalArgumentException("Dynamic file-based cluster manager doesn't support types other than participant and controller");
    }

    return new DynamicFileClusterManager(clusterName, instanceName,
                                         type,
                                         store);
  }
}
