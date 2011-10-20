package com.linkedin.clustermanager;

/**
 * factory that creates cluster managers
 * 
 * for zk-based cluster managers, the getZKXXX(..zkClient) that takes a zkClient parameter
 *   are intended for session expiry test purpose
 */
import org.apache.log4j.Logger;

import com.linkedin.clustermanager.agent.file.DynamicFileClusterManager;
import com.linkedin.clustermanager.agent.file.FileBasedClusterManager;
import com.linkedin.clustermanager.agent.file.FileBasedDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZKClusterManager;
import com.linkedin.clustermanager.agent.zk.ZkClient;

public final class ClusterManagerFactory
{
  private static final Logger logger = Logger.getLogger(ClusterManagerFactory.class);
  
  // zk-based cluster manager factory functions
  /**
   * create zk-based cluster participant
   * @param clusterName
   * @param instanceName
   * @param zkConnectString
   * @return
   * @throws Exception
   */
  public static ClusterManager getZKBasedManagerForParticipant(String clusterName, 
      String instanceName, String zkConnectString)
  throws Exception
  {
    ClusterManager manager = new ZKClusterManager(clusterName, instanceName, 
                           InstanceType.PARTICIPANT, zkConnectString);
    return manager;
  }
  
  /**
   * create a zk-based cluster participant with a pre-created zkClient
   *   which is used to simulate session expiry
   *   this function is used for testing purpose
   * @param clusterName
   * @param instanceName
   * @param zkConnectString
   * @param zkClient
   * @return
   * @throws Exception
   */
  public static ClusterManager getZKBasedManagerForParticipant(String clusterName, 
     String instanceName, String zkConnectString, ZkClient zkClient)
  throws Exception
  {
    ClusterManager manager = new ZKClusterManager(clusterName, instanceName, 
                        InstanceType.PARTICIPANT, zkConnectString, zkClient);
    return manager;
  }
  
  /**
   * 
   * @param clusterName
   * @param zkConnectString
   * @return
   * @throws Exception
   */
  public static ClusterManager getZKBasedManagerForSpectator(String clusterName, 
      String zkConnectString) throws Exception
  {
    return new ZKClusterManager(clusterName, InstanceType.SPECTATOR,
        zkConnectString);
  }

  /**
   * create a zk-based cluster controller without a controller name
   * @param clusterName
   * @param zkConnectString
   * @return
   * @throws Exception
   */
  public static ClusterManager getZKBasedManagerForController(String clusterName, 
     String zkConnectString) throws Exception
  {
    return new ZKClusterManager(clusterName, InstanceType.CONTROLLER, zkConnectString);
  }
  
  /**
   * create a zk-based cluster controller with a controller name
   * @param clusterName
   * @param controllerName
   * @param zkConnectString
   * @return
   * @throws Exception
   */
  public static ClusterManager getZKBasedManagerForController(
     String clusterName, String controllerName, String zkConnectString) throws Exception
  {
    ClusterManager manager = new ZKClusterManager(clusterName, controllerName, 
            InstanceType.CONTROLLER, zkConnectString);
    return manager; 
  }

  /**
   * create a zk-based cluster controller with a pre-created zkClient
   *   which is used to simulate session expiry
   *   this function used for testing purpose
   * @param clusterName
   * @param controllerName
   * @param zkConnectString
   * @param zkClient
   * @return
   * @throws Exception
   */
  public static ClusterManager getZKBasedManagerForController(String clusterName, 
    String controllerName, String zkConnectString, ZkClient zkClient) throws Exception
  {
    ClusterManager manager = new ZKClusterManager(clusterName, controllerName, InstanceType.CONTROLLER, 
                                                  zkConnectString, zkClient);
    return manager;
  }

  /**
   * create a zk-based cluster controller in distributed mode {@ClusterManagerMain}
   *   controllers are firstly participants of a special CONTROLLLER_CLUSTER 
   *   and do leader election
   * @param clusterName
   * @param controllerName
   * @param zkConnectString
   * @return
   * @throws Exception
   */
  public static ClusterManager getZKBasedManagerForControllerParticipant(
     String clusterName, String controllerName, String zkConnectString) throws Exception
  {
    ClusterManager manager = new ZKClusterManager(clusterName, controllerName, 
                           InstanceType.CONTROLLER_PARTICIPANT, zkConnectString);
    return manager;
  }
  
  // file-based cluster manager factory functions
  /**
   * create a static file-based cluster participant
   *   
   * @param clusterName
   * @param instanceName
   * @param file
   * @return
   * @throws Exception
   */
  public static ClusterManager getFileBasedManagerForParticipant(
      String clusterName, String instanceName, String file) throws Exception
  {
    return new FileBasedClusterManager(clusterName, instanceName,
        InstanceType.PARTICIPANT, file);
  }

  /**
   * create a dynamic file-based cluster participant
   * @param clusterName
   * @param instanceName
   * @param accessor
   * @return
   * @throws Exception
   */
  public static ClusterManager getFileBasedManagerForParticipant(
    String clusterName, String instanceName, FileBasedDataAccessor accessor) 
  throws Exception
  {
     return new DynamicFileClusterManager(clusterName, instanceName,
       InstanceType.PARTICIPANT, accessor);
  }

  /**
   * create a dynamic file-based cluster controller
   * @param clusterName
   * @param accessor
   * @return
   */
  public static ClusterManager getFileBasedManagerForController(String clusterName, 
      String instanceName, FileBasedDataAccessor accessor)
  {
    return new DynamicFileClusterManager(clusterName, instanceName, InstanceType.CONTROLLER, 
        accessor);
  }
}
