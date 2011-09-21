package com.linkedin.clustermanager;

/**
 * factory that creates cluster managers
 * 
 * for zk-based cluster managers, the getZKXXX(..zkClient) that takes a zkClient parameter
 *   are intended for session expiry test purpose
 */
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.agent.file.DynamicFileClusterManager;
import com.linkedin.clustermanager.agent.file.FileBasedClusterManager;
import com.linkedin.clustermanager.agent.file.FileBasedDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZKClusterManager;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;

public final class ClusterManagerFactory
{
  private static final Logger logger = Logger.getLogger(ClusterManagerFactory.class);
  
  // for shutting down multiple cluster managers cleanly when a thread gets interrupted
  private final static Map<String, ConcurrentLinkedQueue<ClusterManager>> _managers 
     = new ConcurrentHashMap<String, ConcurrentLinkedQueue<ClusterManager>>();
  
  private static final PropertyJsonSerializer<String> _serializer 
    = new PropertyJsonSerializer<String>(String.class);
  
  private ClusterManagerFactory()
  {
  }

  private static void addManager(String name, ClusterManager manager)
  {
    try
    {
      name = new String(_serializer.serialize(name));
      if (_managers.get(name) == null)
      {
        _managers.put(name, new ConcurrentLinkedQueue<ClusterManager>());
      }
      _managers.get(name).add(manager);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  public static void disconnectManagers(String name)
  {
    try
    {
      name = new String(_serializer.serialize(name));
      ConcurrentLinkedQueue<ClusterManager> queue = _managers.remove(name);
      logger.info("disconnect cluster managers:" + name);
      if (queue != null)
      {
        for (ClusterManager manager : queue)
        {
          manager.disconnect();
        }
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
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
    addManager(instanceName, manager);
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
    addManager(instanceName, manager);
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
    addManager(controllerName, manager);
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
    addManager(controllerName, manager);
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
    addManager(controllerName, manager);
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
      FileBasedDataAccessor accessor)
  {
    return new DynamicFileClusterManager(clusterName, null, InstanceType.CONTROLLER, 
        accessor);
  }
}
