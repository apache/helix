package com.linkedin.helix;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;

import com.linkedin.helix.agent.file.FileBasedDataAccessor;
import com.linkedin.helix.agent.zk.ZKDataAccessor;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.controller.ClusterManagerMain;
import com.linkedin.helix.controller.pipeline.Stage;
import com.linkedin.helix.controller.pipeline.StageContext;
import com.linkedin.helix.controller.stages.AttributeName;
import com.linkedin.helix.controller.stages.BestPossibleStateCalcStage;
import com.linkedin.helix.controller.stages.BestPossibleStateOutput;
import com.linkedin.helix.controller.stages.ClusterDataCache;
import com.linkedin.helix.controller.stages.ClusterEvent;
import com.linkedin.helix.controller.stages.CurrentStateComputationStage;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.ResourceGroup;
import com.linkedin.helix.model.ResourceKey;
import com.linkedin.helix.store.file.FilePropertyStore;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.util.ZKClientPool;

public class TestHelper
{
  private static final Logger LOG = Logger.getLogger(TestHelper.class);

  static public ZkServer startZkSever(final String zkAddress) throws Exception
  {
    List<String> empty = Collections.emptyList();
    return TestHelper.startZkSever(zkAddress, empty);
  }

  static public ZkServer startZkSever(final String zkAddress, final String rootNamespace) throws Exception
  {
    List<String> rootNamespaces = new ArrayList<String>();
    rootNamespaces.add(rootNamespace);
    return TestHelper.startZkSever(zkAddress, rootNamespaces);
  }

  static public ZkServer startZkSever(final String zkAddress, final List<String> rootNamespaces) throws Exception
  {
    System.out.println("Start zookeeper at " + zkAddress
                       + " in thread " + Thread.currentThread().getName());

    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";
    FileUtils.deleteDirectory(new File(dataDir));
    FileUtils.deleteDirectory(new File(logDir));
    ZKClientPool.reset();

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
      {
        for (String rootNamespace : rootNamespaces)
        {
          try
          {
            zkClient.deleteRecursive(rootNamespace);
          }
          catch (Exception e)
          {
            LOG.error("fail to deleteRecursive path:" + rootNamespace + "\nexception:" + e);
          }
        }
      }
    };

    int port = Integer.parseInt(zkAddress.substring(zkAddress.lastIndexOf(':') + 1));
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();

    return zkServer;
  }

  static public void stopZkServer(ZkServer zkServer)
  {
    if (zkServer != null)
    {
      zkServer.shutdown();
      System.out.println("Shut down zookeeper at port " + zkServer.getPort()
                         + " in thread " + Thread.currentThread().getName());
    }
  }

  public static StartCMResult startDummyProcess(final String zkAddr,
                                                final String clusterName,
                                                final String instanceName)
    throws Exception
  {
    StartCMResult result = new StartCMResult();
    ClusterManager manager = null;
    manager = ClusterManagerFactory.getZKClusterManager(clusterName,
                                                        instanceName,
                                                        InstanceType.PARTICIPANT,
                                                        zkAddr);
    result._manager = manager;
    Thread thread = new Thread(new DummyProcessThread(manager, instanceName));
    result._thread = thread;
    thread.start();

    return result;
  }

  public static StartCMResult startController(final String clusterName,
                                              final String controllerName,
                                              final String zkConnectString,
                                              final String controllerMode)
    throws Exception
  {
    final StartCMResult result = new StartCMResult();
    final ClusterManager manager =
        ClusterManagerMain.startClusterManagerMain(zkConnectString,
                                                   clusterName,
                                                   controllerName,
                                                   controllerMode);
    manager.connect();
    result._manager = manager;

    Thread thread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        // ClusterManager manager = null;

        try
        {

          Thread.currentThread().join();
        }
        catch (InterruptedException e)
        {
          String msg =
              "controller:" + controllerName + ", " + Thread.currentThread().getName()
                  + " interrupted";
          LOG.info(msg);
          // System.err.println(msg);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });

    thread.start();
    result._thread = thread;
    return result;
  }

  public static class StartCMResult
  {
    public Thread _thread;
    public ClusterManager _manager;

  }

  public static void setupEmptyCluster(ZkClient zkClient, String clusterName)
  {
    String path = "/" + clusterName;
    zkClient.createPersistent(path);
    zkClient.createPersistent(path + "/" + PropertyType.STATEMODELDEFS.toString());
    zkClient.createPersistent(path + "/" + PropertyType.INSTANCES.toString());
    zkClient.createPersistent(path + "/" + PropertyType.CONFIGS.toString());
    zkClient.createPersistent(path + "/" + PropertyType.IDEALSTATES.toString());
    zkClient.createPersistent(path + "/" + PropertyType.EXTERNALVIEW.toString());
    zkClient.createPersistent(path + "/" + PropertyType.LIVEINSTANCES.toString());
    zkClient.createPersistent(path + "/" + PropertyType.CONTROLLER.toString());

    path = path + "/" + PropertyType.CONTROLLER.toString();
    zkClient.createPersistent(path + "/" + PropertyType.MESSAGES.toString());
    zkClient.createPersistent(path + "/" + PropertyType.HISTORY.toString());
    zkClient.createPersistent(path + "/" + PropertyType.ERRORS.toString());
    zkClient.createPersistent(path + "/" + PropertyType.STATUSUPDATES.toString());
  }


  /**
   * compare two maps
   * @param map1
   * @param map2
   * @return
   */
  public static <K,V> boolean compareMap(Map<K,V> map1, Map<K,V> map2)
  {
    boolean isEqual = true;
    if (map1 == null && map2 == null)
    {
      // OK
    }
    else if (map1 == null && map2 != null)
    {
      if (!map2.isEmpty())
      {
        isEqual = false;
      }
    }
    else if (map1 != null && map2 == null)
    {
      if (!map1.isEmpty())
      {
        isEqual = false;
      }
    }
    else
    {
      // every entry in map1 is contained in map2
      for (Map.Entry<K, V> entry : map1.entrySet())
      {
        K key = entry.getKey();
        V value = entry.getValue();
        if (!map2.containsKey(key))
        {
          LOG.debug("missing value for key:" + key + "(map1:" + value + ", map2:null)");
          isEqual = false;
        }
        else
        {
          if (!value.equals(map2.get(key)))
          {
            LOG.debug("different value for key:" + key + "(map1:" + value
                               + ", map2:" + map2.get(key) + ")");
            isEqual = false;
          }
        }
      }

      // every entry in map2 is contained in map1
      for (Map.Entry<K, V> entry : map2.entrySet())
      {
        K key = entry.getKey();
        V value = entry.getValue();
        if (!map1.containsKey(key))
        {
          LOG.debug("missing value for key:" + key + "(map1:null, map2:" + value + ")");
          isEqual = false;
        }
        else
        {
          if (!value.equals(map1.get(key)))
          {
            LOG.debug("different value for key:" + key + "(map1:" + map1.get(key)
                               + ", map2:" + value + ")");
            isEqual = false;
          }
        }
      }

    }
    return isEqual;
  }

  /**
   * convert T[] to set<T>
   * @param s
   * @return
   */
  public static <T> Set<T> setOf(T... s)
  {
    Set<T> set = new HashSet<T>(Arrays.asList(s));
    return set;
  }

  public static void verifyWithTimeout(String verifierName, Object... args)
  {
    verifyWithTimeout(verifierName, 30 * 1000, args);
  }

  /**
   * generic method for verification with a timeout
   * @param verifierName
   * @param args
   */
  public static void verifyWithTimeout(String verifierName, long timeout, Object... args)
  {
    final long sleepInterval = 1000;  // in ms
    final int loop = (int) (timeout / sleepInterval) + 1;
    try
    {
      boolean result = false;
      int i = 0;
      for (; i < loop; i++)
      {
        Thread.sleep(sleepInterval);
        // verifier should be static method
        result = (Boolean) TestHelper.getMethod(verifierName).invoke(null, args);

        if (result == true)
        {
          break;
        }
      }

      // debug
      // LOG.info(verifierName + ": wait " + ((i + 1) * 1000) + "ms to verify (" + result + ")");
      System.err.println(verifierName + ": wait " + ((i + 1) * 1000) + "ms to verify (" + result + ")");
      LOG.debug("args:" + Arrays.toString(args));
      // System.err.println("args:" + Arrays.toString(args));

      if (result == false)
      {
        LOG.error(verifierName + " fails");
        LOG.error("args:" + Arrays.toString(args));
      }

      Assert.assertTrue(result);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private static Method getMethod(String name)
  {
    Method[] methods = TestHelper.class.getMethods();
    for (Method method : methods)
    {
      if (name.equals(method.getName()))
      {
        return method;
      }
    }
    return null;
  }

  /**
   * verify the best possible state and external view
   *   note that DROPPED states are not checked since when kick off the BestPossibleStateCalcStage
   *   we are providing an empty current state map
   * @param resourceGroupName
   * @param partitions
   * @param stateModelName
   * @param clusterNameSet
   * @param zkClient
   * @return
   */
  public static boolean verifyBestPossAndExtView(String resourceGroupName,
                                                 int partitions,
                                                 String stateModelName,
                                                 Set<String> clusterNameSet,
                                                 String zkAddr)
  {
    return verifyBestPossAndExtViewExtended(resourceGroupName,
                                            partitions,
                                            stateModelName,
                                            clusterNameSet,
                                            zkAddr,
                                            null,
                                            null,
                                            null);
  }

  public static boolean verifyBestPossAndExtViewExtended(String resourceGroupName,
                                                         int partitions,
                                                         String stateModelName,
                                                         Set<String> clusterNameSet,
                                                         String zkAddr,
                                                         Set<String> disabledInstances,
                                                         Map<String, Set<String>> disabledPartitions,
                                                         Map<String, Set<String>> errorStateMap)
  {
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    try
    {
      for(String clusterName : clusterNameSet)
      {
        ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
        ExternalView extView = accessor.getProperty(ExternalView.class, PropertyType.EXTERNALVIEW, resourceGroupName);
        // external view not yet generated
        if (extView == null)
        {
          return false;
        }
  
        BestPossibleStateOutput bestPossOutput =
          TestHelper.calcBestPossState(resourceGroupName, partitions, stateModelName, clusterName, accessor);
  
  //      System.out.println("extView:" + extView.getMapFields());
  //      System.out.println("BestPoss:" + bestPossOutput);
  
        /**
         * check disabled instances
         */
        if (disabledInstances != null)
        {
          for (ResourceKey resourceKey
              : bestPossOutput.getResourceGroupMap(resourceGroupName).keySet())
          {
            Map<String, String> bpInstanceMap = bestPossOutput.getResourceGroupMap(resourceGroupName).get(resourceKey);
            for (String instance : disabledInstances)
            {
              if (bpInstanceMap.containsKey(instance))
              {
                // TODO use state model def's initial state instead
                if (!bpInstanceMap.get(instance).equals("OFFLINE"))
                {
                  LOG.error("Best possible states should set OFFLINE for instance:" + instance
                          + " but was " + bpInstanceMap.get(instance));
                  return false;
                }
                bpInstanceMap.remove(instance);
              }
            }
          }
        }
  
  //      System.out.println("extView:" + extView.getMapFields());
  //      System.out.println("BestPoss:" + bestPossOutput);
  
        /**
         * check disabled <partition, instance>
         */
        if (disabledPartitions != null)
        {
          for (String resourceKey : disabledPartitions.keySet())
          {
            for (String instance : disabledPartitions.get(resourceKey))
            {
  //          String instance = disabledPartitions.get(resourceKey);
  
              Map<String, String> bpInstanceMap = bestPossOutput.getResourceGroupMap(resourceGroupName)
                                                                .get(new ResourceKey(resourceKey));
              if (bpInstanceMap == null || !bpInstanceMap.containsKey(instance))
              {
                LOG.error("Best possible states does NOT contains states for " + resourceGroupName + ":" + resourceKey
                        + " -> " + instance);
                return false;
              }
    
              // TODO use state model def's initial state instead
              if (!bpInstanceMap.get(instance).equals("OFFLINE"))
              {
                return false;
              }
              bpInstanceMap.remove(instance);
            }
          }
        }
  
        /**
         * check ERROR state and remove them from the comparison against external view
         */
        if (errorStateMap != null)
        {
  //        for (Map.Entry<String, String> entry : errorStateMap.entrySet())
          for (String resourceKey : errorStateMap.keySet())
          {
  //          String resourceKey = entry.getKey();
  //          String instance = entry.getValue();
            for (String instance : errorStateMap.get(resourceKey))
            {
              Map<String, String> evInstanceMap = extView.getStateMap(resourceKey);
              if (evInstanceMap == null || !evInstanceMap.containsKey(instance))
              {
                LOG.error("External view does NOT contains states for " 
                        + resourceGroupName + ":" + resourceKey
                        + " -> " + instance);
                return false;
              }
              if (!evInstanceMap.get(instance).equals("ERROR"))
              {
                return false;
              }
              evInstanceMap.remove(instance);
            }
          }
        }
  
        // every entry in external view is contained in best possible state
        for (Map.Entry<String, Map<String, String>> entry : extView.getRecord().getMapFields().entrySet())
        {
          String resourceKey = entry.getKey();
          Map<String, String> evInstanceMap = entry.getValue();
  
          Map<String, String> bpInstanceMap =
           bestPossOutput.getInstanceStateMap(resourceGroupName, new ResourceKey(resourceKey));
  
          boolean result = TestHelper.<String,String>compareMap(evInstanceMap, bpInstanceMap);
          if (result == false)
          {
            LOG.info("verifyBestPossAndExtView() fails for cluster:" + clusterName);
            return false;
          }
        }
  
        // every entry in best possible state is contained in external view
        for (Map.Entry<ResourceKey, Map<String, String>> entry
            : bestPossOutput.getResourceGroupMap(resourceGroupName).entrySet())
        {
          String resourceKey = entry.getKey().getResourceKeyName();
          Map<String, String> bpInstanceMap = entry.getValue();
  
          Map<String, String> evInstanceMap = extView.getStateMap(resourceKey);
  
          boolean result = TestHelper.<String,String>compareMap(evInstanceMap, bpInstanceMap);
          if (result == false)
          {
            LOG.info("verifyBestPossAndExtView() fails for cluster:" + clusterName);
            return false;
          }
        }
        
        // verify that no status updates contain ERROR
        List<LiveInstance> instances = accessor.getChildValues(LiveInstance.class, 
                                                               PropertyType.LIVEINSTANCES);
        for (LiveInstance instance : instances)
        {
          String sessionId = instance.getSessionId();
          String instanceName = instance.getInstanceName();
          List<String> partitionKeys = accessor.getChildNames(PropertyType.STATUSUPDATES, 
                                                             instanceName,
                                                             sessionId,
                                                             resourceGroupName);
          if (partitionKeys != null && partitionKeys.size() > 0)
          {
            for (String partitionKey : partitionKeys)
            {
              // skip error partitions
              if (errorStateMap != null && errorStateMap.containsKey(partitionKey))
              {
                if (errorStateMap.get(partitionKey).contains(instanceName))
                {
                  continue;
                }
              }
              ZNRecord update = accessor.getProperty(PropertyType.STATUSUPDATES, 
                                                     instanceName,
                                                     sessionId,
                                                     resourceGroupName,
                                                     partitionKey);
              String updateStr = update.toString().toLowerCase();
              if (updateStr.indexOf("error") != -1)
              {
                LOG.error("ERROR in statusUpdate. instance:" + instance 
                    + ", resourceGroup:" + resourceGroupName + ", partitionKey:" + partitionKey 
                    + ", statusUpdate:" + update);
                return false;
              }
            }
          }       
        }
      }
      
      return true;
    }
    finally
    {
      zkClient.close();
    }
  }

  // for file-based cluster manager
  public static boolean verifyBestPossAndExtViewFile(String resourceGroupName,
                                                     int partitions,
                                                     String stateModelName,
                                                     Set<String> clusterNameSet,
                                                     FilePropertyStore<ZNRecord> filePropertyStore)
  {
    for(String clusterName : clusterNameSet)
    {
      ClusterDataAccessor accessor = new FileBasedDataAccessor(filePropertyStore, clusterName);

      ExternalView extView = accessor.getProperty(ExternalView.class, PropertyType.EXTERNALVIEW, resourceGroupName);
      // external view not yet generated
      if (extView == null)
      {
        return false;
      }

      BestPossibleStateOutput bestPossOutput =
        calcBestPossState(resourceGroupName, partitions, stateModelName, clusterName, accessor);

      // System.out.println("extView:" + externalView.getMapFields());
      // System.out.println("BestPoss:" + output);

      // every entry in external view is contained in best possible state
      for (Map.Entry<String, Map<String, String>> entry : extView.getRecord().getMapFields().entrySet())
      {
        String resourceKey = entry.getKey();
        Map<String, String> evInstanceMap = entry.getValue();

        Map<String, String> bpInstanceMap =
         bestPossOutput.getInstanceStateMap(resourceGroupName, new ResourceKey(resourceKey));

        boolean result = TestHelper.<String,String>compareMap(evInstanceMap, bpInstanceMap);
        if (result == false)
        {
          LOG.info("verifyBestPossAndExtView() fails for cluster:" + clusterName);
          return false;
        }
      }

      // every entry in best possible state is contained in external view
      for (Map.Entry<ResourceKey, Map<String, String>> entry
          : bestPossOutput.getResourceGroupMap(resourceGroupName).entrySet())
      {
        String resourceKey = entry.getKey().getResourceKeyName();
        Map<String, String> bpInstanceMap = entry.getValue();

        Map<String, String> evInstanceMap = extView.getStateMap(resourceKey);

        boolean result = TestHelper.<String,String>compareMap(evInstanceMap, bpInstanceMap);
        if (result == false)
        {
          LOG.info("verifyBestPossAndExtView() fails for cluster:" + clusterName);
          return false;
        }
      }
    }
    return true;
  }

  private static BestPossibleStateOutput calcBestPossState(String resourceGroupName,
                                                          int partitions,
                                                          String stateModelName,
                                                          String clusterName,
                                                          ClusterDataAccessor accessor)
  {
    Map<String, ResourceGroup> resourceGroupMap 
        = getResourceGroupMap(resourceGroupName, partitions, stateModelName);
    ClusterEvent event = new ClusterEvent("sampleEvent");

    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(), resourceGroupMap);

    ClusterDataCache cache = new ClusterDataCache();
    cache.refresh(accessor);
    event.addAttribute("ClusterDataCache", cache);

    CurrentStateComputationStage csStage = new CurrentStateComputationStage();
    BestPossibleStateCalcStage bpStage = new BestPossibleStateCalcStage();

    runStage(event, csStage);
    runStage(event, bpStage);

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());

    // System.out.println("output:" + output);
    return output;
  }

  private static Map<String, ResourceGroup> getResourceGroupMap(String resourceGroupName,
                                                                int partitions,
                                                                String stateModelName)
  {
    Map<String, ResourceGroup> resourceGroupMap = new HashMap<String, ResourceGroup>();
    ResourceGroup resourceGroup = new ResourceGroup(resourceGroupName);
    resourceGroup.setStateModelDefRef(stateModelName);
    for (int i = 0; i < partitions; i++)
    {
      resourceGroup.addResource(resourceGroupName + "_" + i);
    }
    resourceGroupMap.put(resourceGroupName, resourceGroup);

    return resourceGroupMap;
  }

  private static void runStage(ClusterEvent event, Stage stage)
  {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    stage.postProcess();
  }

  // for file-based cluster manager
  public static boolean verifyEmptyCurStateFile(String clusterName,
                                                String resourceGroupName,
                                                Set<String> instanceNames,
                                                FilePropertyStore<ZNRecord> filePropertyStore)
  {
    ClusterDataAccessor accessor = new FileBasedDataAccessor(filePropertyStore, clusterName);

    for (String instanceName : instanceNames)
    {
      String path = PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                               clusterName,
                                               instanceName);
      List<String> subPaths =
          accessor.getChildNames(PropertyType.CURRENTSTATES, instanceName);

      for (String previousSessionId : subPaths)
      {
        if (filePropertyStore.exists(path + "/" + previousSessionId + "/" + resourceGroupName))
        {
          CurrentState previousCurrentState =
              accessor.getProperty(CurrentState.class,
                                   PropertyType.CURRENTSTATES,
                                   instanceName,
                                   previousSessionId,
                                   resourceGroupName);

          if (previousCurrentState.getRecord().getMapFields().size() != 0)
          {
            return false;
          }
        }
      }
    }
    return true;
  }

  public static boolean verifyEmptyCurStateAndExtView(String clusterName,
                                                      String resourceGroupName,
                                                      Set<String> instanceNames,
                                                      String zkAddr)
  {
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    try
    {
      ZKDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
  
      for (String instanceName : instanceNames)
      {
        List<String> sessionIds = accessor.getChildNames(PropertyType.CURRENTSTATES, 
                                                         instanceName);
  
        for (String sessionId : sessionIds)
        {
          CurrentState curState = accessor.getProperty(CurrentState.class,
                                                       PropertyType.CURRENTSTATES,
                                                       instanceName,
                                                       sessionId,
                                                       resourceGroupName);
  
          if (curState != null && curState.getRecord().getMapFields().size() != 0)
          {
            return false;
          }
        }
        
        ExternalView extView = accessor.getProperty(ExternalView.class, 
                                                    PropertyType.EXTERNALVIEW, 
                                                    resourceGroupName);
        
        if (extView != null && extView.getRecord().getMapFields().size() != 0)
        {
          return false;
        }
  
      }
      
      return true;
    }
    finally
    {
      zkClient.close();
    }
  }

  public static boolean verifyNotConnected(ClusterManager manager)
  {
    return !manager.isConnected();
  }

  public static boolean verifyIdealAndCurState(Set<String> clusterNameSet,
                                               String zkAddr)
  {
    for (String clusterName : clusterNameSet)
    {
      boolean result = ClusterStateVerifier.verifyClusterStates(zkAddr, clusterName);
      if (result == false)
      {
        return result;
      }
    }
    return true;
  }


  public static void setupCluster(String clusterName,
                                  String ZkAddr,
                                  int startPort,
                                  String participantNamePrefix,
                                  String resourceNamePrefix,
                                  int resourceNb,
                                  int partitionNb,
                                  int nodesNb,
                                  int replica,
                                  String stateModelDef,
                                  boolean doRebalance) throws Exception
  {
    ZkClient zkClient = new ZkClient(ZkAddr);
    if (zkClient.exists("/" + clusterName))
    {
      LOG.warn("Cluster already exists:" + clusterName + ". Deleting it");
      zkClient.deleteRecursive("/" + clusterName);
    }

    ClusterSetup setupTool = new ClusterSetup(ZkAddr);
    setupTool.addCluster(clusterName, true);

    for (int i = 0; i < nodesNb; i++)
    {
      int port = startPort + i;
      setupTool.addInstanceToCluster(clusterName, participantNamePrefix + ":" + port);
    }

    for (int i = 0; i < resourceNb; i++)
    {
      String dbName = resourceNamePrefix + i;
      setupTool.addResourceGroupToCluster(clusterName,
                                          dbName,
                                          partitionNb,
                                          stateModelDef);
      if (doRebalance)
      {
        setupTool.rebalanceStorageCluster(clusterName, dbName, replica);
      }
    }
    zkClient.close();
  }
}
