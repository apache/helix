package com.linkedin.clustermanager;

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

import com.linkedin.clustermanager.agent.file.FileBasedDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.controller.stages.AttributeName;
import com.linkedin.clustermanager.controller.stages.BestPossibleStateCalcStage;
import com.linkedin.clustermanager.controller.stages.BestPossibleStateOutput;
import com.linkedin.clustermanager.controller.stages.ClusterDataCache;
import com.linkedin.clustermanager.controller.stages.ClusterEvent;
import com.linkedin.clustermanager.controller.stages.CurrentStateOutput;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyLeaderStandbyStateModelFactory;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyOnlineOfflineStateModelFactory;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyStateModel;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyStateModelFactory;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.pipeline.Stage;
import com.linkedin.clustermanager.pipeline.StageContext;
import com.linkedin.clustermanager.store.file.FilePropertyStore;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.util.ZKClientPool;

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

  /**
   * start dummy cluster participant with a pre-created zkClient for testing session
   * expiry
   *
   * @param zkAddr
   * @param clusterName
   * @param instanceName
   * @param zkClient
   * @return
   * @throws Exception
   */
  public static StartCMResult startDummyProcess(final String zkAddr,
                                                     final String clusterName,
                                                     final String instanceName,
                                                     final ZkClient zkClient) throws Exception
  {
    StartCMResult result = new StartCMResult();
    ClusterManager manager = null;
    manager =
        ClusterManagerFactory.getZKBasedManagerForParticipant(clusterName,
                                                              instanceName,
                                                              zkAddr,
                                                              zkClient);
    result._manager = manager;
    Thread thread = new Thread(new DummyProcessThread(manager, instanceName));
    result._thread = thread;
    thread.start();

    return result;
  }

  /**
   * start cluster controller with a pre-created zkClient for testing session expiry
   *
   * @param clusterName
   * @param controllerName
   * @param zkConnectString
   * @param zkClient
   * @return
 * @throws Exception
   */
  public static StartCMResult startClusterController(final String clusterName,
                                              final String controllerName,
                                              final String zkConnectString,
                                              final String controllerMode,
                                              final ZkClient zkClient) throws Exception
  {
    final StartCMResult result = new StartCMResult();
    final ClusterManager manager =
        ClusterManagerMain.startClusterManagerMain(zkConnectString,
                                                   clusterName,
                                                   controllerName,
                                                   controllerMode,
                                                   zkClient);
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
        /*
        finally
        {
          if (manager != null)
          {
            manager.disconnect();
          }
        }
        */
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

  static class DummyProcessThread implements Runnable
  {
    ClusterManager _manager;
    String _instanceName;

    public DummyProcessThread(ClusterManager manager, String instanceName)
    {
      _manager = manager;
      _instanceName = instanceName;
    }

    @Override
    public void run()
    {
      try
      {
        _manager.connect();
        DummyStateModelFactory stateModelFactory = new DummyStateModelFactory(0);
        StateMachineEngine genericStateMachineHandler =
            new StateMachineEngine();
        genericStateMachineHandler.registerStateModelFactory("MasterSlave", stateModelFactory);

        DummyLeaderStandbyStateModelFactory stateModelFactory1 = new DummyLeaderStandbyStateModelFactory(10);
        DummyOnlineOfflineStateModelFactory stateModelFactory2 = new DummyOnlineOfflineStateModelFactory(10);
        genericStateMachineHandler.registerStateModelFactory("LeaderStandby", stateModelFactory1);
        genericStateMachineHandler.registerStateModelFactory("OnlineOffline", stateModelFactory2);
        _manager.getMessagingService()
                .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
                                               genericStateMachineHandler);

        Thread.currentThread().join();
      }
      catch (InterruptedException e)
      {
        String msg =
            "participant:" + _instanceName + ", " + Thread.currentThread().getName()
                + " interrupted";
        LOG.info(msg);
        // System.err.println(msg);

      }
      catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      /*
      finally
      {
        if (_manager != null)
        {
          _manager.disconnect();
        }
      }
      */
    }
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

  /**
   * generic method for verification with a timeout
   * @param verifierName
   * @param args
   */
  public static void verifyWithTimeout(String verifierName, Object... args)
  {
    final long sleepInterval = 1000;  // in ms
    try
    {
      boolean result = false;
      int i = 0;
      for (; i < 30; i++)
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
                                                 ZkClient zkClient)
  {
    for(String clusterName : clusterNameSet)
    {
      ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
      ZNRecord extView = accessor.getProperty(PropertyType.EXTERNALVIEW, resourceGroupName);
      BestPossibleStateOutput bestPossOutput =
        calcBestPossState(resourceGroupName, partitions, stateModelName, clusterName, accessor);

      // external view not yet generated
      if (extView == null)
      {
        return false;
      }
      // System.out.println("extView:" + externalView.getMapFields());
      // System.out.println("BestPoss:" + output);

      // every entry in external view is contained in best possible state
      for (Map.Entry<String, Map<String, String>> entry : extView.getMapFields().entrySet())
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

        Map<String, String> evInstanceMap = extView.getMapField(resourceKey);

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
      ZNRecord extView = accessor.getProperty(PropertyType.EXTERNALVIEW, resourceGroupName);
      BestPossibleStateOutput bestPossOutput =
        calcBestPossState(resourceGroupName, partitions, stateModelName, clusterName, accessor);

      // external view not yet generated
      if (extView == null)
      {
        return false;
      }
      // System.out.println("extView:" + externalView.getMapFields());
      // System.out.println("BestPoss:" + output);

      // every entry in external view is contained in best possible state
      for (Map.Entry<String, Map<String, String>> entry : extView.getMapFields().entrySet())
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

        Map<String, String> evInstanceMap = extView.getMapField(resourceKey);

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
    Map<String, ResourceGroup> resourceGroupMap =
        getResourceGroupMap(resourceGroupName, partitions, stateModelName);
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    ClusterEvent event = new ClusterEvent("sampleEvent");

    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(), resourceGroupMap);
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);

    ClusterDataCache cache = new ClusterDataCache();
    cache.refresh(accessor);
    event.addAttribute("ClusterDataCache", cache);

    BestPossibleStateCalcStage stage = new BestPossibleStateCalcStage();
    runStage(event, stage);

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
          ZNRecord previousCurrentState =
              accessor.getProperty(PropertyType.CURRENTSTATES,
                                   instanceName,
                                   previousSessionId,
                                   resourceGroupName);

          if (previousCurrentState.getMapFields().size() != 0)
          {
            return false;
          }
        }
      }
    }
    return true;
  }

  public static boolean verifyEmptyCurState(String clusterName,
                                            String resourceGroupName,
                                            Set<String> instanceNames,
                                            ZkClient zkClient)
  {
    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);

    for (String instanceName : instanceNames)
    {
      String path = PropertyPathConfig.getPath(PropertyType.CURRENTSTATES,
                                               clusterName,
                                               instanceName);
      List<String> subPaths =
          accessor.getChildNames(PropertyType.CURRENTSTATES, instanceName);

      for (String previousSessionId : subPaths)
      {
        if (zkClient.exists(path + "/" + previousSessionId + "/" + resourceGroupName))
        {
          ZNRecord previousCurrentState =
              accessor.getProperty(PropertyType.CURRENTSTATES,
                                   instanceName,
                                   previousSessionId,
                                   resourceGroupName);

          if (previousCurrentState.getMapFields().size() != 0)
          {
            return false;
          }
        }
      }
    }
    return true;
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

}
