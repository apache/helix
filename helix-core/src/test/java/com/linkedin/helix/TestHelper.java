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

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.controller.pipeline.Stage;
import com.linkedin.helix.controller.pipeline.StageContext;
import com.linkedin.helix.controller.stages.AttributeName;
import com.linkedin.helix.controller.stages.BestPossibleStateCalcStage;
import com.linkedin.helix.controller.stages.BestPossibleStateOutput;
import com.linkedin.helix.controller.stages.ClusterDataCache;
import com.linkedin.helix.controller.stages.ClusterEvent;
import com.linkedin.helix.controller.stages.CurrentStateComputationStage;
import com.linkedin.helix.manager.file.FileDataAccessor;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.StateModelDefinition;
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

  static public ZkServer startZkSever(final String zkAddress, final String rootNamespace)
      throws Exception
  {
    List<String> rootNamespaces = new ArrayList<String>();
    rootNamespaces.add(rootNamespace);
    return TestHelper.startZkSever(zkAddress, rootNamespaces);
  }

  static public ZkServer startZkSever(final String zkAddress, final List<String> rootNamespaces)
      throws Exception
  {
    System.out.println("Start zookeeper at " + zkAddress + " in thread "
        + Thread.currentThread().getName());

    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";
    FileUtils.deleteDirectory(new File(dataDir));
    FileUtils.deleteDirectory(new File(logDir));
    ZKClientPool.reset();

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
      {
        for (String rootNamespace : rootNamespaces)
        {
          try
          {
            zkClient.deleteRecursive(rootNamespace);
          } catch (Exception e)
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
      System.out.println("Shut down zookeeper at port " + zkServer.getPort() + " in thread "
          + Thread.currentThread().getName());
    }
  }

  public static StartCMResult startDummyProcess(final String zkAddr, final String clusterName,
      final String instanceName) throws Exception
  {
    StartCMResult result = new StartCMResult();
    HelixManager manager = null;
    manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName,
        InstanceType.PARTICIPANT, zkAddr);
    result._manager = manager;
    Thread thread = new Thread(new DummyProcessThread(manager, instanceName));
    result._thread = thread;
    thread.start();

    return result;
  }

  public static StartCMResult startController(final String clusterName,
      final String controllerName, final String zkConnectString, final String controllerMode)
      throws Exception
  {
    final StartCMResult result = new StartCMResult();
    final HelixManager manager = HelixControllerMain.startHelixController(zkConnectString,
        clusterName, controllerName, controllerMode);
    result._manager = manager;

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run()
      {
        // ClusterManager manager = null;

        try
        {

          Thread.currentThread().join();
        } catch (InterruptedException e)
        {
          String msg = "controller:" + controllerName + ", " + Thread.currentThread().getName()
              + " interrupted";
          LOG.info(msg);
          // System.err.println(msg);
        } catch (Exception e)
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
    public HelixManager _manager;

  }

  public static void setupEmptyCluster(ZkClient zkClient, String clusterName)
  {
    String clusterRoot = "/" + clusterName;
    String path;
    zkClient.createPersistent(clusterRoot);
    zkClient.createPersistent(clusterRoot + "/" + PropertyType.STATEMODELDEFS.toString());
    zkClient.createPersistent(clusterRoot + "/" + PropertyType.INSTANCES.toString());
    // zkClient.createPersistent(path + "/" + PropertyType.CONFIGS.toString());
    path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
        ConfigScopeProperty.CLUSTER.toString(), clusterName);
    zkClient.createPersistent(path, true);
    path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
        ConfigScopeProperty.PARTICIPANT.toString());
    zkClient.createPersistent(path);
    path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
        ConfigScopeProperty.RESOURCE.toString());
    zkClient.createPersistent(path);

    zkClient.createPersistent(clusterRoot + "/" + PropertyType.IDEALSTATES.toString());
    zkClient.createPersistent(clusterRoot + "/" + PropertyType.EXTERNALVIEW.toString());
    zkClient.createPersistent(clusterRoot + "/" + PropertyType.LIVEINSTANCES.toString());
    zkClient.createPersistent(clusterRoot + "/" + PropertyType.CONTROLLER.toString());

    clusterRoot = clusterRoot + "/" + PropertyType.CONTROLLER.toString();
    zkClient.createPersistent(clusterRoot + "/" + PropertyType.MESSAGES.toString());
    zkClient.createPersistent(clusterRoot + "/" + PropertyType.HISTORY.toString());
    zkClient.createPersistent(clusterRoot + "/" + PropertyType.ERRORS.toString());
    zkClient.createPersistent(clusterRoot + "/" + PropertyType.STATUSUPDATES.toString());
  }

  /**
   * compare two maps
   * 
   * @param map1
   * @param map2
   * @return
   */
  public static <K, V> boolean compareMap(Map<K, V> map1, Map<K, V> map2)
  {
    boolean isEqual = true;
    if (map1 == null && map2 == null)
    {
      // OK
    } else if (map1 == null && map2 != null)
    {
      if (!map2.isEmpty())
      {
        isEqual = false;
      }
    } else if (map1 != null && map2 == null)
    {
      if (!map1.isEmpty())
      {
        isEqual = false;
      }
    } else
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
        } else
        {
          if (!value.equals(map2.get(key)))
          {
            LOG.debug("different value for key:" + key + "(map1:" + value + ", map2:"
                + map2.get(key) + ")");
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
        } else
        {
          if (!value.equals(map1.get(key)))
          {
            LOG.debug("different value for key:" + key + "(map1:" + map1.get(key) + ", map2:"
                + value + ")");
            isEqual = false;
          }
        }
      }

    }
    return isEqual;
  }

  /**
   * convert T[] to set<T>
   * 
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
   * 
   * @param verifierName
   * @param args
   */
  public static void verifyWithTimeout(String verifierName, long timeout, Object... args)
  {
    final long sleepInterval = 1000; // in ms
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
      // LOG.info(verifierName + ": wait " + ((i + 1) * 1000) + "ms to verify ("
      // + result + ")");
      System.err.println(verifierName + ": wait " + ((i + 1) * 1000) + "ms to verify " + " ("
          + result + ")");
      LOG.debug("args:" + Arrays.toString(args));
      // System.err.println("args:" + Arrays.toString(args));

      if (result == false)
      {
        LOG.error(verifierName + " fails");
        LOG.error("args:" + Arrays.toString(args));
      }

      Assert.assertTrue(result);
    } catch (Exception e)
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
   * verify the best possible state and external view note that DROPPED states
   * are not checked since when kick off the BestPossibleStateCalcStage we are
   * providing an empty current state map
   * 
   * @param zkAddr
   * @param clusterNameSet
   * @param resourceNameSet
   * @return
   */

  public static boolean verifyBestPossAndExtView(String zkAddr, Set<String> clusterNameSet,
      Set<String> resourceNameSet)
  {
    return verifyBestPossAndExtViewExtended(zkAddr, clusterNameSet, resourceNameSet, null, null,
        null);
  }

  /**
   * 
   * @param zkAddr
   * @param clusterNameSet
   * @param resourceNameSet
   * @param disabledInstances
   * @param disabledPartitions
   * @param errorStateMap
   *          : "Resource/partitionKey" -> setOf(instances)
   * @return
   */
  public static boolean verifyBestPossAndExtViewExtended(String zkAddr, Set<String> clusterNameSet,
      Set<String> resourceNameSet, Set<String> disabledInstances,
      Map<String, Set<String>> disabledPartitions, Map<String, Set<String>> errorStateMap)
  {
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    try
    {
      for (String clusterName : clusterNameSet)
      {
        DataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);

        for (String resourceName : resourceNameSet)
        {
          ExternalView extView = accessor.getProperty(ExternalView.class,
              PropertyType.EXTERNALVIEW, resourceName);
          // external view not yet generated
          if (extView == null)
          {
            return false;
          }

          Map<String, IdealState> idealStates = accessor.getChildValuesMap(IdealState.class,
              PropertyType.IDEALSTATES);
          if (!idealStates.containsKey(resourceName))
          {
            LOG.error("No ideal state for " + resourceName);
            return false;
          }
          IdealState idealState = idealStates.get(resourceName);
          int partitions = idealState.getNumPartitions();
          String stateModelName = idealState.getStateModelDefRef();

          Map<String, StateModelDefinition> stateModelDefs = accessor.getChildValuesMap(
              StateModelDefinition.class, PropertyType.STATEMODELDEFS);

          if (!stateModelDefs.containsKey(stateModelName))
          {
            LOG.error("No state model definition " + stateModelName);
            return false;
          }
          StateModelDefinition stateModelDef = stateModelDefs.get(stateModelName);
          String initState = stateModelDef.getInitialState();

          BestPossibleStateOutput bestPossOutput = TestHelper.calcBestPossState(resourceName,
              partitions, stateModelName, clusterName, accessor, errorStateMap);

          // System.out.println("extView:" + extView.getMapFields());
          // System.out.println("BestPoss:" + bestPossOutput);

          // check disabled instances
          if (disabledInstances != null)
          {
            for (Partition partition : bestPossOutput.getResourceMap(resourceName).keySet())
            {
              Map<String, String> bpInstanceMap = bestPossOutput.getInstanceStateMap(resourceName,
                  partition);
              for (String instance : disabledInstances)
              {
                if (bpInstanceMap.containsKey(instance))
                {
                  if (!initState.equals(bpInstanceMap.get(instance))
                      && !"ERROR".equals(bpInstanceMap.get(instance)))
                  {
                    LOG.error("Best possible states should set " + resourceName + "/"
                        + partition.getPartitionName() + " to " + initState + " for " + instance
                        + " (was " + bpInstanceMap.get(instance) + ")");
                    return false;
                  }
                }
              }
            }
          }

          // System.out.println("extView:" + extView.getMapFields());
          // System.out.println("BestPoss:" + bestPossOutput);

          // check disabled <partition, setOf(instance)>
          if (disabledPartitions != null)
          {
            for (String resGroupPartitionKey : disabledPartitions.keySet())
            {
              Map<String, String> retMap = getResourceAndPartitionKey(resGroupPartitionKey);
              String resGroup = retMap.get("RESOURCE");
              String partitionKey = retMap.get("PARTITION");

              if (resourceName.equals(resGroup))
              {
                for (String instance : disabledPartitions.get(partitionKey))
                {
                  Partition partition = new Partition(partitionKey);
                  Map<String, String> bpInstanceMap = bestPossOutput.getInstanceStateMap(
                      resourceName, partition);
                  if (bpInstanceMap.containsKey(instance))
                  {
                    if (!initState.equals(bpInstanceMap.get(instance))
                        && !"ERROR".equals(bpInstanceMap.get(instance)))
                    {
                      LOG.error("Best possible states should set " + resGroup + "/" + partitionKey
                          + " to " + initState + " for " + instance + " (was "
                          + bpInstanceMap.get(instance) + ")");
                      return false;
                    }
                  }
                }
              }
            }
          }

          /**
           * check ERROR state and remove them from the comparison against
           * external view
           */
          if (errorStateMap != null)
          {
            for (String resGroupPartitionKey : errorStateMap.keySet())
            {
              Map<String, String> retMap = getResourceAndPartitionKey(resGroupPartitionKey);
              String resGroup = retMap.get("RESOURCE");
              String partitionKey = retMap.get("PARTITION");

              if (resGroup.equals(resourceName))
              {
                for (String instance : errorStateMap.get(partitionKey))
                {
                  Partition partition = new Partition(partitionKey);
                  Map<String, String> stateMap = bestPossOutput.getInstanceStateMap(resourceName,
                      partition);
                  stateMap.put(instance, "ERROR");
                  bestPossOutput.setState(resourceName, partition, stateMap);
                }
              }
            }
          }

          // every entry in external view is contained in best possible state
          for (Map.Entry<String, Map<String, String>> entry : extView.getRecord().getMapFields()
              .entrySet())
          {
            String partitionName = entry.getKey();
            Map<String, String> evInstanceMap = entry.getValue();

            Map<String, String> bpInstanceMap = bestPossOutput.getInstanceStateMap(resourceName,
                new Partition(partitionName));

            boolean result = TestHelper.<String, String> compareMap(evInstanceMap, bpInstanceMap);
            if (result == false)
            {
              LOG.info("verifyBestPossAndExtViewExt() fails for cluster:" + clusterName);
              return false;
            }
          }

          // every entry in best possible state is contained in external view
          for (Map.Entry<Partition, Map<String, String>> entry : bestPossOutput.getResourceMap(
              resourceName).entrySet())
          {
            String partitionName = entry.getKey().getPartitionName();
            Map<String, String> bpInstanceMap = entry.getValue();

            Map<String, String> evInstanceMap = extView.getStateMap(partitionName);

            boolean result = TestHelper.<String, String> compareMap(evInstanceMap, bpInstanceMap);
            if (result == false)
            {
              LOG.info("verifyBestPossAndExtViewExt() fails for cluster:" + clusterName);
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
                instanceName, sessionId, resourceName);
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
                ZNRecord update = accessor.getProperty(PropertyType.STATUSUPDATES, instanceName,
                    sessionId, resourceName, partitionKey);
                String updateStr = update.toString().toLowerCase();
                if (updateStr.indexOf("error") != -1)
                {
                  LOG.error("ERROR in statusUpdate. instance:" + instance + ", resource:"
                      + resourceName + ", partitionKey:" + partitionKey + ", statusUpdate:"
                      + update);
                  return false;
                }
              }
            }
          }
        }
      }

      return true;
    } finally
    {
      zkClient.close();
    }
  }

  // for file-based cluster manager
  public static boolean verifyBestPossAndExtViewFile(String resourceName, int partitions,
      String stateModelName, Set<String> clusterNameSet,
      FilePropertyStore<ZNRecord> filePropertyStore)
  {
    for (String clusterName : clusterNameSet)
    {
      DataAccessor accessor = new FileDataAccessor(filePropertyStore, clusterName);

      ExternalView extView = accessor.getProperty(ExternalView.class, PropertyType.EXTERNALVIEW,
          resourceName);
      // external view not yet generated
      if (extView == null)
      {
        return false;
      }

      BestPossibleStateOutput bestPossOutput = calcBestPossState(resourceName, partitions,
          stateModelName, clusterName, accessor, null);

      // System.out.println("extView:" + externalView.getMapFields());
      // System.out.println("BestPoss:" + output);

      // every entry in external view is contained in best possible state
      for (Map.Entry<String, Map<String, String>> entry : extView.getRecord().getMapFields()
          .entrySet())
      {
        String partitionName = entry.getKey();
        Map<String, String> evInstanceMap = entry.getValue();

        Map<String, String> bpInstanceMap = bestPossOutput.getInstanceStateMap(resourceName,
            new Partition(partitionName));

        boolean result = TestHelper.<String, String> compareMap(evInstanceMap, bpInstanceMap);
        if (result == false)
        {
          LOG.info("verifyBestPossAndExtViewFile() fails for cluster:" + clusterName);
          return false;
        }
      }

      // every entry in best possible state is contained in external view
      for (Map.Entry<Partition, Map<String, String>> entry : bestPossOutput.getResourceMap(
          resourceName).entrySet())
      {
        String partitionName = entry.getKey().getPartitionName();
        Map<String, String> bpInstanceMap = entry.getValue();

        Map<String, String> evInstanceMap = extView.getStateMap(partitionName);

        boolean result = TestHelper.<String, String> compareMap(evInstanceMap, bpInstanceMap);
        if (result == false)
        {
          LOG.info("verifyBestPossAndExtViewFile() fails for cluster:" + clusterName);
          return false;
        }
      }
    }
    return true;
  }

  /**
   * 
   * @param resourceName
   * @param partitions
   * @param stateModelName
   * @param clusterName
   * @param accessor
   * @param errorStateMap
   *          : partition->setOf(instances)
   * @return
   */
  private static BestPossibleStateOutput calcBestPossState(String resourceName, int partitions,
      String stateModelName, String clusterName, DataAccessor accessor,
      Map<String, Set<String>> errorStateMap)
  {
    Map<String, Resource> resourceMap = getResourceMap(resourceName, partitions, stateModelName);
    ClusterEvent event = new ClusterEvent("sampleEvent");

    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);

    ClusterDataCache cache = new ClusterDataCache();
    cache.refresh(accessor);

    event.addAttribute("ClusterDataCache", cache);

    CurrentStateComputationStage csStage = new CurrentStateComputationStage();
    BestPossibleStateCalcStage bpStage = new BestPossibleStateCalcStage();

    runStage(event, csStage);
    runStage(event, bpStage);

    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE
        .toString());

    if (errorStateMap != null)
    {
      for (String resGroupPartitionKey : errorStateMap.keySet())
      {
        Map<String, String> retMap = getResourceAndPartitionKey(resGroupPartitionKey);
        String resGroup = retMap.get("RESOURCE");
        String partitionKey = retMap.get("PARTITION");
        for (String instance : errorStateMap.get(resGroupPartitionKey))
        {
          Map<String, String> instanceState = output.getInstanceStateMap(resGroup, new Partition(
              partitionKey));
          instanceState.put(instance, "ERROR");
        }
      }
    }

    // System.out.println("output:" + output);
    return output;
  }

  private static Map<String, Resource> getResourceMap(String resourceName, int partitions,
      String stateModelName)
  {
    Map<String, Resource> resourceMap = new HashMap<String, Resource>();
    Resource resource = new Resource(resourceName);
    resource.setStateModelDefRef(stateModelName);
    for (int i = 0; i < partitions; i++)
    {
      resource.addPartition(resourceName + "_" + i);
    }
    resourceMap.put(resourceName, resource);

    return resourceMap;
  }

  private static void runStage(ClusterEvent event, Stage stage)
  {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    stage.postProcess();
  }

  // for file-based cluster manager
  public static boolean verifyEmptyCurStateFile(String clusterName, String resourceName,
      Set<String> instanceNames, FilePropertyStore<ZNRecord> filePropertyStore)
  {
    DataAccessor accessor = new FileDataAccessor(filePropertyStore, clusterName);

    for (String instanceName : instanceNames)
    {
      String path = PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, clusterName,
          instanceName);
      List<String> subPaths = accessor.getChildNames(PropertyType.CURRENTSTATES, instanceName);

      for (String previousSessionId : subPaths)
      {
        if (filePropertyStore.exists(path + "/" + previousSessionId + "/" + resourceName))
        {
          CurrentState previousCurrentState = accessor.getProperty(CurrentState.class,
              PropertyType.CURRENTSTATES, instanceName, previousSessionId, resourceName);

          if (previousCurrentState.getRecord().getMapFields().size() != 0)
          {
            return false;
          }
        }
      }
    }
    return true;
  }

  public static boolean verifyEmptyCurStateAndExtView(String clusterName, String resourceName,
      Set<String> instanceNames, String zkAddr)
  {
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    try
    {
      ZKDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);

      for (String instanceName : instanceNames)
      {
        List<String> sessionIds = accessor.getChildNames(PropertyType.CURRENTSTATES, instanceName);

        for (String sessionId : sessionIds)
        {
          CurrentState curState = accessor.getProperty(CurrentState.class,
              PropertyType.CURRENTSTATES, instanceName, sessionId, resourceName);

          if (curState != null && curState.getRecord().getMapFields().size() != 0)
          {
            return false;
          }
        }

        ExternalView extView = accessor.getProperty(ExternalView.class, PropertyType.EXTERNALVIEW,
            resourceName);

        if (extView != null && extView.getRecord().getMapFields().size() != 0)
        {
          return false;
        }

      }

      return true;
    } finally
    {
      zkClient.close();
    }
  }

  public static boolean verifyNotConnected(HelixManager manager)
  {
    return !manager.isConnected();
  }

  public static boolean verifyIdealAndCurState(Set<String> clusterNameSet, String zkAddr)
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

  public static void setupCluster(String clusterName, String ZkAddr, int startPort,
      String participantNamePrefix, String resourceNamePrefix, int resourceNb, int partitionNb,
      int nodesNb, int replica, String stateModelDef, boolean doRebalance) throws Exception
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
      setupTool.addResourceToCluster(clusterName, dbName, partitionNb, stateModelDef);
      if (doRebalance)
      {
        setupTool.rebalanceStorageCluster(clusterName, dbName, replica);
      }
    }
    zkClient.close();
  }

  /**
   * 
   * @param stateMap
   *          : "ResourceName/partitionKey" -> setOf(instances)
   * @param state
   *          : MASTER|SLAVE|ERROR...
   */
  public static void verifyState(String clusterName, String zkAddr,
      Map<String, Set<String>> stateMap, String state)
  {
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    try
    {
      ZKDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);

      for (String resGroupPartitionKey : stateMap.keySet())
      {
        Map<String, String> retMap = getResourceAndPartitionKey(resGroupPartitionKey);
        String resGroup = retMap.get("RESOURCE");
        String partitionKey = retMap.get("PARTITION");

        ExternalView extView = accessor.getProperty(ExternalView.class, PropertyType.EXTERNALVIEW,
            resGroup);
        for (String instance : stateMap.get(resGroupPartitionKey))
        {
          String actualState = extView.getStateMap(partitionKey).get(instance);
          Assert.assertNotNull(actualState, "externalView doesn't contain state for " + resGroup
              + "/" + partitionKey + " on " + instance + " (expect " + state + ")");

          Assert
              .assertEquals(actualState, state, "externalView for " + resGroup + "/" + partitionKey
                  + " on " + instance + " is " + actualState + " (expect " + state + ")");
        }
      }
    } finally
    {
      zkClient.close();
    }
  }

  /**
   * 
   * @param resourcePartition
   *          : key is in form of "resource/partitionKey" or "resource_x"
   * 
   * @return
   */
  private static Map<String, String> getResourceAndPartitionKey(String resourcePartition)
  {
    String resourceName;
    String partitionName;
    int idx = resourcePartition.indexOf('/');
    if (idx > -1)
    {
      resourceName = resourcePartition.substring(0, idx);
      partitionName = resourcePartition.substring(idx + 1);
    } else
    {
      idx = resourcePartition.lastIndexOf('_');
      resourceName = resourcePartition.substring(0, idx);
      partitionName = resourcePartition;
    }

    Map<String, String> retMap = new HashMap<String, String>();
    retMap.put("RESOURCE", resourceName);
    retMap.put("PARTITION", partitionName);
    return retMap;
  }
}
