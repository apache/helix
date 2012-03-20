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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;

import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.file.FileDataAccessor;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.store.file.FilePropertyStore;
import com.linkedin.helix.tools.ClusterSetup;
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
    ZKHelixAdmin admin = new ZKHelixAdmin(zkClient);
    admin.addCluster(clusterName, true);
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

  public static <T> Map<String, T> startThreadsConcurrently(final int nrThreads,
                                                            final Callable<T> method,
                                                            final long timeout)
  {
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch finishCounter = new CountDownLatch(nrThreads);
    final Map<String, T> resultsMap = new ConcurrentHashMap<String, T>();
    final List<Thread> threadList = new ArrayList<Thread>();

    for (int i = 0; i < nrThreads; i++)
    {
      Thread thread = new Thread()
      {
        @Override
        public void run()
        {
          try
          {
            boolean isTimeout = !startLatch.await(timeout, TimeUnit.SECONDS);
            if (isTimeout)
            {
              LOG.error("Timeout while waiting for start latch");
            }
          }
          catch (InterruptedException ex)
          {
            LOG.error("Interrupted while waiting for start latch");
          }

          try
          {
            T result = method.call();
            if (result != null)
            {
              resultsMap.put("thread_" + this.getId(), result);
            }
            LOG.debug("result=" + result);
          }
          catch (Exception e)
          {
            LOG.error("Exeption in executing " + method.getClass().getName(), e);
          }

          finishCounter.countDown();
        }
      };
      threadList.add(thread);
      thread.start();
    }
    startLatch.countDown();

    // wait for all thread to complete
    try
    {
      boolean isTimeout = !finishCounter.await(timeout, TimeUnit.SECONDS);
      if (isTimeout)
      {
        LOG.error("Timeout while waiting for finish latch. Interrupt all threads");
        for (Thread thread : threadList)
        {
          thread.interrupt();
        }
      }
    }
    catch (InterruptedException e)
    {
      LOG.error("Interrupted while waiting for finish latch", e);
    }

    return resultsMap;
  }
}
