package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;

import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyStateModel;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyStateModelFactory;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.util.ZKClientPool;

public class TestHelper
{
  private static final Logger logger = Logger.getLogger(TestHelper.class);
  // volatile static boolean alreadyRunning = false;
  // static Object lock = new Object();
  private static final Semaphore available = new Semaphore(1, true);

  static public ZkServer startZkSever(final String zkAddress, final String rootNamespace)
  {
    List<String> rootNamespaces = new ArrayList<String>();
    rootNamespaces.add(rootNamespace);
    return TestHelper.startZkSever(zkAddress, rootNamespaces);
  }

  static public ZkServer startZkSever(final String zkAddress, final List<String> rootNamespaces)
  {
    System.out.println("Starting zookeeper at " +zkAddress + " in thread "+ Thread.currentThread().getName());
    /*
    synchronized (lock)
    {
      logger.info("alreadyRunning: "+alreadyRunning+ " for " + Thread.currentThread().getName());
      while (alreadyRunning)
      {
        try
        {
          logger.info("alreadyRunning: "+alreadyRunning+ ". Will wait. " + Thread.currentThread().getName());
          lock.wait();
          logger.info("Woke up " + Thread.currentThread().getName());
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
        }
      }
      logger.info("started zk, alreadyRunning: "+alreadyRunning+ " for " + Thread.currentThread().getName());
      alreadyRunning = true;
    }
    */
    try
    {
      available.acquire();
      System.out.println("started zk for " + Thread.currentThread().getName());
    }
    catch (InterruptedException e1)
    {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    final String logDir = "/tmp/logs";
    final String dataDir = "/tmp/dataDir";

    /*
     * try { FileUtils.deleteDirectory(new File(dataDir)); FileUtils.deleteDirectory(new
     * File(logDir)); } catch (IOException e) { logger.warn("fail to delete dir: " +
     * dataDir + ", " + logDir + "\nexception:" + e); }
     */

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
            logger.error("fail to deleteRecursive path:" + rootNamespace + "\nexception:" + e);
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
      System.out.println("Shutting down ZK " + Thread.currentThread().getName());
      /*
      synchronized (lock)
      {
        alreadyRunning = false;
        lock.notify();
      }
      */
      available.release();
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
  public static DummyProcessResult startDummyProcess(final String zkAddr,
                                                     final String clusterName,
                                                     final String instanceName,
                                                     final ZkClient zkClient) throws Exception
  {
    DummyProcessResult result = new DummyProcessResult();
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
   */
  public static Thread startClusterController(final String clusterName,
                                              final String controllerName,
                                              final String zkConnectString,
                                              final String controllerMode,
                                              final ZkClient zkClient)
  {
    Thread thread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        ClusterManager manager = null;

        try
        {
          manager =
              ClusterManagerMain.startClusterManagerMain(zkConnectString,
                                                         clusterName,
                                                         controllerName,
                                                         controllerMode,
                                                         zkClient);
          Thread.currentThread().join();
        }
        catch (InterruptedException e)
        {
          String msg =
              "controller:" + controllerName + ", " + Thread.currentThread().getName()
                  + " interrupted";
          logger.info(msg);
          // System.err.println(msg);

        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
        finally
        {
          if (manager != null)
          {
            manager.disconnect();
          }
        }
      }
    });

    thread.start();
    return thread;
  }

  public static Thread startClusterController(final String args)
  {
    Thread thread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          ClusterManagerMain.main(createArgs(args));
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });

    thread.start();
    return thread;
  }

  private static String[] createArgs(String str)
  {
    String[] split = str.split("[ ]+");
    logger.info("args=" + Arrays.toString(split));
    return split;
  }

  public static class DummyProcessResult
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

        DummyStateModelFactory stateModelFactory = new DummyStateModelFactory(0);
        StateMachineEngine<DummyStateModel> genericStateMachineHandler =
            new StateMachineEngine<DummyStateModel>(stateModelFactory);
        _manager.getMessagingService()
                .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
                                               genericStateMachineHandler);

        _manager.connect();
        Thread.currentThread().join();
      }
      catch (InterruptedException e)
      {
        String msg =
            "participant:" + _instanceName + ", " + Thread.currentThread().getName()
                + " interrupted";
        logger.info(msg);
        // System.err.println(msg);

      }
      catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      finally
      {
        if (_manager != null)
        {
          _manager.disconnect();
        }
      }
    }
  }
}
