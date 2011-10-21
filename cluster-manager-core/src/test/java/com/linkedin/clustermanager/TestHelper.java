package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  static public ZkServer startZkSever(final String zkAddress)
  {
    List<String> empty = Collections.emptyList();
    return TestHelper.startZkSever(zkAddress, empty);
  }

  static public ZkServer startZkSever(final String zkAddress, final String rootNamespace)
  {
    List<String> rootNamespaces = new ArrayList<String>();
    rootNamespaces.add(rootNamespace);
    return TestHelper.startZkSever(zkAddress, rootNamespaces);
  }

  static public ZkServer startZkSever(final String zkAddress, final List<String> rootNamespaces)
  {
    System.out.println("Starting zookeeper at " + zkAddress + " in thread "+ Thread.currentThread().getName());
    
    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";

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
      System.out.println("Shutting down ZK at port " + zkServer.getPort() 
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
   */
  public static StartCMResult startClusterController(final String clusterName,
                                              final String controllerName,
                                              final String zkConnectString,
                                              final String controllerMode,
                                              final ZkClient zkClient)
  {
    final StartCMResult result = new StartCMResult();
    final ClusterManager manager =
        ClusterManagerMain.startClusterManagerMain(zkConnectString,
                                                   clusterName,
                                                   controllerName,
                                                   controllerMode,
                                                   zkClient);
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
          logger.info(msg);
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
}
