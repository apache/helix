package com.linkedin.clustermanager;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

import com.linkedin.clustermanager.participant.DistClusterControllerElection;
import com.linkedin.clustermanager.participant.DistClusterControllerStateModelFactory;
import com.linkedin.clustermanager.participant.StateMachineEngine;

public class TestHelper
{
  static public ZkServer startZkSever(final String zkAddress, final String rootNamespace)
  {
    List<String> rootNamespaces = new ArrayList<String>();
    rootNamespaces.add(rootNamespace);
    return TestHelper.startZkSever(zkAddress, rootNamespaces);
  }
  
  static public ZkServer startZkSever(final String zkAddress, 
                                      final List<String> rootNamespaces)
  {
    final String logDir = "/tmp/logs";
    final String dataDir = "/tmp/dataDir";
    new File(dataDir).delete();
    
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient)
      {
        for (String rootNamespace : rootNamespaces)
        {
          zkClient.deleteRecursive(rootNamespace);
        }
      }
    };
   
    int port = Integer.parseInt(zkAddress.substring(zkAddress.lastIndexOf(':') + 1,
                                                    zkAddress.length()));
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();
    
    return zkServer;
  }
  
  static public void stopZkServer(ZkServer zkServer)
  {
    if (zkServer != null)
    {
      zkServer.shutdown();
    }
  }
  
  public static Thread startDistClusterController(final String clusterName,
      final String instanceName, final String zkAddr)
  {
    Thread thread = new Thread(new Runnable()
    {
    
      @Override
      public void run()
      {
        // participate CONTROLLOR_CLUSTER and do leader election
        try
        {
          ClusterManager manager 
            = ClusterManagerFactory.getZKBasedManagerForParticipant(clusterName,
                                                                    instanceName,
                                                                    zkAddr);
          
          DistClusterControllerStateModelFactory stateModelFactory 
             = new DistClusterControllerStateModelFactory(zkAddr);
          StateMachineEngine genericStateMachineHandler 
            = new StateMachineEngine(stateModelFactory);
          manager.addMessageListener(genericStateMachineHandler, instanceName);
          
          DistClusterControllerElection leaderElection = new DistClusterControllerElection();
          manager.addControllerListener(leaderElection);
          
          Thread.currentThread().join();
        }
        catch (InterruptedException e)
        {
          // manager.disconnect();
        
        }
        catch (Exception e)
        {
          // TODO Auto-generated catch block
            e.printStackTrace();
        }
      
      }
    
    });
  
    thread.start();
    return thread;
  }

  
}
