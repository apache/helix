package com.linkedin.helix.recipes.rabbitmq;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.controller.HelixControllerMain;

public class StartClusterManager
{
  public static void main(String[] args)
  {
    if (args.length < 1)
    {
      System.err.println("USAGE: java StartClusterManager zookeeperAddress (e.g. localhost:2181)");
      System.exit(1);
    }
    
    final String clusterName = SetupConsumerCluster.CLUSTER_NAME;
    final String zkAddr = args[0];
    
    try
    {
      final HelixManager manager = HelixControllerMain.startHelixController(zkAddr, clusterName, null,
                                                        HelixControllerMain.STANDALONE);
      
      Runtime.getRuntime().addShutdownHook(new Thread()
      {
        @Override
        public void run()
        {
          System.out.println("Shutting down cluster manager: " + manager.getInstanceName());
          manager.disconnect();
        }
      });
      
      Thread.currentThread().join();
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
