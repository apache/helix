package com.linkedin.helix.recipes.rabbitmq;

import com.linkedin.helix.controller.HelixControllerMain;

public class StartClusterManager
{
  public static void main(String[] args)
  {
    if (args.length < 1)
    {
      System.err.println("USAGE: java StartClusterManager zookeeperAddress (e.g. localhost:2181");
      System.exit(1);
    }
    
    final String clusterName = SetupConsumerCluster.CLUSTER_NAME;
    final String zkAddr = args[0];
    
    try
    {
      HelixControllerMain.main(new String[] { "--zkSvr", zkAddr, "--cluster", clusterName });
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
