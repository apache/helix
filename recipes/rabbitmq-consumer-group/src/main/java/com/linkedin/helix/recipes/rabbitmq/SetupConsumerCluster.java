package com.linkedin.helix.recipes.rabbitmq;

import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.tools.StateModelConfigGenerator;

public class SetupConsumerCluster
{
  public static final String CLUSTER_NAME = "consumer-cluster";
  
  public static void main(String[] args)
  {
    if (args.length < 1)
    {
      System.err.println("USAGE: java SetupConsumerCluster zookeeperAddress (e.g. localhost:2181");
      System.exit(1);
    }
    
    final String zkAddr = args[0];
    final String clusterName = CLUSTER_NAME;
    
    ZkClient zkclient = null;
    try
    {
      zkclient = new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
          ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);
      
      // add cluster
      admin.addCluster(clusterName, true);

      // add state model definition
      StateModelConfigGenerator generator = new StateModelConfigGenerator();
      admin.addStateModelDef(clusterName, "MasterSlave",
          new StateModelDefinition(generator.generateConfigForMasterSlave()));

      // add 5 participants: "consumer_0-4"
      for (int i = 0; i < 5; i++)
      {
        // int port = 12918 + i;
        InstanceConfig config = new InstanceConfig("consumer_" + i);
        config.setHostName("localhost");
        config.setInstanceEnabled(true);
        admin.addInstance(clusterName, config);
      }

      // add resource "topic" which has 60 partitions
      String resourceName = "topic";
      admin.addResource(clusterName, resourceName, 60, "MasterSlave");

      // rebalance resource "topic" using 3 replicas
      admin.rebalance(clusterName, resourceName, 3);

    } finally
    {
      if (zkclient != null)
      {
        zkclient.close();
      }
    }
  }
}
