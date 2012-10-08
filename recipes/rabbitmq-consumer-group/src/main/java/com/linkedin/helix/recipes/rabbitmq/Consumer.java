package com.linkedin.helix.recipes.rabbitmq;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.tools.StateModelConfigGenerator;

public class Consumer
{
  private final String _zkAddr;
  private final String _clusterName;
  private final String _consumerId;
  private final String _mqServer;

  public Consumer(String zkAddr, String clusterName, String consumerId, String mqServer)
  {
    _zkAddr = zkAddr;
    _clusterName = clusterName;
    _consumerId = consumerId;
    _mqServer = mqServer;
  }

  public void connects()
  {
    HelixManager manager = null;
    try
    {
      manager = HelixManagerFactory.getZKHelixManager(_clusterName, _consumerId,
          InstanceType.PARTICIPANT, _zkAddr);

      StateMachineEngine stateMach = manager.getStateMachineEngine();
      ConsumerStateModelFactory modelFactory = new ConsumerStateModelFactory(_consumerId, _mqServer);
      stateMach.registerStateModelFactory("MasterSlave", modelFactory);

      manager.connect();

      Thread.currentThread().join();
    } catch (InterruptedException e)
    {
      System.err.println(" [-] " + _consumerId + " is interrupted ...");
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally
    {
      if (manager != null)
      {
        manager.disconnect();
      }
    }
  }

//  static void setupCluster(String zkAddr, String clusterName)
//  {
//    // add cluster
//    ZkClient zkclient = null;
//
//    try
//    {
//      zkclient = new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
//          ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
//      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);
//      admin.addCluster(clusterName, true);
//
//      // add state model definition
//      StateModelConfigGenerator generator = new StateModelConfigGenerator();
//      admin.addStateModelDef(clusterName, "MasterSlave",
//          new StateModelDefinition(generator.generateConfigForMasterSlave()));
//
//      // add 5 participants: "consumer_0-4"
//      for (int i = 0; i < 5; i++)
//      {
//        // int port = 12918 + i;
//        InstanceConfig config = new InstanceConfig("consumer_" + i);
//        config.setHostName("localhost");
//        config.setInstanceEnabled(true);
//        admin.addInstance(clusterName, config);
//      }
//
//      // add resource "topic" which has 60 partitions
//      String resourceName = "topic";
//      admin.addResource(clusterName, resourceName, 60, "MasterSlave");
//
//      // rebalance resource "topic" using 3 replicas
//      admin.rebalance(clusterName, resourceName, 3);
//
//    } finally
//    {
//      if (zkclient != null)
//      {
//        zkclient.close();
//      }
//    }
//  }

  public static void main(String[] args) throws Exception
  {
    if (args.length < 3)
    {
      System.err.println("USAGE: java Consumer zookeeperAddress (e.g. localhost:2181) consumerId (0-4), rabbitmqServer (e.g. localhost)");
      System.exit(1);
    }
    
    final String zkAddr = args[0];  // "zzhang-ld:2191";
    // final String clusterName = "consumer-cluster";
    final String clusterName = SetupConsumerCluster.CLUSTER_NAME;
    final String consumerId = args[1];
    final String mqServer = args[2]; // "zzhang-ld";

//    setupCluster(zkAddr, clusterName);

    // start consumer
    Consumer consumer = new Consumer(zkAddr, clusterName, "consumer_" + consumerId, mqServer);
    consumer.connects();

    
    
    // start 4 consumers
//    Thread[] consumers = new Thread[5];
//    for (int i = 0; i < 4; i++)
//    {
//      String consumerId = "consumer_" + i;
//      final Consumer consumer = new Consumer(zkAddr, clusterName, consumerId, mqServer);
//
//      consumers[i] = new Thread(new Runnable() {
//
//        @Override
//        public void run()
//        {
//          consumer.connects();
//        }
//      });
//      consumers[i].start();
//    }

//    // start cluster manager
//    new Thread(new Runnable() {
//
//      @Override
//      public void run()
//      {
//        try
//        {
//          HelixControllerMain.main(new String[] { "--zkSvr", zkAddr, "--cluster", clusterName });
//        } catch (Exception e)
//        {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//        }
//      }
//
//    }).start();
//
//    // stop consumer_3
//    System.out.println("Press any key to stop consumer_3 ...");
//    System.in.read();
//    consumers[3].interrupt();
//
//    
//    // start consumer_3 and conumser_4
//    System.out.println("Press any key to start consumer_3 and consumer_4...");
//    System.in.read();
//    for (int i = 3; i < 5; i++)
//    {
//      String consumerId = "consumer_" + i;
//      final Consumer consumer = new Consumer(zkAddr, clusterName, consumerId, mqServer);
//
//      consumers[i] = new Thread(new Runnable() {
//
//        @Override
//        public void run()
//        {
//          consumer.connects();
//        }
//      });
//      consumers[i].start();
//    }
  }
}
