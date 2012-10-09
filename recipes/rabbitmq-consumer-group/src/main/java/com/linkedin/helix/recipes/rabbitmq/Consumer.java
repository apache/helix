package com.linkedin.helix.recipes.rabbitmq;

import java.util.List;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.participant.StateMachineEngine;

public class Consumer
{
  private final String _zkAddr;
  private final String _clusterName;
  private final String _consumerId;
  private final String _mqServer;
  private HelixManager _manager = null;

  public Consumer(String zkAddr, String clusterName, String consumerId, String mqServer)
  {
    _zkAddr = zkAddr;
    _clusterName = clusterName;
    _consumerId = consumerId;
    _mqServer = mqServer;
  }

  public void connect()
  {
    try
    {
      _manager =
          HelixManagerFactory.getZKHelixManager(_clusterName,
                                                _consumerId,
                                                InstanceType.PARTICIPANT,
                                                _zkAddr);

      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      ConsumerStateModelFactory modelFactory =
          new ConsumerStateModelFactory(_consumerId, _mqServer);
      stateMach.registerStateModelFactory("MasterSlave", modelFactory);

      _manager.connect();

      Thread.currentThread().join();
    }
    catch (InterruptedException e)
    {
      System.err.println(" [-] " + _consumerId + " is interrupted ...");
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    finally
    {
      disconnect();
    }
  }

  public void disconnect()
  {
    if (_manager != null)
    {
      _manager.disconnect();
    }
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length < 3)
    {
      System.err.println("USAGE: java Consumer zookeeperAddress (e.g. localhost:2181) consumerId (0-4), rabbitmqServer (e.g. localhost)");
      System.exit(1);
    }

    final String zkAddr = args[0]; // "zzhang-ld:2191";
    final String clusterName = SetupConsumerCluster.CLUSTER_NAME;
    final String consumerId = args[1];
    final String mqServer = args[2]; // "zzhang-ld";

    ZkClient zkclient = null;
    try
    {
      // add node to cluster if not already added
      zkclient =
          new ZkClient(zkAddr,
                       ZkClient.DEFAULT_SESSION_TIMEOUT,
                       ZkClient.DEFAULT_CONNECTION_TIMEOUT,
                       new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

      List<String> nodes = admin.getInstancesInCluster(clusterName);
      if (!nodes.contains("consumer_" + consumerId))
      {
        InstanceConfig config = new InstanceConfig("consumer_" + consumerId);
        config.setHostName("localhost");
        config.setInstanceEnabled(true);
        admin.addInstance(clusterName, config);
        
        // rebalance resource "topic" using replicas=nodes# 
        String resourceName = SetupConsumerCluster.RESOURCE_NAME;
        nodes = admin.getInstancesInCluster(clusterName);
        System.out.println("Nodes in cluster " + clusterName + " are: " + nodes);
        admin.rebalance(clusterName, resourceName, nodes.size());
      }

      // start consumer
      final Consumer consumer =
          new Consumer(zkAddr, clusterName, "consumer_" + consumerId, mqServer);

      Runtime.getRuntime().addShutdownHook(new Thread()
      {
        @Override
        public void run()
        {
          System.out.println("Shutting down consumer_" + consumerId);
          consumer.disconnect();
        }
      });

      consumer.connect();
    }
    finally
    {
      if (zkclient != null)
      {
        zkclient.close();
      }
    }
  }
}
