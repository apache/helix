package org.apache.helix.filestore;

import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;

public class FileStore
{
  private final String _zkAddr;
  private final String _clusterName;
  private final String _serverId;
  private HelixManager _manager = null;

  public FileStore(String zkAddr, String clusterName, String serverId)
  {
    _zkAddr = zkAddr;
    _clusterName = clusterName;
    _serverId = serverId;
  }

  public void connect()
  {
    try
    {
      _manager =
          HelixManagerFactory.getZKHelixManager(_clusterName,
                                                _serverId,
                                                InstanceType.PARTICIPANT,
                                                _zkAddr);

      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      FileStoreStateModelFactory modelFactory =
          new FileStoreStateModelFactory(_manager);
      stateMach.registerStateModelFactory(SetupCluster.DEFAULT_STATE_MODEL, modelFactory);
      _manager.connect();
//      _manager.addExternalViewChangeListener(replicator);
      Thread.currentThread().join();
    }
    catch (InterruptedException e)
    {
      System.err.println(" [-] " + _serverId + " is interrupted ...");
    }
    catch (Exception e)
    {
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
      System.err.println("USAGE: java FileStore zookeeperAddress (e.g. localhost:2181) serverId , rabbitmqServer (e.g. localhost)");
      System.exit(1);
    }

    final String zkAddr = args[0];
    final String clusterName = SetupCluster.DEFAULT_CLUSTER_NAME;
    final String consumerId = args[1];

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
      }

      // start consumer
      final FileStore consumer =
          new FileStore(zkAddr, clusterName, "consumer_" + consumerId);

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
