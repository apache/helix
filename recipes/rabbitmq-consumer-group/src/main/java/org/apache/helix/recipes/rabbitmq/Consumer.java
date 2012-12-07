package org.apache.helix.recipes.rabbitmq;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;

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
      stateMach.registerStateModelFactory(SetupConsumerCluster.DEFAULT_STATE_MODEL, modelFactory);

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
      System.err.println("USAGE: java Consumer zookeeperAddress (e.g. localhost:2181) consumerId (0-2), rabbitmqServer (e.g. localhost)");
      System.exit(1);
    }

    final String zkAddr = args[0];
    final String clusterName = SetupConsumerCluster.DEFAULT_CLUSTER_NAME;
    final String consumerId = args[1];
    final String mqServer = args[2];

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
