package org.apache.helix.taskexecution;

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
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;

/**
 * Generic Worker that is a HELIX Participant which on start up joins the cluster and
 * waits for state transitions from Helix.<br/>
 * This class takes taskfactory and taskresultstore as arguments.<br/>
 * As part of state transition @see {@link TaskStateModel},
 * it launches task corresponding to the resource id.
 */
public class Worker implements Runnable {
  private final String _zkAddr;
  private final String _clusterName;
  private final String _instanceName;
  private HelixManager _manager = null;
  private final TaskFactory _taskFactory;
  private final TaskResultStore _taskResultStore;

  public Worker(String zkAddr, String clusterName, String workerId, TaskFactory taskFactory,
      TaskResultStore taskResultStore) {
    _zkAddr = zkAddr;
    _clusterName = clusterName;
    _taskResultStore = taskResultStore;
    _instanceName = "worker_" + workerId;
    _taskFactory = taskFactory;
  }

  public void connect() {
    try {
      _manager =
          HelixManagerFactory.getZKHelixManager(_clusterName, _instanceName,
              InstanceType.PARTICIPANT, _zkAddr);

      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      TaskStateModelFactory modelFactory =
          new TaskStateModelFactory(_instanceName, _taskFactory, _taskResultStore);
      stateMach.registerStateModelFactory(StateModelDefId.from(TaskCluster.DEFAULT_STATE_MODEL),
          modelFactory);

      _manager.connect();

      Thread.currentThread().join();
    } catch (InterruptedException e) {
      System.err.println(" [-] " + _instanceName + " is interrupted ...");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      disconnect();
    }
  }

  public void disconnect() {
    if (_manager != null) {
      _manager.disconnect();
    }
  }

  @Override
  public void run() {
    ZkClient zkclient = null;
    try {
      // add node to cluster if not already added
      zkclient =
          new ZkClient(_zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

      List<String> nodes = admin.getInstancesInCluster(_clusterName);
      if (!nodes.contains(_instanceName)) {
        InstanceConfig config = new InstanceConfig(_instanceName);
        config.setHostName("localhost");
        config.setInstanceEnabled(true);
        admin.addInstance(_clusterName, config);
      }

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Shutting down " + _instanceName);
          disconnect();
        }
      });

      connect();
    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err
          .println("USAGE: java Worker zookeeperAddress redisServerHost redisServerPort workerId");
      System.exit(1);
    }

    final String zkAddr = args[0];
    final String clusterName = TaskCluster.DEFAULT_CLUSTER_NAME;
    final String redisServerHost = args[1];
    final int redisServerPort = Integer.parseInt(args[2]);
    final String workerId = args[3];

    TaskFactory taskFactory = new AnalyticsTaskFactory();
    TaskResultStore taskResultStore =
        new RedisTaskResultStore(redisServerHost, redisServerPort, 1000);
    Worker worker = new Worker(zkAddr, clusterName, workerId, taskFactory, taskResultStore);
    worker.run();
  }

}
