package org.apache.helix.filestore;

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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.participant.StateMachineEngine;

public class FileStore {
  private final String _zkAddr;
  private final String _clusterName;
  private final String _serverId;
  private HelixManager _manager = null;

  public FileStore(String zkAddr, String clusterName, String serverId) {
    _zkAddr = zkAddr;
    _clusterName = clusterName;
    _serverId = serverId;
  }

  public void connect() {
    try {
      _manager =
          HelixManagerFactory.getZKHelixManager(_clusterName, _serverId, InstanceType.PARTICIPANT,
              _zkAddr);

      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      FileStoreStateModelFactory modelFactory = new FileStoreStateModelFactory(_manager);
      stateMach.registerStateModelFactory(StateModelDefId.from(SetupCluster.DEFAULT_STATE_MODEL),
          modelFactory);
      _manager.connect();
      // _manager.addExternalViewChangeListener(replicator);
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      System.err.println(" [-] " + _serverId + " is interrupted ...");
    } catch (Exception e) {
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

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("USAGE: java FileStore zookeeperAddress(e.g. localhost:2181) serverId(host_port)");
      System.exit(1);
    }

    final String zkAddr = args[0];
    final String clusterName = SetupCluster.DEFAULT_CLUSTER_NAME;
    final String serverId = args[1];

    ZkClient zkclient = null;
    try {
      // start consumer
      final FileStore store = new FileStore(zkAddr, clusterName, serverId);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Shutting down server:" + serverId);
          store.disconnect();
        }
      });
      store.connect();
    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }
}
