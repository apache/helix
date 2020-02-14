package org.apache.helix.zookeeper.impl.factory;

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

import java.util.HashMap;

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.impl.client.SharedZkClient;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Singleton factory that build shared ZkClient which use a shared ZkConnection.
 */
public class SharedZkClientFactory extends HelixZkClientFactory {
  private static Logger LOG = LoggerFactory.getLogger(SharedZkClientFactory.class);
  // The connection pool to track all created connections.
  private final HashMap<HelixZkClient.ZkConnectionConfig, ZkConnectionManager>
      _connectionManagerPool = new HashMap<>();

  private final HashMap<RealmAwareZkClient.RealmAwareZkConnectionConfig, ZkConnectionManager>
      _realmConnManagerPool = new HashMap<>();

  protected SharedZkClientFactory() {
  }

  /**
   * Build a Shared RealmAwareZkClient that uses sharing ZkConnection that is created based on the specified connection config.
   *
   * @param connectionConfig The connection configuration that is used to search for a shared connection. Or create new connection if necessary.
   * @param clientConfig
   * @return Shared ZkClient
   */
  @Override
  public RealmAwareZkClient buildZkClient(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig) {
    synchronized (_realmConnManagerPool) {
      String realmKey = connectionConfig.getZkRealmShardingKey();
      final ZkConnectionManager realmConnectionManager =
          getOrCreateZkConnectionManager(connectionConfig, clientConfig.getConnectInitTimeout());
      if (realmConnectionManager == null) {
        throw new ZkClientException("Failed to create a realm connection manager in the pool to share.");
      }
      LOG.info("Sharing ZkConnection {} to a new SharedZkClient.", connectionConfig.toString());
      return new SharedZkClient(realmKey, realmConnectionManager, clientConfig,
          new SharedZkClient.OnCloseCallback() {
        @Override
        public void onClose() {
          cleanupConnectionManager(realmConnectionManager);
        }
      });
    }
  }

  /* rely on default implementation in the RealAwareZkClientFactory interface
  @Override
  public RealmAwareZkClient buildZkClient(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig) {
    // TODO: Implement the logic
    return null;
  }
  */

  private static class SingletonHelper {
    private static final SharedZkClientFactory INSTANCE = new SharedZkClientFactory();
  }

  public static SharedZkClientFactory getInstance() {
    return SingletonHelper.INSTANCE;
  }

  /**
   * Build a Shared ZkClient that uses sharing ZkConnection that is created based on the specified connection config.
   *
   * @param connectionConfig The connection configuration that is used to search for a shared connection. Or create new connection if necessary.
   * @param clientConfig
   * @return Shared ZkClient
   */
  @Override
  public HelixZkClient buildZkClient(HelixZkClient.ZkConnectionConfig connectionConfig,
      HelixZkClient.ZkClientConfig clientConfig) {
    synchronized (_connectionManagerPool) {
      final ZkConnectionManager zkConnectionManager =
          getOrCreateZkConnectionManager(connectionConfig, clientConfig.getConnectInitTimeout());
      if (zkConnectionManager == null) {
        throw new ZkClientException("Failed to create a connection manager in the pool to share.");
      }
      LOG.info("Sharing ZkConnection {} to a new SharedZkClient.", connectionConfig.toString());
      return new SharedZkClient(zkConnectionManager, clientConfig,
          new SharedZkClient.OnCloseCallback() {
            @Override
            public void onClose() {
              cleanupConnectionManager(zkConnectionManager);
            }
          });
    }
  }

  private ZkConnectionManager getOrCreateZkConnectionManager(
      HelixZkClient.ZkConnectionConfig connectionConfig, long connectInitTimeout) {
    ZkConnectionManager connectionManager = _connectionManagerPool.get(connectionConfig);
    if (connectionManager == null || connectionManager.isClosed()) {
      connectionManager =
          new ZkConnectionManager(createZkConnection(connectionConfig), connectInitTimeout,
              connectionConfig.toString());
      _connectionManagerPool.put(connectionConfig, connectionManager);
    }
    return connectionManager;
  }

  private ZkConnectionManager getOrCreateZkConnectionManager(
     RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig, long connectInitTimeout) {
    ZkConnectionManager connectionManager = _realmConnManagerPool.get(connectionConfig);
    if (connectionManager == null || connectionManager.isClosed()) {
      // TODO: call MSDS to query zkserver Address based on connection string;
      // if the query fails, log and return null. The build method would throw appropriate exception.
      String zkServers = null;
      HelixZkClient.ZkConnectionConfig zkConnectionConfig = new HelixZkClient.ZkConnectionConfig(zkServers);
      zkConnectionConfig.setSessionTimeout(connectionConfig.getSessionTimeout());
      connectionManager =
          new ZkConnectionManager(createZkConnection(zkConnectionConfig), connectInitTimeout,
              zkConnectionConfig.toString());
    }
    return connectionManager;
  }

  // Close the ZkConnectionManager if no other shared client is referring to it.
  // Note the close operation of connection manager needs to be synchronized with the pool operation
  // to avoid race condition.
  private void cleanupConnectionManager(ZkConnectionManager zkConnectionManager) {
    synchronized (_connectionManagerPool) {
      zkConnectionManager.close(true);
    }
  }

  // For testing purposes only
  public int getActiveConnectionCount() {
    int count = 0;
    synchronized (_connectionManagerPool) {
      for (ZkConnectionManager manager : _connectionManagerPool.values()) {
        if (!manager.isClosed()) {
          count++;
        }
      }
    }
    return count;
  }
}
