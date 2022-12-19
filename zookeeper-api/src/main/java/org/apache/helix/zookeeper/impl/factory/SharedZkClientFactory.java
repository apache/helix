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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.impl.client.SharedZkClient;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkConnection;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
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

  /*
   * Since we cannot really disconnect the ZkConnection, we need a dummy ZkConnection placeholder.
   * This is to ensure connection field is never null even the shared RealmAwareZkClient instance is closed so as to avoid NPE.
   */
  private final static ZkConnection IDLE_CONNECTION = new ZkConnection("Dummy_ZkServers");

  protected SharedZkClientFactory() {
  }

  @Override
  public RealmAwareZkClient buildZkClient(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig)
      throws InvalidRoutingDataException {
    // Note, the logic sharing connectionManager logic is inside SharedZkClient, similar to innerSharedZkClient.
    return new SharedZkClient(connectionConfig, clientConfig);
  }

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
      LOG.info("Sharing ZkConnection {} to a new InnerSharedZkClient.",
          connectionConfig.toString());
      return new InnerSharedZkClient(zkConnectionManager, clientConfig, new OnCloseCallback() {
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

  // Close the ZkConnectionManager if no other shared client is referring to it.
  // Note the close operation of connection manager needs to be synchronized with the pool operation
  // to avoid race condition.
  private void cleanupConnectionManager(ZkConnectionManager zkConnectionManager) {
    synchronized (_connectionManagerPool) {
      zkConnectionManager.close(true);
    }
  }

  // For testing purposes only
  @VisibleForTesting
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

  public interface OnCloseCallback {
    /**
     * Triggered after the SharedZkClient is closed.
     */
    void onClose();
  }

  /**
   * NOTE: do NOT use this class directly. Please use SharedZkClientFactory to create an instance of SharedZkClient.
   * InnerSharedZkClient is a ZkClient used by SharedZkClient to power ZK operations against a single ZK realm.
   *
   * NOTE2: current InnerSharedZkClient replace the original SharedZKClient. We intend to keep the behavior of original
   * SharedZkClient intact. (Think of rename the original SharedZkClient as InnerSharedZkClient. This would maintain
   * backward compatibility.
   */
  public static class InnerSharedZkClient extends ZkClient implements HelixZkClient {

    private final OnCloseCallback _onCloseCallback;
    private final ZkConnectionManager _connectionManager;

    public InnerSharedZkClient(ZkConnectionManager connectionManager, ZkClientConfig clientConfig,
        OnCloseCallback callback) {
      super(connectionManager.getConnection(), 0, clientConfig.getOperationRetryTimeout(),
          clientConfig.getZkSerializer(), clientConfig.getMonitorType(),
          clientConfig.getMonitorKey(), clientConfig.getMonitorInstanceName(),
          clientConfig.isMonitorRootPathOnly());
      _connectionManager = connectionManager;
      // Register to the base dedicated RealmAwareZkClient
      _connectionManager.registerWatcher(this);
      _onCloseCallback = callback;
    }

    @Override
    public void close() {
      super.close();
      if (isClosed()) {
        // Note that if register is not done while constructing, these private fields may not be init yet.
        if (_connectionManager != null) {
          _connectionManager.unregisterWatcher(this);
        }
        if (_onCloseCallback != null) {
          _onCloseCallback.onClose();
        }
      }
    }

    @Override
    public IZkConnection getConnection() {
      if (isClosed()) {
        return IDLE_CONNECTION;
      }
      return super.getConnection();
    }

    /**
     * Since ZkConnection session is shared in this HelixZkClient, do not create ephemeral node using a SharedZKClient.
     */
    @Override
    public String create(final String path, Object datat, final List<ACL> acl,
        final CreateMode mode) {
      return create(path, datat, acl, mode, TTL_NOT_SET);
    }
    @Override
    public String create(final String path, Object datat, final List<ACL> acl,
        final CreateMode mode, long ttl) {
      if (mode.isEphemeral()) {
        throw new UnsupportedOperationException(
            "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
                + " is not supported.");
      }
      return super.create(path, datat, acl, mode, ttl);
    }

    @Override
    protected boolean isManagingZkConnection() {
      return false;
    }
  }
}
