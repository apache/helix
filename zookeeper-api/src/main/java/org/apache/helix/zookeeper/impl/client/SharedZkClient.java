package org.apache.helix.zookeeper.impl.client;

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

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.factory.ZkConnectionManager;
import org.apache.helix.zookeeper.zkclient.IZkConnection;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * NOTE: DO NOT USE THIS CLASS DIRECTLY. USE SharedZkClientFactory instead.
 *
 * HelixZkClient that uses shared ZkConnection.
 * A SharedZkClient won't manipulate the shared ZkConnection directly.
 */
public class SharedZkClient extends ZkClient implements HelixZkClient {
  private static Logger LOG = LoggerFactory.getLogger(SharedZkClient.class);
  /*
   * Since we cannot really disconnect the ZkConnection, we need a dummy ZkConnection placeholder.
   * This is to ensure connection field is never null even the shared RealmAwareZkClient instance is closed so as to avoid NPE.
   */
  private final static ZkConnection IDLE_CONNECTION = new ZkConnection("Dummy_ZkServers");
  private final OnCloseCallback _onCloseCallback;
  private final ZkConnectionManager _connectionManager;

  public interface OnCloseCallback {
    /**
     * Triggered after the RealmAwareZkClient is closed.
     */
    void onClose();
  }

  /**
   * Construct a shared ZkClient that uses a shared ZkConnection.
   *
   * @param connectionManager     The manager of the shared ZkConnection.
   * @param clientConfig          ZkClientConfig details to create the shared RealmAwareZkClient.
   * @param callback              Clean up logic when the shared RealmAwareZkClient is closed.
   */
  public SharedZkClient(ZkConnectionManager connectionManager, ZkClientConfig clientConfig,
      OnCloseCallback callback) {
    super(connectionManager.getConnection(), 0, clientConfig.getOperationRetryTimeout(),
        clientConfig.getZkSerializer(), clientConfig.getMonitorType(), clientConfig.getMonitorKey(),
        clientConfig.getMonitorInstanceName(), clientConfig.isMonitorRootPathOnly());
    _connectionManager = connectionManager;
    // Register to the base dedicated RealmAwareZkClient
    _connectionManager.registerWatcher(this);
    _onCloseCallback = callback;
  }

  /**
   * Construct a shared RealmAwareZkClient that uses a shared ZkConnection.
   *
   * @param connectionManager     The manager of the shared ZkConnection.
   * @param clientConfig          ZkClientConfig details to create the shared RealmAwareZkClient.
   * @param callback              Clean up logic when the shared RealmAwareZkClient is closed.
   */
  public SharedZkClient(String realmKey, ZkConnectionManager connectionManager, RealmAwareZkClientConfig clientConfig,
      OnCloseCallback callback) {
    // todo: assert realmKey != null?
    super(realmKey,connectionManager.getConnection(), 0, clientConfig.getOperationRetryTimeout(),
        clientConfig.getZkSerializer(), clientConfig.getMonitorType(), clientConfig.getMonitorKey(),
        clientConfig.getMonitorInstanceName(), clientConfig.isMonitorRootPathOnly());
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
   * Since ZkConnection session is shared in this RealmAwareZkClient, do not create ephemeral node using a SharedZKClient.
   */
  @Override
  public String create(final String path, Object datat, final List<ACL> acl,
      final CreateMode mode) {
    if (mode.isEphemeral()) {
      throw new UnsupportedOperationException(
          "Create ephemeral nodes using a " + SharedZkClient.class.getSimpleName()
              + " is not supported.");
    }
    return super.create(path, datat, acl, mode);
  }

  @Override
  protected boolean isManagingZkConnection() {
    return false;
  }
}
