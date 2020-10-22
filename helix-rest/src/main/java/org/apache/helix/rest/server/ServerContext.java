package org.apache.helix.rest.server;


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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.metadatastore.ZkMetadataStoreDirectory;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ByteArraySerializer;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerContext implements IZkDataListener, IZkChildListener, IZkStateListener {
  private static final Logger LOG = LoggerFactory.getLogger(ServerContext.class);

  private final String _zkAddr;
  private boolean _isMultiZkEnabled;
  private final String _msdsEndpoint;
  private volatile RealmAwareZkClient _zkClient;
  private volatile RealmAwareZkClient _byteArrayZkClient;

  private volatile ZKHelixAdmin _zkHelixAdmin;
  private volatile ClusterSetup _clusterSetup;
  private volatile ConfigAccessor _configAccessor;
  // A lazily-initialized base data accessor that reads/writes byte array to ZK
  // TODO: Only read (deserialize) is supported at this time. This baseDataAccessor should support write (serialize) as needs arise
  private volatile ZkBaseDataAccessor<byte[]> _byteArrayZkBaseDataAccessor;
  // 1 Cluster name will correspond to 1 helix data accessor
  private final Map<String, HelixDataAccessor> _helixDataAccessorPool;
  // 1 Cluster name will correspond to 1 task driver
  private final Map<String, TaskDriver> _taskDriverPool;

  /**
   * Multi-ZK support
   */
  private ZkMetadataStoreDirectory _zkMetadataStoreDirectory;
  // Create a dedicated ZkClient for listening to data changes in routing data
  private RealmAwareZkClient _zkClientForRoutingDataListener;

  public ServerContext(String zkAddr) {
    this(zkAddr, false, null);
  }

  /**
   * Initializes a ServerContext for this namespace.
   * @param zkAddr routing ZK address (on multi-zk mode)
   * @param isMultiZkEnabled boolean flag for whether multi-zk mode is enabled
   * @param msdsEndpoint if given, this server context will try to read routing data from this MSDS.
   */
  public ServerContext(String zkAddr, boolean isMultiZkEnabled, String msdsEndpoint) {
    _zkAddr = zkAddr;
    _isMultiZkEnabled = isMultiZkEnabled;
    _msdsEndpoint = msdsEndpoint; // only applicable on multi-zk mode

    // We should NOT initiate _zkClient and anything that depends on _zkClient in
    // constructor, as it is reasonable to start up HelixRestServer first and then
    // ZooKeeper. In this case, initializing _zkClient will fail and HelixRestServer
    // cannot be started correctly.
    _helixDataAccessorPool = new ConcurrentHashMap<>();
    _taskDriverPool = new ConcurrentHashMap<>();

    // Initialize the singleton ZkMetadataStoreDirectory instance to allow it to be closed later
    _zkMetadataStoreDirectory = ZkMetadataStoreDirectory.getInstance();
  }

  /**
   * Lazy initialization of RealmAwareZkClient used throughout the REST server.
   * @return
   */
  public RealmAwareZkClient getRealmAwareZkClient() {
    if (_zkClient == null) {
      synchronized (this) {
        if (_zkClient == null) {
          _zkClient = createRealmAwareZkClient(_zkClient, true, new ZNRecordSerializer());
        }
      }
    }
    return _zkClient;
  }

  /**
   * Returns a RealmAWareZkClient with ByteArraySerializer with double-checked locking.
   * NOTE: this is different from getRealmAwareZkClient in that it does not reset listeners for
   * _zkClientForListener because this RealmAwareZkClient is independent from routing data changes.
   * @return
   */
  public RealmAwareZkClient getByteArrayRealmAwareZkClient() {
    if (_byteArrayZkClient == null) {
      synchronized (this) {
        if (_byteArrayZkClient == null) {
          _byteArrayZkClient =
              createRealmAwareZkClient(_byteArrayZkClient, false, new ByteArraySerializer());
        }
      }
    }
    return _byteArrayZkClient;
  }

  /**
   * Main creation logic for RealmAwareZkClient.
   * @param realmAwareZkClient
   * @param shouldSubscribeToRoutingDataChange if true, it will initialize zk client to listen on
   *                                           routing data change and refresh change subscription
   * @param zkSerializer the type of ZkSerializer to use
   * @return
   */
  private RealmAwareZkClient createRealmAwareZkClient(RealmAwareZkClient realmAwareZkClient,
      boolean shouldSubscribeToRoutingDataChange, ZkSerializer zkSerializer) {
    // If the multi ZK config is enabled, use FederatedZkClient on multi-realm mode
    if (_isMultiZkEnabled || Boolean
        .parseBoolean(System.getProperty(SystemPropertyKeys.MULTI_ZK_ENABLED))) {
      try {
        if (shouldSubscribeToRoutingDataChange) {
          initializeZkClientForRoutingData();
        }
        RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder connectionConfigBuilder =
            new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
        // If MSDS endpoint is set for this namespace, use that instead.
        if (_msdsEndpoint != null && !_msdsEndpoint.isEmpty()) {
          connectionConfigBuilder.setRoutingDataSourceEndpoint(_msdsEndpoint)
              .setRoutingDataSourceType(RoutingDataReaderType.HTTP.name());
        }
        realmAwareZkClient = new FederatedZkClient(connectionConfigBuilder.build(),
            new RealmAwareZkClient.RealmAwareZkClientConfig().setZkSerializer(zkSerializer));
        LOG.info("ServerContext: FederatedZkClient created successfully!");
      } catch (InvalidRoutingDataException | IllegalStateException e) {
        throw new HelixException("Failed to create FederatedZkClient!", e);
      }
    } else {
      // If multi ZK config is not set, just connect to the ZK address given
      HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
      clientConfig.setZkSerializer(zkSerializer);
      realmAwareZkClient = SharedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(_zkAddr), clientConfig);
    }
    return realmAwareZkClient;
  }

  /**
   * Initialization logic for ZkClient for routing data listener.
   * NOTE: The initialization lifecycle of zkClientForRoutingDataListener is tied to the private
   * volatile zkClient.
   */
  private void initializeZkClientForRoutingData() {
    // Make sure the ServerContext is subscribed to routing data change so that it knows
    // when to reset ZkClient and Helix APIs
    if (_zkClientForRoutingDataListener == null) {
      // Routing data is always in the ZNRecord format
      _zkClientForRoutingDataListener = DedicatedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(_zkAddr),
              new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));
    }
    // Refresh data subscription
    _zkClientForRoutingDataListener.unsubscribeAll();
    _zkClientForRoutingDataListener.subscribeRoutingDataChanges(this, this);
    LOG.info("ServerContext: subscribed to routing data in routing ZK at {}!", _zkAddr);
  }

  @Deprecated
  public ZkClient getZkClient() {
    return (ZkClient) getRealmAwareZkClient();
  }

  public HelixAdmin getHelixAdmin() {
    if (_zkHelixAdmin == null) {
      synchronized (this) {
        if (_zkHelixAdmin == null) {
          _zkHelixAdmin = new ZKHelixAdmin(getRealmAwareZkClient());
        }
      }
    }
    return _zkHelixAdmin;
  }

  public ClusterSetup getClusterSetup() {
    if (_clusterSetup == null) {
      synchronized (this) {
        if (_clusterSetup == null) {
          _clusterSetup = new ClusterSetup(getRealmAwareZkClient(), getHelixAdmin());
        }
      }
    }
    return _clusterSetup;
  }

  public TaskDriver getTaskDriver(String clusterName) {
    TaskDriver taskDriver = _taskDriverPool.get(clusterName);
    if (taskDriver == null) {
      synchronized (this) {
        if (!_taskDriverPool.containsKey(clusterName)) {
          _taskDriverPool.put(clusterName, new TaskDriver(getRealmAwareZkClient(), clusterName));
        }
        taskDriver = _taskDriverPool.get(clusterName);
      }
    }
    return taskDriver;
  }

  public ConfigAccessor getConfigAccessor() {
    if (_configAccessor == null) {
      synchronized (this) {
        if (_configAccessor == null) {
          _configAccessor = new ConfigAccessor(getRealmAwareZkClient());
        }
      }
    }
    return _configAccessor;
  }

  public HelixDataAccessor getDataAccessor(String clusterName) {
    HelixDataAccessor dataAccessor = _helixDataAccessorPool.get(clusterName);
    if (dataAccessor == null) {
      synchronized (this) {
        if (!_helixDataAccessorPool.containsKey(clusterName)) {
          ZkBaseDataAccessor<ZNRecord> baseDataAccessor =
              new ZkBaseDataAccessor<>(getRealmAwareZkClient());
          _helixDataAccessorPool.put(clusterName,
              new ZKHelixDataAccessor(clusterName, InstanceType.ADMINISTRATOR, baseDataAccessor));
        }
        dataAccessor = _helixDataAccessorPool.get(clusterName);
      }
    }
    return dataAccessor;
  }

  /**
   * Returns a lazily-instantiated ZkBaseDataAccessor for the byte array type.
   * @return
   */
  public BaseDataAccessor<byte[]> getByteArrayZkBaseDataAccessor() {
    if (_byteArrayZkBaseDataAccessor == null) {
      synchronized (this) {
        if (_byteArrayZkBaseDataAccessor == null) {
          _byteArrayZkBaseDataAccessor = new ZkBaseDataAccessor<>(getByteArrayRealmAwareZkClient());
        }
      }
    }
    return _byteArrayZkBaseDataAccessor;
  }

  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
    }
    if (_zkMetadataStoreDirectory != null) {
      _zkMetadataStoreDirectory.close();
    }
    if (_zkClientForRoutingDataListener != null) {
      _zkClientForRoutingDataListener.close();
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) {
    if (_zkClientForRoutingDataListener == null || _zkClientForRoutingDataListener.isClosed()) {
      return;
    }
    // Resubscribe
    _zkClientForRoutingDataListener.unsubscribeAll();
    _zkClientForRoutingDataListener.subscribeRoutingDataChanges(this, this);
    resetZkResources();
  }

  @Override
  public void handleDataChange(String dataPath, Object data) {
    if (_zkClientForRoutingDataListener == null || _zkClientForRoutingDataListener.isClosed()) {
      return;
    }
    resetZkResources();
  }

  @Override
  public void handleDataDeleted(String dataPath) {
    if (_zkClientForRoutingDataListener == null || _zkClientForRoutingDataListener.isClosed()) {
      return;
    }
    // Resubscribe
    _zkClientForRoutingDataListener.unsubscribeAll();
    _zkClientForRoutingDataListener.subscribeRoutingDataChanges(this, this);
    resetZkResources();
  }

  @Override
  public void handleStateChanged(Watcher.Event.KeeperState state) {
    if (_zkClientForRoutingDataListener == null || _zkClientForRoutingDataListener.isClosed()) {
      return;
    }
    // Resubscribe
    _zkClientForRoutingDataListener.unsubscribeAll();
    _zkClientForRoutingDataListener.subscribeRoutingDataChanges(this, this);
    resetZkResources();
  }

  @Override
  public void handleNewSession(String sessionId) {
    if (_zkClientForRoutingDataListener == null || _zkClientForRoutingDataListener.isClosed()) {
      return;
    }
    // Resubscribe
    _zkClientForRoutingDataListener.unsubscribeAll();
    _zkClientForRoutingDataListener.subscribeRoutingDataChanges(this, this);
    resetZkResources();
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error) {
    if (_zkClientForRoutingDataListener == null || _zkClientForRoutingDataListener.isClosed()) {
      return;
    }
    // Resubscribe
    _zkClientForRoutingDataListener.unsubscribeAll();
    _zkClientForRoutingDataListener.subscribeRoutingDataChanges(this, this);
    resetZkResources();
  }

  /**
   * Resets all internally cached routing data by closing and nullifying the ZkClient and Helix APIs.
   * This is okay because routing data update should be infrequent.
   */
  private void resetZkResources() {
    synchronized (this) {
      LOG.info("ServerContext: Resetting ZK resources due to routing data change! Routing ZK: {}",
          _zkAddr);
      try {
        // Reset RoutingDataManager's cache
        RoutingDataManager.getInstance().reset();
        // All Helix APIs will be closed implicitly because ZkClient is closed
        if (_zkClient != null && !_zkClient.isClosed()) {
          _zkClient.close();
        }
        if (_byteArrayZkBaseDataAccessor != null) {
          _byteArrayZkBaseDataAccessor.close();
        }
        _zkClient = null;
        _zkHelixAdmin = null;
        _clusterSetup = null;
        _configAccessor = null;
        _byteArrayZkBaseDataAccessor = null;
        _helixDataAccessorPool.clear();
        _taskDriverPool.clear();
      } catch (Exception e) {
        LOG.error("Failed to reset ZkClient and Helix APIs in ServerContext!", e);
      }
    }
  }
}
