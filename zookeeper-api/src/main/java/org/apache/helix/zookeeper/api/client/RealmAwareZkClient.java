package org.apache.helix.zookeeper.api.client;

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
import java.util.concurrent.TimeUnit;

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


/**
 * The Realm-aware ZkClient interface.
 * NOTE: "Realm-aware" does not necessarily mean that the RealmAwareZkClient instance will be connecting to multiple ZK realms.
 * On single-realm mode, RealmAwareZkClient will reject requests going out to other ZK realms than the one set at initialization.
 * On multi-realm mode, RealmAwareZkClient will connect to multiple ZK realms but will reject EPHEMERAL AccessMode operations.
 */
public interface RealmAwareZkClient {

  /**
   * Specifies which mode to run this RealmAwareZkClient on.
   *
   * SINGLE_REALM: CRUD, change subscription, and EPHEMERAL CreateMode are supported.
   * MULTI_REALM: CRUD and change subscription are supported. Operations involving EPHEMERAL CreateMode will throw an UnsupportedOperationException.
   */
  enum RealmMode {
    SINGLE_REALM, MULTI_REALM
  }

  int DEFAULT_OPERATION_TIMEOUT = Integer.MAX_VALUE;
  int DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;
  int DEFAULT_SESSION_TIMEOUT = 30 * 1000;

  // listener subscription

  /**
   * Subscribe the path and the listener will handle child events of the path.
   * Add exists watch to path if the path does not exist in ZooKeeper server.
   * WARNING: if the path is created after deletion, users need to re-subscribe the path
   * @param path The zookeeper path
   * @param listener Instance of {@link IZkDataListener}
   * The method return null if the path does not exists. Otherwise, return a list of children
   * under the path. The list can be empty if there is no children.
   */
  @Deprecated
  List<String> subscribeChildChanges(String path, IZkChildListener listener);

  /**
   * Subscribe the path and the listener will handle child events of the path
   * WARNING: if the path is created after deletion, users need to re-subscribe the path
   * @param path The zookeeper path
   * @param listener Instance of {@link IZkDataListener}
   * @param skipWatchingNonExistNode True means not installing any watch if path does not exist.
   * @return ChildrentSubsribeResult. If the path does not exists, the isInstalled field
   * is false. Otherwise, it is true and list of children are returned.
   */
  ChildrenSubscribeResult subscribeChildChanges(String path, IZkChildListener listener,
      boolean skipWatchingNonExistNode);

  void unsubscribeChildChanges(String path, IZkChildListener listener);

  /**
   * Subscribe the path and the listener will handle data events of the path
   * Add the exists watch to Zookeeper server even if the path does not exists in zookeeper server
   * WARNING: if the path is created after deletion, users need to re-subscribe the path
   * @param path The zookeeper path
   * @param listener Instance of {@link IZkDataListener}
   */
  @Deprecated
  void subscribeDataChanges(String path, IZkDataListener listener);

  /**
   * Subscribe the path and the listener will handle data events of the path
   * WARNING: if the path is created after deletion, users need to re-subscribe the path
   * @param path The zookeeper path
   * @param listener Instance of {@link IZkDataListener}
   * @param skipWatchingNonExistNode True means not installing any watch if path does not exist.
   * return True if installation of watch succeed. Otherwise, return false.
   */
  boolean subscribeDataChanges(String path, IZkDataListener listener,
      boolean skipWatchingNonExistNode);

  void unsubscribeDataChanges(String path, IZkDataListener listener);

  /*
   * This is for backwards compatibility.
   *
   * TODO: remove below default implementation when getting rid of I0Itec in the new zk client.
   */
  default void subscribeStateChanges(final IZkStateListener listener) {
    subscribeStateChanges(new I0ItecIZkStateListenerImpl(listener));
  }

  /*
   * This is for backwards compatibility.
   *
   * TODO: remove below default implementation when getting rid of I0Itec in the new zk client.
   */
  default void unsubscribeStateChanges(IZkStateListener listener) {
    unsubscribeStateChanges(new I0ItecIZkStateListenerImpl(listener));
  }

  /**
   * Subscribes state changes for a
   * {@link org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener} listener.
   * @deprecated
   *             This is deprecated. It is kept for backwards compatibility. Please use
   *             {@link #subscribeStateChanges(IZkStateListener)}.
   * @param listener {@link org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener}
   *          listener
   */
  @Deprecated
  void subscribeStateChanges(
      final org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener listener);

  /**
   * Unsubscribes state changes for a
   * {@link org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener} listener.
   * @deprecated
   *             This is deprecated. It is kept for backwards compatibility. Please use
   *             {@link #unsubscribeStateChanges(IZkStateListener)}.
   * @param listener {@link org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener}
   *          listener
   */
  @Deprecated
  void unsubscribeStateChanges(
      org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener listener);

  void unsubscribeAll();

  // data access
  void createPersistent(String path);

  void createPersistent(String path, boolean createParents);

  void createPersistent(String path, boolean createParents, List<ACL> acl);

  void createPersistent(String path, Object data);

  void createPersistent(String path, Object data, List<ACL> acl);

  String createPersistentSequential(String path, Object data);

  String createPersistentSequential(String path, Object data, List<ACL> acl);

  void createEphemeral(final String path);

  void createEphemeral(final String path, final String sessionId);

  void createEphemeral(final String path, final List<ACL> acl);

  void createEphemeral(final String path, final List<ACL> acl, final String sessionId);

  String create(final String path, Object data, final CreateMode mode);

  String create(final String path, Object datat, final List<ACL> acl, final CreateMode mode);

  void createEphemeral(final String path, final Object data);

  void createEphemeral(final String path, final Object data, final String sessionId);

  void createEphemeral(final String path, final Object data, final List<ACL> acl);

  void createEphemeral(final String path, final Object data, final List<ACL> acl,
      final String sessionId);

  String createEphemeralSequential(final String path, final Object data);

  String createEphemeralSequential(final String path, final Object data, final List<ACL> acl);

  String createEphemeralSequential(final String path, final Object data, final String sessionId);

  String createEphemeralSequential(final String path, final Object data, final List<ACL> acl,
      final String sessionId);

  List<String> getChildren(String path);

  int countChildren(String path);

  boolean exists(final String path);

  Stat getStat(final String path);

  boolean waitUntilExists(String path, TimeUnit timeUnit, long time);

  void deleteRecursively(String path);

  boolean delete(final String path);

  <T extends Object> T readData(String path);

  <T extends Object> T readData(String path, boolean returnNullIfPathNotExists);

  <T extends Object> T readData(String path, Stat stat);

  <T extends Object> T readData(final String path, final Stat stat, final boolean watch);

  <T extends Object> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists);

  void writeData(String path, Object object);

  <T extends Object> void updateDataSerialized(String path, DataUpdater<T> updater);

  void writeData(final String path, Object datat, final int expectedVersion);

  Stat writeDataReturnStat(final String path, Object datat, final int expectedVersion);

  Stat writeDataGetStat(final String path, Object datat, final int expectedVersion);

  void asyncCreate(final String path, Object datat, final CreateMode mode,
      final ZkAsyncCallbacks.CreateCallbackHandler cb);

  void asyncSetData(final String path, Object datat, final int version,
      final ZkAsyncCallbacks.SetDataCallbackHandler cb);

  void asyncGetData(final String path, final ZkAsyncCallbacks.GetDataCallbackHandler cb);

  void asyncExists(final String path, final ZkAsyncCallbacks.ExistsCallbackHandler cb);

  void asyncDelete(final String path, final ZkAsyncCallbacks.DeleteCallbackHandler cb);

  void watchForData(final String path);

  List<String> watchForChilds(final String path);

  long getCreationTime(String path);

  @Deprecated
  List<OpResult> multi(final Iterable<Op> ops);

  List<OpResult> multiOps(final List<MultiOp> ops);

  // ZK state control
  boolean waitUntilConnected(long time, TimeUnit timeUnit);

  /**
   * Waits for SyncConnected state and returns a valid session ID(non-zero). The implementation of
   * this method should wait for SyncConnected state and ZK session to be established, and should
   * guarantee the established session's ID is returned before keeper state changes.
   *
   * Please note: this default implementation may have race condition issue and return an unexpected
   * session ID that is zero or another new session's ID. The default implementation is for backward
   * compatibility purpose.
   *
   * @param timeout Max waiting time for connecting to ZK server.
   * @param timeUnit Time unit for the timeout.
   * @return A valid ZK session ID which is non-zero.
   */
  default long waitForEstablishedSession(long timeout, TimeUnit timeUnit) {
    if (!waitUntilConnected(timeout, timeUnit)) {
      throw new ZkTimeoutException(
          "Failed to get established session because connecting to ZK server has timed out in "
              + timeout + " " + timeUnit);
    }
    return getSessionId();
  }

  String getServers();

  long getSessionId();

  void close();

  boolean isClosed();

  // other
  byte[] serialize(Object data, String path);

  <T extends Object> T deserialize(byte[] data, String path);

  void setZkSerializer(ZkSerializer zkSerializer);

  void setZkSerializer(PathBasedZkSerializer zkSerializer);

  PathBasedZkSerializer getZkSerializer();

  default RealmAwareZkConnectionConfig getRealmAwareZkConnectionConfig() {
    throw new UnsupportedOperationException("getRealmAwareZkClientConfig() is not supported!");
  }

  default RealmAwareZkClientConfig getRealmAwareZkClientConfig() {
    throw new UnsupportedOperationException("getRealmAwareZkClientConfig() is not supported!");
  }

  /**
   * A class that wraps a default implementation of
   * {@link IZkStateListener}, which means this listener
   * runs the methods of {@link IZkStateListener}.
   * This is for backward compatibility and to avoid breaking the original implementation of
   * {@link org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener}.
   */
  class I0ItecIZkStateListenerImpl implements org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener {
    private IZkStateListener _listener;

    I0ItecIZkStateListenerImpl(IZkStateListener listener) {
      _listener = listener;
    }

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
      _listener.handleStateChanged(keeperState);
    }

    @Override
    public void handleNewSession() throws Exception {
      /*
       * org.apache.helix.manager.zk.zookeeper.IZkStateListener does not have handleNewSession(),
       * so null is passed into handleNewSession(sessionId).
       */
      _listener.handleNewSession(null);
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) throws Exception {
      _listener.handleSessionEstablishmentError(error);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof I0ItecIZkStateListenerImpl)) {
        return false;
      }
      if (_listener == null) {
        return false;
      }

      I0ItecIZkStateListenerImpl defaultListener = (I0ItecIZkStateListenerImpl) obj;

      return _listener.equals(defaultListener._listener);
    }

    @Override
    public int hashCode() {
      /*
       * The original listener's hashcode helps find the wrapped listener with the same original
       * listener. This is helpful in unsubscribeStateChanges(listener).
       */
      return _listener.hashCode();
    }
  }

  /**
   * ZkConnection-related configs for creating an instance of RealmAwareZkClient.
   */
  class RealmAwareZkConnectionConfig {
    /**
     * zkRealmShardingKey: used to deduce which ZK realm this RealmAwareZkClientConfig should connect to.
     * NOTE: this field will be ignored if RealmMode is MULTI_REALM!
     */
    private String _zkRealmShardingKey;
    private String _routingDataSourceType;
    private String _routingDataSourceEndpoint;
    private int _sessionTimeout = DEFAULT_SESSION_TIMEOUT;

    private RealmAwareZkConnectionConfig(Builder builder) {
      _zkRealmShardingKey = builder._zkRealmShardingKey;
      _routingDataSourceType = builder._routingDataSourceType;
      _routingDataSourceEndpoint = builder._routingDataSourceEndpoint;
      _sessionTimeout = builder._sessionTimeout;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof RealmAwareZkConnectionConfig)) {
        return false;
      }
      RealmAwareZkConnectionConfig configObj = (RealmAwareZkConnectionConfig) obj;
      return (_zkRealmShardingKey == null && configObj._zkRealmShardingKey == null
          || _zkRealmShardingKey != null && _zkRealmShardingKey
          .equals(configObj._zkRealmShardingKey)) && _sessionTimeout == configObj._sessionTimeout;
    }

    @Override
    public int hashCode() {
      return _sessionTimeout * 31 + _zkRealmShardingKey.hashCode();
    }

    @Override
    public String toString() {
      return (_zkRealmShardingKey + "_" + _sessionTimeout).replaceAll("[\\W]", "_");
    }

    public RealmAwareZkConnectionConfig setSessionTimeout(int sessionTimeout) {
      this._sessionTimeout = sessionTimeout;
      return this;
    }

    public String getZkRealmShardingKey() {
      return _zkRealmShardingKey;
    }

    public int getSessionTimeout() {
      return _sessionTimeout;
    }

    public String getRoutingDataSourceType() {
      return _routingDataSourceType;
    }

    public String getRoutingDataSourceEndpoint() {
      return _routingDataSourceEndpoint;
    }

    public static class Builder {
      private RealmMode _realmMode;
      private String _zkRealmShardingKey;
      private String _routingDataSourceType;
      private String _routingDataSourceEndpoint;
      private int _sessionTimeout = DEFAULT_SESSION_TIMEOUT;

      public Builder() {
      }

      public Builder setRealmMode(RealmMode mode) {
        _realmMode = mode;
        return this;
      }

      /**
       * Note: this must be a valid ZK path. For example, if you are trying to create a sharding key
       * out of a cluster name "CLUSTER", then your sharding key should be "/CLUSTER".
       * @param shardingKey
       * @return
       */
      public Builder setZkRealmShardingKey(String shardingKey) {
        _zkRealmShardingKey = shardingKey;
        return this;
      }

      public Builder setRoutingDataSourceType(String routingDataSourceType) {
        _routingDataSourceType = routingDataSourceType;
        return this;
      }

      public Builder setRoutingDataSourceEndpoint(String routingDataSourceEndpoint) {
        _routingDataSourceEndpoint = routingDataSourceEndpoint;
        return this;
      }

      public Builder setSessionTimeout(int sessionTimeout) {
        _sessionTimeout = sessionTimeout;
        return this;
      }

      public RealmAwareZkConnectionConfig build() {
        validate();
        return new RealmAwareZkConnectionConfig(this);
      }

      /**
       * Validate the internal fields of the builder before creating an instance.
       */
      private void validate() {
        boolean isShardingKeySet = _zkRealmShardingKey != null && !_zkRealmShardingKey.isEmpty();
        if (_realmMode == RealmMode.MULTI_REALM && isShardingKeySet) {
          throw new IllegalArgumentException(
              "RealmAwareZkConnectionConfig.Builder: ZK sharding key cannot be set on multi-realm mode! Sharding key: "
                  + _zkRealmShardingKey);
        }
        if (_realmMode == RealmMode.SINGLE_REALM && !isShardingKeySet) {
          throw new IllegalArgumentException(
              "RealmAwareZkConnectionConfig.Builder: ZK sharding key must be set on single-realm mode!");
        }
        if ((_routingDataSourceEndpoint == null && _routingDataSourceType != null) || (
            _routingDataSourceEndpoint != null && _routingDataSourceType == null)) {
          // For routing data source type and endpoint, if one is set and not the other, it is invalid
          throw new IllegalArgumentException(
              "RealmAwareZkConnectionConfig.Builder: routing data source type and endpoint are not configured properly! Type: "
                  + _routingDataSourceType + " Endpoint: " + _routingDataSourceEndpoint);
        }
      }
    }
  }

  /**
   * ZkClient-related configs for creating an instance of RealmAwareZkClient.
   */
  class RealmAwareZkClientConfig {
    // For client to init the connection
    protected long _connectInitTimeout = DEFAULT_CONNECTION_TIMEOUT;

    // Data access configs
    protected long _operationRetryTimeout = DEFAULT_OPERATION_TIMEOUT;

    // Serializer
    protected PathBasedZkSerializer _zkSerializer;

    // Monitoring
    protected String _monitorType;
    protected String _monitorKey;
    protected String _monitorInstanceName = null;
    protected boolean _monitorRootPathOnly = true;

    public RealmAwareZkClientConfig setZkSerializer(PathBasedZkSerializer zkSerializer) {
      this._zkSerializer = zkSerializer;
      return this;
    }

    public RealmAwareZkClientConfig setZkSerializer(ZkSerializer zkSerializer) {
      this._zkSerializer = new BasicZkSerializer(zkSerializer);
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     *
     * @param monitorType
     */
    public RealmAwareZkClientConfig setMonitorType(String monitorType) {
      this._monitorType = monitorType;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     *
     * @param monitorKey
     */
    public RealmAwareZkClientConfig setMonitorKey(String monitorKey) {
      this._monitorKey = monitorKey;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is optional.
     *
     * @param instanceName
     */
    public RealmAwareZkClientConfig setMonitorInstanceName(String instanceName) {
      this._monitorInstanceName = instanceName;
      return this;
    }

    public RealmAwareZkClientConfig setMonitorRootPathOnly(Boolean monitorRootPathOnly) {
      this._monitorRootPathOnly = monitorRootPathOnly;
      return this;
    }

    public RealmAwareZkClientConfig setOperationRetryTimeout(Long operationRetryTimeout) {
      this._operationRetryTimeout = operationRetryTimeout;
      return this;
    }

    public RealmAwareZkClientConfig setConnectInitTimeout(long _connectInitTimeout) {
      this._connectInitTimeout = _connectInitTimeout;
      return this;
    }

    public PathBasedZkSerializer getZkSerializer() {
      if (_zkSerializer == null) {
        _zkSerializer = new BasicZkSerializer(new SerializableSerializer());
      }
      return _zkSerializer;
    }

    public long getOperationRetryTimeout() {
      return _operationRetryTimeout;
    }

    public String getMonitorType() {
      return _monitorType;
    }

    public String getMonitorKey() {
      return _monitorKey;
    }

    public String getMonitorInstanceName() {
      return _monitorInstanceName;
    }

    public boolean isMonitorRootPathOnly() {
      return _monitorRootPathOnly;
    }

    public long getConnectInitTimeout() {
      return _connectInitTimeout;
    }

    /**
     * Create HelixZkClient.ZkClientConfig based on RealmAwareZkClientConfig.
     * @return
     */
    public HelixZkClient.ZkClientConfig createHelixZkClientConfig() {
      return new HelixZkClient.ZkClientConfig().setZkSerializer(_zkSerializer)
          .setMonitorType(_monitorType).setMonitorKey(_monitorKey)
          .setMonitorInstanceName(_monitorInstanceName).setMonitorRootPathOnly(_monitorRootPathOnly)
          .setOperationRetryTimeout(_operationRetryTimeout)
          .setConnectInitTimeout(_connectInitTimeout);
    }
  }

  /**
   * Subscribes to the routing data paths using the provided ZkClient.
   * Note: this method assumes that the routing data path has already been created.
   * @param childListener
   * @param dataListener
   */
  default void subscribeRoutingDataChanges(IZkChildListener childListener,
      IZkDataListener dataListener) {
    subscribeChildChanges(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, childListener);
    for (String child : getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
      subscribeDataChanges(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + child,
          dataListener);
    }
  }

  /**
   * Based on the RealmAwareZkConnectionConfig given, return MetadataStoreRoutingData.
   * @param connectionConfig
   * @return
   */
  static MetadataStoreRoutingData getMetadataStoreRoutingData(
      RealmAwareZkConnectionConfig connectionConfig) throws InvalidRoutingDataException {
    String routingDataSourceEndpoint = connectionConfig.getRoutingDataSourceEndpoint();
    if (routingDataSourceEndpoint == null || routingDataSourceEndpoint.isEmpty()) {
      // If endpoint is not given explicitly, use HTTP and the endpoint set in System Properties
      return RoutingDataManager.getInstance().getMetadataStoreRoutingData();
    } else {
      return RoutingDataManager.getInstance().getMetadataStoreRoutingData(
          RoutingDataReaderType.lookUp(connectionConfig.getRoutingDataSourceType()),
          routingDataSourceEndpoint);
    }
  }
}
