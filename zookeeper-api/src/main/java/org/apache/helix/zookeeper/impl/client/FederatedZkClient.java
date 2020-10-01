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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.ChildrenSubscribeResult;
import org.apache.helix.zookeeper.api.client.MultiOp;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.RoutingDataConstants;
import org.apache.helix.zookeeper.constant.RoutingSystemPropertyKeys;
import org.apache.helix.zookeeper.exception.MultiZkException;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements and supports all ZK operations defined in interface {@link RealmAwareZkClient},
 * except for session-aware operations such as creating ephemeral nodes, for which
 * an {@link UnsupportedOperationException} will be thrown.
 * <p>
 * It acts as a single ZK client but will automatically route read/write/change subscription
 * requests to the corresponding ZkClient with the help of metadata store directory service.
 * It could connect to multiple ZK addresses and maintain a {@link ZkClient} for each ZK address.
 * <p>
 * Note: each Zk realm has its own event queue to handle listeners. So listeners from different ZK
 * realms could be handled concurrently because listeners of a ZK realm are handled in its own
 * queue. The concurrency of listeners should be aware of when implementing listeners for different
 * ZK realms. The users should use thread-safe data structures if they wish to handle change
 * callbacks.
 */
public class FederatedZkClient implements RealmAwareZkClient {
  private static final Logger LOG = LoggerFactory.getLogger(FederatedZkClient.class);

  private static final String FEDERATED_ZK_CLIENT = FederatedZkClient.class.getSimpleName();
  private static final String DEDICATED_ZK_CLIENT_FACTORY =
      DedicatedZkClientFactory.class.getSimpleName();

  private volatile MetadataStoreRoutingData _metadataStoreRoutingData;
  private final RealmAwareZkClient.RealmAwareZkConnectionConfig _connectionConfig;
  private final RealmAwareZkClient.RealmAwareZkClientConfig _clientConfig;

  // ZK realm -> ZkClient
  private final Map<String, ZkClient> _zkRealmToZkClientMap;

  private volatile boolean _isClosed;
  private PathBasedZkSerializer _pathBasedZkSerializer;
  private final boolean _routingDataUpdateOnCacheMissEnabled = Boolean.parseBoolean(
      System.getProperty(RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS));
  private long _routingDataUpdateInterval;

  // TODO: support capacity of ZkClient number in one FederatedZkClient and do garbage collection.
  public FederatedZkClient(RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig) throws InvalidRoutingDataException {
    if (connectionConfig == null) {
      throw new IllegalArgumentException("RealmAwareZkConnectionConfig cannot be null!");
    }
    if (clientConfig == null) {
      throw new IllegalArgumentException("RealmAwareZkClientConfig cannot be null!");
    }
    _metadataStoreRoutingData = RealmAwareZkClient.getMetadataStoreRoutingData(connectionConfig);
    _isClosed = false;
    _connectionConfig = connectionConfig;
    _clientConfig = clientConfig;
    _pathBasedZkSerializer = clientConfig.getZkSerializer();
    _zkRealmToZkClientMap = new ConcurrentHashMap<>();
    getRoutingDataUpdateInterval();
  }

  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    return getZkClient(path).subscribeChildChanges(path, listener);
  }

  @Override
  public ChildrenSubscribeResult subscribeChildChanges(String path, IZkChildListener listener,
      boolean skipWatchingNodeNotExist) {
    return getZkClient(path).subscribeChildChanges(path, listener, skipWatchingNodeNotExist);
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener) {
    getZkClient(path).unsubscribeChildChanges(path, listener);
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    getZkClient(path).subscribeDataChanges(path, listener);
  }

  @Override
  public boolean subscribeDataChanges(String path, IZkDataListener listener,
      boolean skipWatchingNodeNotExist) {
    return getZkClient(path).subscribeDataChanges(path, listener, skipWatchingNodeNotExist);
  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener) {
    getZkClient(path).unsubscribeDataChanges(path, listener);
  }

  @Override
  public void subscribeStateChanges(IZkStateListener listener) {
    throwUnsupportedOperationException();
  }

  @Override
  public void unsubscribeStateChanges(IZkStateListener listener) {
    throwUnsupportedOperationException();
  }

  @Override
  public void subscribeStateChanges(
      org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener listener) {
    throwUnsupportedOperationException();
  }

  @Override
  public void unsubscribeStateChanges(
      org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener listener) {
    throwUnsupportedOperationException();
  }

  @Override
  public void unsubscribeAll() {
    _zkRealmToZkClientMap.values().forEach(ZkClient::unsubscribeAll);
  }

  @Override
  public void createPersistent(String path) {
    createPersistent(path, false);
  }

  @Override
  public void createPersistent(String path, boolean createParents) {
    createPersistent(path, createParents, ZooDefs.Ids.OPEN_ACL_UNSAFE);
  }

  @Override
  public void createPersistent(String path, boolean createParents, List<ACL> acl) {
    getZkClient(path).createPersistent(path, createParents, acl);
  }

  @Override
  public void createPersistent(String path, Object data) {
    create(path, data, CreateMode.PERSISTENT);
  }

  @Override
  public void createPersistent(String path, Object data, List<ACL> acl) {
    create(path, data, acl, CreateMode.PERSISTENT);
  }

  @Override
  public String createPersistentSequential(String path, Object data) {
    return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  @Override
  public String createPersistentSequential(String path, Object data, List<ACL> acl) {
    return create(path, data, acl, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  @Override
  public void createEphemeral(String path) {
    create(path, null, CreateMode.EPHEMERAL);
  }

  @Override
  public void createEphemeral(String path, String sessionId) {
    createEphemeral(path, null, sessionId);
  }

  @Override
  public void createEphemeral(String path, List<ACL> acl) {
    create(path, null, acl, CreateMode.EPHEMERAL);
  }

  @Override
  public void createEphemeral(String path, List<ACL> acl, String sessionId) {
    create(path, null, acl, CreateMode.EPHEMERAL, sessionId);
  }

  @Override
  public String create(String path, Object data, CreateMode mode) {
    return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
  }

  @Override
  public String create(String path, Object data, List<ACL> acl, CreateMode mode) {
    return create(path, data, acl, mode, null);
  }

  @Override
  public void createEphemeral(String path, Object data) {
    create(path, data, CreateMode.EPHEMERAL);
  }

  @Override
  public void createEphemeral(String path, Object data, String sessionId) {
    create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, sessionId);
  }

  @Override
  public void createEphemeral(String path, Object data, List<ACL> acl) {
    create(path, data, acl, CreateMode.EPHEMERAL);
  }

  @Override
  public void createEphemeral(String path, Object data, List<ACL> acl, String sessionId) {
    create(path, data, acl, CreateMode.EPHEMERAL, sessionId);
  }

  @Override
  public String createEphemeralSequential(String path, Object data) {
    return create(path, data, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  @Override
  public String createEphemeralSequential(String path, Object data, List<ACL> acl) {
    return create(path, data, acl, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  @Override
  public String createEphemeralSequential(String path, Object data, String sessionId) {
    return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,
        sessionId);
  }

  @Override
  public String createEphemeralSequential(String path, Object data, List<ACL> acl,
      String sessionId) {
    return create(path, data, acl, CreateMode.EPHEMERAL_SEQUENTIAL, sessionId);
  }

  @Override
  public List<String> getChildren(String path) {
    return getZkClient(path).getChildren(path);
  }

  @Override
  public int countChildren(String path) {
    return getZkClient(path).countChildren(path);
  }

  @Override
  public boolean exists(String path) {
    return getZkClient(path).exists(path);
  }

  @Override
  public Stat getStat(String path) {
    return getZkClient(path).getStat(path);
  }

  @Override
  public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) {
    return getZkClient(path).waitUntilExists(path, timeUnit, time);
  }

  @Override
  public void deleteRecursively(String path) {
    getZkClient(path).deleteRecursively(path);
  }

  @Override
  public boolean delete(String path) {
    return getZkClient(path).delete(path);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T readData(String path) {
    return (T) readData(path, false);
  }

  @Override
  public <T> T readData(String path, boolean returnNullIfPathNotExists) {
    return getZkClient(path).readData(path, returnNullIfPathNotExists);
  }

  @Override
  public <T> T readData(String path, Stat stat) {
    return getZkClient(path).readData(path, stat);
  }

  @Override
  public <T> T readData(String path, Stat stat, boolean watch) {
    return getZkClient(path).readData(path, stat, watch);
  }

  @Override
  public <T> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists) {
    return getZkClient(path).readData(path, stat, returnNullIfPathNotExists);
  }

  @Override
  public void writeData(String path, Object object) {
    writeData(path, object, -1);
  }

  @Override
  public <T> void updateDataSerialized(String path, DataUpdater<T> updater) {
    getZkClient(path).updateDataSerialized(path, updater);
  }

  @Override
  public void writeData(String path, Object data, int expectedVersion) {
    writeDataReturnStat(path, data, expectedVersion);
  }

  @Override
  public Stat writeDataReturnStat(String path, Object data, int expectedVersion) {
    return getZkClient(path).writeDataReturnStat(path, data, expectedVersion);
  }

  @Override
  public Stat writeDataGetStat(String path, Object data, int expectedVersion) {
    return writeDataReturnStat(path, data, expectedVersion);
  }

  @Override
  public void asyncCreate(String path, Object data, CreateMode mode,
      ZkAsyncCallbacks.CreateCallbackHandler cb) {
    getZkClient(path).asyncCreate(path, data, mode, cb);
  }

  @Override
  public void asyncSetData(String path, Object data, int version,
      ZkAsyncCallbacks.SetDataCallbackHandler cb) {
    getZkClient(path).asyncSetData(path, data, version, cb);
  }

  @Override
  public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
    getZkClient(path).asyncGetData(path, cb);
  }

  @Override
  public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {
    getZkClient(path).asyncExists(path, cb);
  }

  @Override
  public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {
    getZkClient(path).asyncDelete(path, cb);
  }

  @Override
  public void watchForData(String path) {
    getZkClient(path).watchForData(path);
  }

  @Override
  public List<String> watchForChilds(String path) {
    return getZkClient(path).watchForChilds(path);
  }

  @Override
  public long getCreationTime(String path) {
    return getZkClient(path).getCreationTime(path);
  }

  @Deprecated
  @Override
  public List<OpResult> multi(Iterable<Op> ops) {
    throwUnsupportedOperationException();
    return null;
  }

  @Override
  public List<OpResult> multiOps(final List<MultiOp> ops) {
    throwUnsupportedOperationException();
    return null;
  }

  @Override
  public boolean waitUntilConnected(long time, TimeUnit timeUnit) {
    throwUnsupportedOperationException();
    return false;
  }

  @Override
  public String getServers() {
    throwUnsupportedOperationException();
    return null;
  }

  @Override
  public long getSessionId() {
    // Session-aware is unsupported.
    throwUnsupportedOperationException();
    return 0L;
  }

  @Override
  public void close() {
    if (isClosed()) {
      return;
    }

    _isClosed = true;

    synchronized (_zkRealmToZkClientMap) {
      Iterator<Map.Entry<String, ZkClient>> iterator = _zkRealmToZkClientMap.entrySet().iterator();

      while (iterator.hasNext()) {
        Map.Entry<String, ZkClient> entry = iterator.next();
        String zkRealm = entry.getKey();
        ZkClient zkClient = entry.getValue();

        // Catch any exception from ZkClient's close() to avoid that there is leakage of
        // remaining unclosed ZkClient.
        try {
          zkClient.close();
        } catch (Exception e) {
          LOG.error("Exception thrown when closing ZkClient for ZkRealm: {}!", zkRealm, e);
        }
        iterator.remove();
      }
    }

    LOG.info("{} is successfully closed.", FEDERATED_ZK_CLIENT);
  }

  @Override
  public boolean isClosed() {
    return _isClosed;
  }

  @Override
  public byte[] serialize(Object data, String path) {
    return getZkClient(path).serialize(data, path);
  }

  @Override
  public <T> T deserialize(byte[] data, String path) {
    return getZkClient(path).deserialize(data, path);
  }

  @Override
  public void setZkSerializer(ZkSerializer zkSerializer) {
    _pathBasedZkSerializer = new BasicZkSerializer(zkSerializer);
    _zkRealmToZkClientMap.values()
        .forEach(zkClient -> zkClient.setZkSerializer(_pathBasedZkSerializer));
  }

  @Override
  public void setZkSerializer(PathBasedZkSerializer zkSerializer) {
    _pathBasedZkSerializer = zkSerializer;
    _zkRealmToZkClientMap.values().forEach(zkClient -> zkClient.setZkSerializer(zkSerializer));
  }

  @Override
  public PathBasedZkSerializer getZkSerializer() {
    return _pathBasedZkSerializer;
  }

  @Override
  public RealmAwareZkConnectionConfig getRealmAwareZkConnectionConfig() {
    return _connectionConfig;
  }

  @Override
  public RealmAwareZkClientConfig getRealmAwareZkClientConfig() {
    return _clientConfig;
  }

  private String create(final String path, final Object dataObject, final List<ACL> acl,
      final CreateMode mode, final String expectedSessionId) {
    if (mode.isEphemeral()) {
      throwUnsupportedOperationException();
    }

    // Create mode is not session-aware, so the node does not have to be created
    // by the expectedSessionId.
    return getZkClient(path).create(path, dataObject, acl, mode);
  }

  private ZkClient getZkClient(String path) {
    // If FederatedZkClient is closed, should not return ZkClient.
    checkClosedState();

    String zkRealm = getZkRealm(path);
    // Use this zkClient reference to protect the returning zkClient from being null because of
    // race condition. Once we get the reference, even _zkRealmToZkClientMap is cleared by closed(),
    // this zkClient is not null which guarantees the returned value not null.
    ZkClient zkClient = _zkRealmToZkClientMap.get(zkRealm);

    if (zkClient == null) {
      // 1. Synchronized to avoid creating duplicate ZkClient for the same ZkRealm.
      // 2. Synchronized with close() to avoid creating new ZkClient when all ZkClients are
      // being closed and _zkRealmToZkClientMap is being cleared.
      synchronized (_zkRealmToZkClientMap) {
        // Because of potential race condition: thread B to get ZkClient could be blocked by this
        // synchronized, while thread A is executing closed() in its synchronized block. So thread B
        // could still enter this synchronized block once A completes executing closed() and
        // releases the synchronized lock.
        // Check closed state again to avoid creating a new ZkClient after FederatedZkClient
        // is already closed.
        checkClosedState();

        if (!_zkRealmToZkClientMap.containsKey(zkRealm)) {
          zkClient = createZkClient(zkRealm);
          _zkRealmToZkClientMap.put(zkRealm, zkClient);
        } else {
          zkClient = _zkRealmToZkClientMap.get(zkRealm);
        }
      }
    }

    return zkClient;
  }

  private String getZkRealm(String path) {
    if (_routingDataUpdateOnCacheMissEnabled) {
      try {
        return updateRoutingDataOnCacheMiss(path);
      } catch (InvalidRoutingDataException e) {
        LOG.error(
            "FederatedZkClient::getZkRealm: Failed to update routing data due to invalid routing "
                + "data!", e);
        throw new MultiZkException(e);
      }
    }
    return _metadataStoreRoutingData.getMetadataStoreRealm(path);
  }

  /**
   * Perform a 2-tier routing data cache update:
   * 1. Do an in-memory update from the singleton RoutingDataManager
   * 2. Do an I/O based read from the routing data source by resetting RoutingDataManager
   * @param path
   * @return
   * @throws InvalidRoutingDataException
   */
  private String updateRoutingDataOnCacheMiss(String path) throws InvalidRoutingDataException {
    String zkRealm;
    try {
      zkRealm = _metadataStoreRoutingData.getMetadataStoreRealm(path);
    } catch (NoSuchElementException e1) {
      synchronized (this) {
        try {
          zkRealm = _metadataStoreRoutingData.getMetadataStoreRealm(path);
        } catch (NoSuchElementException e2) {
          // Try 1) Refresh MetadataStoreRoutingData from RoutingDataManager
          // This is an in-memory refresh from the Singleton RoutingDataManager - other
          // FederatedZkClient objects may have triggered a cache refresh, so we first update the
          // in-memory reference. This refresh only affects this object/thread, so we synchronize
          // on "this".
          _metadataStoreRoutingData =
              RealmAwareZkClient.getMetadataStoreRoutingData(_connectionConfig);
          try {
            zkRealm = _metadataStoreRoutingData.getMetadataStoreRealm(path);
          } catch (NoSuchElementException e3) {
            synchronized (FederatedZkClient.class) {
              try {
                zkRealm = _metadataStoreRoutingData.getMetadataStoreRealm(path);
              } catch (NoSuchElementException e4) {
                if (shouldThrottleRead()) {
                  // If routing data update from routing data source has taken place recently,
                  // then just skip the update and throw the exception
                  throw e4;
                }
                // Try 2) Reset RoutingDataManager and re-read the routing data from routing data
                // source via I/O. Since RoutingDataManager's cache doesn't have it either, so we
                // synchronize on all threads by locking on FederatedZkClient.class.
                RoutingDataManager.getInstance().reset();
                _metadataStoreRoutingData =
                    RealmAwareZkClient.getMetadataStoreRoutingData(_connectionConfig);
                // No try-catch for the following call because if this throws a
                // NoSuchElementException, it means the ZK path sharding key doesn't exist even
                // after a full cache refresh
                zkRealm = _metadataStoreRoutingData.getMetadataStoreRealm(path);
              }
            }
          }
        }
      }
    }
    return zkRealm;
  }

  private ZkClient createZkClient(String zkAddress) {
    LOG.debug("Creating ZkClient for realm: {}.", zkAddress);
    return new ZkClient(new ZkConnection(zkAddress), (int) _clientConfig.getConnectInitTimeout(),
        _clientConfig.getOperationRetryTimeout(), _pathBasedZkSerializer,
        _clientConfig.getMonitorType(), _clientConfig.getMonitorKey(),
        _clientConfig.getMonitorInstanceName(), _clientConfig.isMonitorRootPathOnly());
  }

  private void checkClosedState() {
    if (isClosed()) {
      throw new IllegalStateException(FEDERATED_ZK_CLIENT + " is closed!");
    }
  }

  private void throwUnsupportedOperationException() {
    throw new UnsupportedOperationException(
        "Session-aware operation is not supported by " + FEDERATED_ZK_CLIENT
            + ". Instead, please use " + DEDICATED_ZK_CLIENT_FACTORY
            + " to create a dedicated RealmAwareZkClient for this operation.");
  }

  /**
   * Resolves the routing data update interval value from System Properties.
   */
  private void getRoutingDataUpdateInterval() {
    try {
      _routingDataUpdateInterval = Long.parseLong(
          System.getProperty(RoutingSystemPropertyKeys.ROUTING_DATA_UPDATE_INTERVAL_MS));
      if (_routingDataUpdateInterval < 0) {
        LOG.warn("FederatedZkClient::shouldThrottleRead(): invalid value: {} given for "
                + "ROUTING_DATA_UPDATE_INTERVAL_MS, using the default value (5 sec) instead!",
            _routingDataUpdateInterval);
        _routingDataUpdateInterval = RoutingDataConstants.DEFAULT_ROUTING_DATA_UPDATE_INTERVAL_MS;
      }
    } catch (NumberFormatException e) {
      LOG.warn("FederatedZkClient::shouldThrottleRead(): failed to parse "
          + "ROUTING_DATA_UPDATE_INTERVAL_MS, using the default value (5 sec) instead!", e);
      _routingDataUpdateInterval = RoutingDataConstants.DEFAULT_ROUTING_DATA_UPDATE_INTERVAL_MS;
    }
  }

  /**
   * Return whether the read request to routing data source should be throttled using the default
   * routing data update interval.
   * @return
   */
  private boolean shouldThrottleRead() {
    return System.currentTimeMillis() - RoutingDataManager.getInstance().getLastResetTimestamp()
        < _routingDataUpdateInterval;
  }
}
