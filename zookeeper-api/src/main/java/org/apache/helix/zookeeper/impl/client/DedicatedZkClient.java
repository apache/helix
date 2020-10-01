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

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.ChildrenSubscribeResult;
import org.apache.helix.zookeeper.api.client.MultiOp;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkConnection;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener;
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
 * NOTE: DO NOT USE THIS CLASS DIRECTLY. Use DedicatedZkClientFactory to create instances of DedicatedZkClient.
 *
 * An implementation of the RealmAwareZkClient interface.
 * Supports CRUD, data change subscription, and ephemeral mode operations.
 */
public class DedicatedZkClient implements RealmAwareZkClient {
  private static Logger LOG = LoggerFactory.getLogger(DedicatedZkClient.class);

  private final ZkClient _rawZkClient;
  private final MetadataStoreRoutingData _metadataStoreRoutingData;
  private final String _zkRealmShardingKey;
  private final RealmAwareZkClient.RealmAwareZkConnectionConfig _connectionConfig;
  private final RealmAwareZkClient.RealmAwareZkClientConfig _clientConfig;

  /**
   * DedicatedZkClient connects to a single ZK realm and supports full ZkClient functionalities
   * such as CRUD, change callback, and ephemeral operations for a single ZkRealmShardingKey.
   * @param connectionConfig
   * @param clientConfig
   * @throws InvalidRoutingDataException
   */
  public DedicatedZkClient(RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig) throws InvalidRoutingDataException {
    if (connectionConfig == null) {
      throw new IllegalArgumentException("RealmAwareZkConnectionConfig cannot be null!");
    }
    if (clientConfig == null) {
      throw new IllegalArgumentException("RealmAwareZkClientConfig cannot be null!");
    }
    _connectionConfig = connectionConfig;
    _clientConfig = clientConfig;
    _metadataStoreRoutingData = RealmAwareZkClient.getMetadataStoreRoutingData(connectionConfig);
    _zkRealmShardingKey = connectionConfig.getZkRealmShardingKey();
    if (_zkRealmShardingKey == null || _zkRealmShardingKey.isEmpty()) {
      throw new IllegalArgumentException(
          "RealmAwareZkConnectionConfig's ZK realm sharding key cannot be null or empty for DedicatedZkClient!");
    }

    // Get the ZkRealm address based on the ZK path sharding key
    String zkRealmAddress = _metadataStoreRoutingData.getMetadataStoreRealm(_zkRealmShardingKey);
    if (zkRealmAddress == null || zkRealmAddress.isEmpty()) {
      throw new IllegalArgumentException(
          "ZK realm address for the given ZK realm sharding key is invalid! ZK realm address: "
              + zkRealmAddress + " ZK realm sharding key: " + _zkRealmShardingKey);
    }

    // Create a ZK connection
    IZkConnection zkConnection =
        new ZkConnection(zkRealmAddress, connectionConfig.getSessionTimeout());

    // Create a ZkClient
    _rawZkClient = new ZkClient(zkConnection, (int) clientConfig.getConnectInitTimeout(),
        clientConfig.getOperationRetryTimeout(), clientConfig.getZkSerializer(),
        clientConfig.getMonitorType(), clientConfig.getMonitorKey(),
        clientConfig.getMonitorInstanceName(), clientConfig.isMonitorRootPathOnly());
  }

  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.subscribeChildChanges(path, listener);
  }

  @Override
  public ChildrenSubscribeResult subscribeChildChanges(String path, IZkChildListener listener,
      boolean skipWatchingNodeNotExist) {
    return _rawZkClient.subscribeChildChanges(path, listener, skipWatchingNodeNotExist);
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.unsubscribeChildChanges(path, listener);
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.subscribeDataChanges(path, listener);
  }

  @Override
  public boolean subscribeDataChanges(String path, IZkDataListener listener,
      boolean skipWatchingNodeNotExist) {
    return _rawZkClient.subscribeDataChanges(path, listener, skipWatchingNodeNotExist);
  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.unsubscribeDataChanges(path, listener);
  }

  @Override
  public void subscribeStateChanges(IZkStateListener listener) {
    _rawZkClient.subscribeStateChanges(listener);
  }

  @Override
  public void unsubscribeStateChanges(IZkStateListener listener) {
    _rawZkClient.unsubscribeStateChanges(listener);
  }

  @Override
  public void unsubscribeAll() {
    _rawZkClient.unsubscribeAll();
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
    checkIfPathContainsShardingKey(path);
    _rawZkClient.createPersistent(path, createParents, acl);
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
    checkIfPathContainsShardingKey(path);
    _rawZkClient.createEphemeral(path, acl, sessionId);
  }

  @Override
  public String create(String path, Object data, CreateMode mode) {
    return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
  }

  @Override
  public String create(String path, Object datat, List<ACL> acl, CreateMode mode) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.create(path, datat, acl, mode);
  }

  @Override
  public void createEphemeral(String path, Object data) {
    create(path, data, CreateMode.EPHEMERAL);
  }

  @Override
  public void createEphemeral(String path, Object data, String sessionId) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.createEphemeral(path, data, sessionId);
  }

  @Override
  public void createEphemeral(String path, Object data, List<ACL> acl) {
    create(path, data, acl, CreateMode.EPHEMERAL);
  }

  @Override
  public void createEphemeral(String path, Object data, List<ACL> acl, String sessionId) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.createEphemeral(path, data, acl, sessionId);
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
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.createEphemeralSequential(path, data, sessionId);
  }

  @Override
  public String createEphemeralSequential(String path, Object data, List<ACL> acl,
      String sessionId) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.createEphemeralSequential(path, data, acl, sessionId);
  }

  @Override
  public List<String> getChildren(String path) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.getChildren(path);
  }

  @Override
  public int countChildren(String path) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.countChildren(path);
  }

  @Override
  public boolean exists(String path) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.exists(path);
  }

  @Override
  public Stat getStat(String path) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.getStat(path);
  }

  @Override
  public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.waitUntilExists(path, timeUnit, time);
  }

  @Override
  public void deleteRecursively(String path) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.deleteRecursively(path);
  }

  @Override
  public boolean delete(String path) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.delete(path);
  }

  @Override
  public <T> T readData(String path) {
    return readData(path, false);
  }

  @Override
  public <T> T readData(String path, boolean returnNullIfPathNotExists) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.readData(path, returnNullIfPathNotExists);
  }

  @Override
  public <T> T readData(String path, Stat stat) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.readData(path, stat);
  }

  @Override
  public <T> T readData(String path, Stat stat, boolean watch) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.readData(path, stat, watch);
  }

  @Override
  public <T> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.readDataAndStat(path, stat, returnNullIfPathNotExists);
  }

  @Override
  public void writeData(String path, Object object) {
    writeData(path, object, -1);
  }

  @Override
  public <T> void updateDataSerialized(String path, DataUpdater<T> updater) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.updateDataSerialized(path, updater);
  }

  @Override
  public void writeData(String path, Object datat, int expectedVersion) {
    writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public Stat writeDataReturnStat(String path, Object datat, int expectedVersion) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public Stat writeDataGetStat(String path, Object datat, int expectedVersion) {
    return writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public void asyncCreate(String path, Object datat, CreateMode mode,
      ZkAsyncCallbacks.CreateCallbackHandler cb) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.asyncCreate(path, datat, mode, cb);
  }

  @Override
  public void asyncSetData(String path, Object datat, int version,
      ZkAsyncCallbacks.SetDataCallbackHandler cb) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.asyncSetData(path, datat, version, cb);
  }

  @Override
  public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.asyncGetData(path, cb);
  }

  @Override
  public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.asyncExists(path, cb);
  }

  @Override
  public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.asyncDelete(path, cb);
  }

  @Override
  public void watchForData(String path) {
    checkIfPathContainsShardingKey(path);
    _rawZkClient.watchForData(path);
  }

  @Override
  public List<String> watchForChilds(String path) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.watchForChilds(path);
  }

  @Override
  public long getCreationTime(String path) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.getCreationTime(path);
  }

  @Deprecated
  @Override
  public List<OpResult> multi(Iterable<Op> ops) {
    return _rawZkClient.multi(ops);
  }

  @Override
  public List<OpResult> multiOps(final List<MultiOp> ops) {
    return _rawZkClient.multiOps(ops);
  }

  @Override
  public boolean waitUntilConnected(long time, TimeUnit timeUnit) {
    return _rawZkClient.waitUntilConnected(time, timeUnit);
  }

  @Override
  public String getServers() {
    return _rawZkClient.getServers();
  }

  @Override
  public long getSessionId() {
    return _rawZkClient.getSessionId();
  }

  @Override
  public void close() {
    _rawZkClient.close();
  }

  @Override
  public boolean isClosed() {
    return _rawZkClient.isClosed();
  }

  @Override
  public byte[] serialize(Object data, String path) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.serialize(data, path);
  }

  @Override
  public <T> T deserialize(byte[] data, String path) {
    checkIfPathContainsShardingKey(path);
    return _rawZkClient.deserialize(data, path);
  }

  @Override
  public void setZkSerializer(ZkSerializer zkSerializer) {
    _rawZkClient.setZkSerializer(zkSerializer);
  }

  @Override
  public void setZkSerializer(PathBasedZkSerializer zkSerializer) {
    _rawZkClient.setZkSerializer(zkSerializer);
  }

  @Override
  public PathBasedZkSerializer getZkSerializer() {
    return _rawZkClient.getZkSerializer();
  }

  @Override
  public RealmAwareZkConnectionConfig getRealmAwareZkConnectionConfig() {
    return _connectionConfig;
  }

  @Override
  public RealmAwareZkClientConfig getRealmAwareZkClientConfig() {
    return _clientConfig;
  }

  /**
   * Checks whether the given path belongs matches the ZK path sharding key this DedicatedZkClient is designated to at initialization.
   * @param path
   * @return
   */
  private void checkIfPathContainsShardingKey(String path) {
    try {
      String targetShardingKey = _metadataStoreRoutingData.getShardingKeyInPath(path);
      if (!_zkRealmShardingKey.equals(targetShardingKey)) {
        throw new IllegalArgumentException(
            "Given path: " + path + "'s ZK sharding key: " + targetShardingKey
                + " does not match the ZK sharding key: " + _zkRealmShardingKey
                + " for this DedicatedZkClient!");
      }
    } catch (NoSuchElementException e) {
      throw new IllegalArgumentException("Given path: " + path
          + " does not have a valid sharding key or its ZK sharding key is not found in the cached routing data!");
    }
  }
}
