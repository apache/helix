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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkConnection;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.helix.zookeeper.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


/**
 * NOTE: DO NOT USE THIS CLASS DIRECTLY. Use DedicatedZkClientFactory to create instances of DedicatedZkClient.
 *
 * An implementation of the RealmAwareZkClient interface.
 * Supports CRUD, data change subscriptiona, and ephemeral mode operations.
 */
public class DedicatedZkClient implements RealmAwareZkClient {
  private final ZkClient _rawZkClient;
  private final Map<String, String> _routingDataCache;
  private final String _zkRealmShardingKey;

  public DedicatedZkClient(RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig,
      Map<String, String> routingDataCache) {

    if (connectionConfig == null) {
      throw new IllegalArgumentException("RealmAwareZkConnectionConfig cannot be null!");
    }
    _zkRealmShardingKey = connectionConfig.getZkRealmShardingKey();

    // TODO: Replace this Map with a real RoutingDataCache
    if (routingDataCache == null) {
      throw new IllegalArgumentException("RoutingDataCache cannot be null!");
    }
    _routingDataCache = routingDataCache;

    // Get the ZkRealm address based on the ZK path sharding key
    String zkRealmAddress = _routingDataCache.get(_zkRealmShardingKey);

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
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.subscribeChildChanges(path, listener);
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.unsubscribeChildChanges(path, listener);
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.subscribeDataChanges(path, listener);
  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
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
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
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
    create(path, null, acl, CreateMode.EPHEMERAL, sessionId);
  }

  @Override
  public String create(String path, Object data, CreateMode mode) {
    return create(path, data, mode);
  }

  @Override
  public String create(String path, Object datat, List<ACL> acl, CreateMode mode) {
    return create(path, datat, acl, mode, null);
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
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.getChildren(path);
  }

  @Override
  public int countChildren(String path) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return countChildren(path);
  }

  @Override
  public boolean exists(String path) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.exists(path);
  }

  @Override
  public Stat getStat(String path) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.getStat(path);
  }

  @Override
  public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.waitUntilExists(path, timeUnit, time);
  }

  @Override
  public void deleteRecursively(String path) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.deleteRecursively(path);
  }

  @Override
  public boolean delete(String path) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.delete(path);
  }

  @Override
  public <T> T readData(String path) {
    return readData(path, false);
  }

  @Override
  public <T> T readData(String path, boolean returnNullIfPathNotExists) {
    T data = null;
    try {
      return readData(path, null);
    } catch (ZkNoNodeException e) {
      if (!returnNullIfPathNotExists) {
        throw e;
      }
    }
    return data;
  }

  @Override
  public <T> T readData(String path, Stat stat) {
    return readData(path, stat, _rawZkClient.hasListeners(path));
  }

  @Override
  public <T> T readData(String path, Stat stat, boolean watch) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.readData(path, stat, watch);
  }

  @Override
  public <T> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists) {
    T data = null;
    try {
      data = readData(path, stat);
    } catch (ZkNoNodeException e) {
      if (!returnNullIfPathNotExists) {
        throw e;
      }
    }
    return data;
  }

  @Override
  public void writeData(String path, Object object) {
    writeData(path, object, -1);
  }

  @Override
  public <T> void updateDataSerialized(String path, DataUpdater<T> updater) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.updateDataSerialized(path, updater);
  }

  @Override
  public void writeData(String path, Object datat, int expectedVersion) {
    writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public Stat writeDataReturnStat(String path, Object datat, int expectedVersion) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public Stat writeDataGetStat(String path, Object datat, int expectedVersion) {
    return writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public void asyncCreate(String path, Object datat, CreateMode mode,
      ZkAsyncCallbacks.CreateCallbackHandler cb) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.asyncCreate(path, datat, mode, cb);
  }

  @Override
  public void asyncSetData(String path, Object datat, int version,
      ZkAsyncCallbacks.SetDataCallbackHandler cb) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.asyncSetData(path, datat, version, cb);
  }

  @Override
  public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.asyncGetData(path, cb);
  }

  @Override
  public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.asyncExists(path, cb);
  }

  @Override
  public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.asyncDelete(path, cb);
  }

  @Override
  public void watchForData(String path) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    _rawZkClient.watchForData(path);
  }

  @Override
  public List<String> watchForChilds(String path) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.watchForChilds(path);
  }

  @Override
  public long getCreationTime(String path) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.getCreationTime(path);
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) {
    return _rawZkClient.multi(ops);
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
    return _rawZkClient.serialize(data, path);
  }

  @Override
  public <T> T deserialize(byte[] data, String path) {
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

  /**
   * The most comprehensive create() method that checks the path to see if the create request should be authorized to go through.
   * @param path
   * @param dataObject
   * @param acl
   * @param mode
   * @param expectedSessionId
   * @return
   */
  private String create(final String path, final Object dataObject, final List<ACL> acl,
      final CreateMode mode, final String expectedSessionId) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
    return _rawZkClient.create(path, dataObject, acl, mode, expectedSessionId);
  }

  private boolean checkIfPathBelongsToZkRealm(String path) {
    // TODO: Check if the path's sharding key equals the sharding key
    // TODO: Implement this with TrieRoutingData
    return true;
  }
}
