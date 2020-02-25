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
import java.util.concurrent.TimeUnit;

import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * NOTE: DO NOT USE THIS CLASS DIRECTLY. USE SharedZkClientFactory instead.
 *
 * HelixZkClient that uses shared ZkConnection.
 * A SharedZkClient won't manipulate the shared ZkConnection directly.
 */
public class SharedZkClient implements RealmAwareZkClient {
  private static Logger LOG = LoggerFactory.getLogger(SharedZkClient.class);

  private final HelixZkClient _innerSharedZkClient;
  private final String _zkRealmShardingKey;

  public SharedZkClient(RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig,
      MetadataStoreRoutingData metadataStoreRoutingData) {

    if (connectionConfig == null) {
      throw new IllegalArgumentException("RealmAwareZkConnectionConfig cannot be null!");
    }
    _zkRealmShardingKey = connectionConfig.getZkRealmShardingKey();

    // TODO: use _zkRealmShardingKey to generate zkRealmAddress. This can done the same way of pull 765 once @hunter check it in.
    // Get the ZkRealm address based on the ZK path sharding key
    String zkRealmAddress = null;
    zkRealmAddress = metadataStoreRoutingData.getMetadataStoreRealm(_zkRealmShardingKey);

    // Create an InnerSharedZkClient to actually serve ZK requests
    // TODO: Rename HelixZkClient in the future or remove it entirely - this will be a backward-compatibility breaking change because HelixZkClient is being used by Helix users.

    // Note, here delegate _innerSharedZkClient would share the same connectionManager. Once the close() API of
    // SharedZkClient is invoked, we can just call the close() API of delegate _innerSharedZkClient. This would follow
    // exactly the pattern of innerSharedZkClient closing logic, which would close the connectionManager when the last
    // sharedInnerZkClient is closed.
    HelixZkClient.ZkConnectionConfig zkConnectionConfig =
        new HelixZkClient.ZkConnectionConfig(zkRealmAddress)
            .setSessionTimeout(connectionConfig.getSessionTimeout());
    HelixZkClient.ZkClientConfig zkClientConfig = new HelixZkClient.ZkClientConfig();
    zkClientConfig.setZkSerializer(clientConfig.getZkSerializer())
        .setConnectInitTimeout(clientConfig.getConnectInitTimeout())
        .setOperationRetryTimeout(clientConfig.getOperationRetryTimeout())
        .setMonitorInstanceName(clientConfig.getMonitorInstanceName())
        .setMonitorKey(clientConfig.getMonitorKey())
        .setMonitorType(clientConfig.getMonitorType())
        .setMonitorRootPathOnly(clientConfig.isMonitorRootPathOnly());
    _innerSharedZkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(zkConnectionConfig, zkClientConfig);
  }

  private void checkPathAndThrow(String path) {
    if (!checkIfPathBelongsToZkRealm(path)) {
      throw new IllegalArgumentException(
          "The given path does not map to the ZK realm for this DedicatedZkClient! Path: " + path
              + " ZK realm sharding key: " + _zkRealmShardingKey);
    }
  }

  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.subscribeChildChanges(path, listener);
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener) {
    checkPathAndThrow(path);
    _innerSharedZkClient.unsubscribeChildChanges(path, listener);
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    checkPathAndThrow(path);
    _innerSharedZkClient.subscribeDataChanges(path, listener);
  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener) {
    checkPathAndThrow(path);
    _innerSharedZkClient.unsubscribeDataChanges(path, listener);
  }

  @Override
  public void subscribeStateChanges(IZkStateListener listener) {
    _innerSharedZkClient.subscribeStateChanges(listener);
  }

  @Override
  public void unsubscribeStateChanges(IZkStateListener listener) {
    _innerSharedZkClient.unsubscribeStateChanges(listener);
  }

  @Override
  public void unsubscribeAll() {
    _innerSharedZkClient.unsubscribeAll();
  }

  @Override
  public void createPersistent(String path) {
    checkPathAndThrow(path);
    _innerSharedZkClient.createPersistent(path);
  }

  @Override
  public void createPersistent(String path, boolean createParents) {
    checkPathAndThrow(path);
    _innerSharedZkClient.createPersistent(path, createParents);
  }

  @Override
  public void createPersistent(String path, boolean createParents, List<ACL> acl) {
    checkPathAndThrow(path);
    _innerSharedZkClient.createPersistent(path, createParents, acl);
  }

  @Override
  public void createPersistent(String path, Object data) {
    checkPathAndThrow(path);
    _innerSharedZkClient.createPersistent(path, data);
  }

  @Override
  public void createPersistent(String path, Object data, List<ACL> acl) {
    checkPathAndThrow(path);
    _innerSharedZkClient.createPersistent(path, data, acl);
  }

  @Override
  public String createPersistentSequential(String path, Object data) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.createPersistentSequential(path, data);
  }

  @Override
  public String createPersistentSequential(String path, Object data, List<ACL> acl) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.createPersistentSequential(path, data, acl);
  }

  @Override
  public void createEphemeral(String path) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public void createEphemeral(String path, String sessionId) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public void createEphemeral(String path, List<ACL> acl) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public void createEphemeral(String path, List<ACL> acl, String sessionId) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public String create(String path, Object data, CreateMode mode) {
    checkPathAndThrow(path);
    // delegate to _innerSharedZkClient is fine as _innerSharedZkClient would not allow creating ephemeral node.
    // this still keeps the same behavior.
    return _innerSharedZkClient.create(path, data, mode);
  }

  @Override
  public String create(String path, Object datat, List<ACL> acl, CreateMode mode) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.create(path, datat, acl, mode);
  }

  @Override
  public void createEphemeral(String path, Object data) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public void createEphemeral(String path, Object data, String sessionId) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public void createEphemeral(String path, Object data, List<ACL> acl) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public void createEphemeral(String path, Object data, List<ACL> acl, String sessionId) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public String createEphemeralSequential(String path, Object data) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public String createEphemeralSequential(String path, Object data, List<ACL> acl) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public String createEphemeralSequential(String path, Object data, String sessionId) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public String createEphemeralSequential(String path, Object data, List<ACL> acl,
      String sessionId) {
    throw new UnsupportedOperationException(
        "Create ephemeral nodes using " + SharedZkClient.class.getSimpleName()
            + " is not supported.");
  }

  @Override
  public List<String> getChildren(String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.getChildren(path);
  }

  @Override
  public int countChildren(String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.countChildren(path);
  }

  @Override
  public boolean exists(String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.exists(path);
  }

  @Override
  public Stat getStat(String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.getStat(path);
  }

  @Override
  public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.waitUntilExists(path, timeUnit, time);
  }

  @Override
  public void deleteRecursively(String path) {
    checkPathAndThrow(path);
    _innerSharedZkClient.deleteRecursively(path);
  }

  @Override
  public boolean delete(String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.delete(path);
  }

  @Override
  public <T> T readData(String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.readData(path);
  }

  @Override
  public <T> T readData(String path, boolean returnNullIfPathNotExists) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.readData(path, returnNullIfPathNotExists);
  }

  @Override
  public <T> T readData(String path, Stat stat) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.readData(path, stat);
  }

  @Override
  public <T> T readData(String path, Stat stat, boolean watch) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.readData(path, stat, watch);
  }

  @Override
  public <T> T readDataAndStat(String path, Stat stat, boolean returnNullIfPathNotExists) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.readDataAndStat(path, stat, returnNullIfPathNotExists);
  }

  @Override
  public void writeData(String path, Object object) {
    checkPathAndThrow(path);
    _innerSharedZkClient.writeData(path, object);
  }

  @Override
  public <T> void updateDataSerialized(String path, DataUpdater<T> updater) {
    checkPathAndThrow(path);
    _innerSharedZkClient.updateDataSerialized(path, updater);
  }

  @Override
  public void writeData(String path, Object datat, int expectedVersion) {
    checkPathAndThrow(path);
    _innerSharedZkClient.writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public Stat writeDataReturnStat(String path, Object datat, int expectedVersion) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public Stat writeDataGetStat(String path, Object datat, int expectedVersion) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.writeDataReturnStat(path, datat, expectedVersion);
  }

  @Override
  public void asyncCreate(String path, Object datat, CreateMode mode,
      ZkAsyncCallbacks.CreateCallbackHandler cb) {
    checkPathAndThrow(path);
    _innerSharedZkClient.asyncCreate(path, datat, mode, cb);
  }

  @Override
  public void asyncSetData(String path, Object datat, int version,
      ZkAsyncCallbacks.SetDataCallbackHandler cb) {
    checkPathAndThrow(path);
    _innerSharedZkClient.asyncSetData(path, datat, version, cb);
  }

  @Override
  public void asyncGetData(String path, ZkAsyncCallbacks.GetDataCallbackHandler cb) {
    checkPathAndThrow(path);
    _innerSharedZkClient.asyncGetData(path, cb);
  }

  @Override
  public void asyncExists(String path, ZkAsyncCallbacks.ExistsCallbackHandler cb) {
    checkPathAndThrow(path);
    _innerSharedZkClient.asyncExists(path, cb);
  }

  @Override
  public void asyncDelete(String path, ZkAsyncCallbacks.DeleteCallbackHandler cb) {
    checkPathAndThrow(path);
    _innerSharedZkClient.asyncDelete(path, cb);
  }

  @Override
  public void watchForData(String path) {
    checkPathAndThrow(path);
    _innerSharedZkClient.watchForData(path);
  }

  @Override
  public List<String> watchForChilds(String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.watchForChilds(path);
  }

  @Override
  public long getCreationTime(String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.getCreationTime(path);
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) {
    return _innerSharedZkClient.multi(ops);
  }

  @Override
  public boolean waitUntilConnected(long time, TimeUnit timeUnit) {
    return _innerSharedZkClient.waitUntilConnected(time, timeUnit);
  }

  @Override
  public String getServers() {
    return _innerSharedZkClient.getServers();
  }

  @Override
  public long getSessionId() {
    return _innerSharedZkClient.getSessionId();
  }

  @Override
  public void close() {
    _innerSharedZkClient.close();
  }

  @Override
  public boolean isClosed() {
    return _innerSharedZkClient.isClosed();
  }

  @Override
  public byte[] serialize(Object data, String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.serialize(data, path);
  }

  @Override
  public <T> T deserialize(byte[] data, String path) {
    checkPathAndThrow(path);
    return _innerSharedZkClient.deserialize(data, path);
  }

  @Override
  public void setZkSerializer(ZkSerializer zkSerializer) {
    _innerSharedZkClient.setZkSerializer(zkSerializer);
  }

  @Override
  public void setZkSerializer(PathBasedZkSerializer zkSerializer) {
    _innerSharedZkClient.setZkSerializer(zkSerializer);
  }

  @Override
  public PathBasedZkSerializer getZkSerializer() {
    return _innerSharedZkClient.getZkSerializer();
  }

  private boolean checkIfPathBelongsToZkRealm(String path) {
    // TODO: Check if the path's sharding key equals the sharding key
    // TODO: Implement this with TrieRoutingData
    return true;
  }
}
