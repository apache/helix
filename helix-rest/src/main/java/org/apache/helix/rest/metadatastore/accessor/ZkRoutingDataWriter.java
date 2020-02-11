package org.apache.helix.rest.metadatastore.accessor;

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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.rest.metadatastore.concurrency.ZkDistributedLeaderElection;
import org.apache.helix.rest.metadatastore.constant.MetadataStoreRoutingConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkRoutingDataWriter implements MetadataStoreRoutingDataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ZkBaseDataAccessor.class);

  private final String _namespace;
  private final HelixZkClient _zkClient;
  private final ZkDistributedLeaderElection _leaderElection;

  public ZkRoutingDataWriter(String namespace, String zkAddress) {
    if (namespace == null || namespace.isEmpty()) {
      throw new IllegalArgumentException("namespace cannot be null or empty!");
    }
    _namespace = namespace;
    if (zkAddress == null || zkAddress.isEmpty()) {
      throw new IllegalArgumentException("Zk address cannot be null or empty!");
    }
    _zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
            new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));

    // Ensure that ROUTING_DATA_PATH exists in ZK. If not, create
    // create() semantic will fail if it already exists
    try {
      _zkClient.createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, true);
    } catch (ZkNodeExistsException e) {
      // This is okay
    }

    // Get the hostname (REST endpoint) from System property
    // TODO: Fill in when Helix REST implementations are ready
    ZNRecord myServerInfo = new ZNRecord("dummy hostname");
    _leaderElection = new ZkDistributedLeaderElection(_zkClient,
        MetadataStoreRoutingConstants.LEADER_ELECTION_ZNODE, myServerInfo);
  }

  @Override
  public synchronized boolean addMetadataStoreRealm(String realm) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      return createZkRealm(realm);
    }

    // TODO: Forward the request to leader
    return true;
  }

  @Override
  public synchronized boolean deleteMetadataStoreRealm(String realm) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      return _zkClient.delete(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm);
    }

    // TODO: Forward the request to leader
    return true;
  }

  @Override
  public synchronized boolean addShardingKey(String realm, String shardingKey) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      // If the realm does not exist already, then create the realm
      String realmPath = MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm;
      if (!_zkClient.exists(realmPath)) {
        // Create the realm
        if (!createZkRealm(realm)) {
          // Failed to create the realm - log and return false
          LOG.error(
              "Failed to add sharding key because ZkRealm creation failed! Namespace: {}, Realm: {}, Sharding key: {}",
              _namespace, realm, shardingKey);
          return false;
        }
      }

      // Add the sharding key to an empty ZNRecord
      ZNRecord znRecord;
      try {
        znRecord = _zkClient.readData(realmPath);
      } catch (Exception e) {
        LOG.error(
            "Failed to read the realm ZNRecord in addShardingKey()! Namespace: {}, Realm: {}, ShardingKey: {}",
            _namespace, realm, shardingKey, e);
        return false;
      }
      znRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
          Collections.singletonList(shardingKey));
      try {
        _zkClient.writeData(realmPath, znRecord);
      } catch (Exception e) {
        LOG.error(
            "Failed to write the realm ZNRecord in addShardingKey()! Namespace: {}, Realm: {}, ShardingKey: {}",
            _namespace, realm, shardingKey, e);
        return false;
      }
      return true;
    }

    // TODO: Forward the request to leader
    return true;
  }

  @Override
  public synchronized boolean deleteShardingKey(String realm, String shardingKey) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      ZNRecord znRecord =
          _zkClient.readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm, true);
      if (znRecord == null || !znRecord
          .getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
          .contains(shardingKey)) {
        // This realm does not exist or shardingKey doesn't exist. Return true!
        return true;
      }
      znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
          .remove(shardingKey);
      // Overwrite this ZNRecord with the sharding key removed
      try {
        _zkClient
            .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm, znRecord);
      } catch (Exception e) {
        LOG.error(
            "Failed to write the data back in deleteShardingKey()! Namespace: {}, Realm: {}, ShardingKey: {}",
            _namespace, realm, shardingKey, e);
        return false;
      }
      return true;
    }

    // TODO: Forward the request to leader
    return true;
  }

  @Override
  public synchronized boolean setRoutingData(Map<String, List<String>> routingData) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      if (routingData == null) {
        throw new IllegalArgumentException("routingData given is null!");
      }

      // Remove existing routing data
      for (String zkRealm : _zkClient
          .getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
        if (!_zkClient.delete(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm)) {
          LOG.error(
              "Failed to delete existing routing data in setRoutingData()! Namespace: {}, Realm: {}",
              _namespace, zkRealm);
          return false;
        }
      }

      // For each ZkRealm, write the given routing data to ZooKeeper
      for (Map.Entry<String, List<String>> routingDataEntry : routingData.entrySet()) {
        String zkRealm = routingDataEntry.getKey();
        List<String> shardingKeyList = routingDataEntry.getValue();

        ZNRecord znRecord = new ZNRecord(zkRealm);
        znRecord
            .setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY, shardingKeyList);

        String realmPath = MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm;
        try {
          if (!_zkClient.exists(realmPath)) {
            _zkClient.createPersistent(realmPath);
          }
          _zkClient.writeData(realmPath, znRecord);
        } catch (Exception e) {
          LOG.error("Failed to write data in setRoutingData()! Namespace: {}, Realm: {}",
              _namespace, zkRealm, e);
          return false;
        }
      }
      return true;
    }

    // TODO: Forward the request to leader
    return true;
  }

  @Override
  public synchronized void close() {
    _zkClient.close();
  }

  /**
   * Creates a ZK realm ZNode and populates it with an empty ZNRecord if it doesn't exist already.
   * @param realm
   * @return
   */
  private boolean createZkRealm(String realm) {
    if (_zkClient.exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm)) {
      LOG.warn("createZkRealm() called for realm: {}, but this realm already exists! Namespace: {}",
          realm, _namespace);
      return true;
    }
    try {
      _zkClient.createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm);
      _zkClient.writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm,
          new ZNRecord(realm));
    } catch (Exception e) {
      LOG.error("Failed to create ZkRealm: {}, Namespace: ", realm, _namespace);
      return false;
    }

    return true;
  }
}
