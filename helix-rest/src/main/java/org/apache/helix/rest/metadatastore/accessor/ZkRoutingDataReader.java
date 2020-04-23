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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.msdcommon.callback.RoutingDataListener;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.metadatastore.ZkMetadataStoreDirectory;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.Watcher;


public class ZkRoutingDataReader implements MetadataStoreRoutingDataReader, IZkDataListener, IZkChildListener, IZkStateListener {
  private final String _namespace;
  private final String _zkAddress;
  private final HelixZkClient _zkClient;
  private final RoutingDataListener _routingDataListener;

  public ZkRoutingDataReader(String namespace, String zkAddress,
      RoutingDataListener routingDataListener) {
    if (namespace == null || namespace.isEmpty()) {
      throw new IllegalArgumentException("namespace cannot be null or empty!");
    }
    _namespace = namespace;
    if (zkAddress == null || zkAddress.isEmpty()) {
      throw new IllegalArgumentException("Zk address cannot be null or empty!");
    }
    _zkAddress = zkAddress;
    _zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
            new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));

    ZkMetadataStoreDirectory.createRoutingDataPath(_zkClient, _zkAddress);

    _routingDataListener = routingDataListener;
    if (_routingDataListener != null) {
      _zkClient.subscribeRoutingDataChanges(this, this);
    }
  }

  /**
   * Returns (realm, list of ZK path sharding keys) mappings.
   * @return Map <realm, list of ZK path sharding keys>
   * @throws InvalidRoutingDataException - when the node on
   *           MetadataStoreRoutingConstants.ROUTING_DATA_PATH is missing
   */
  public Map<String, List<String>> getRoutingData() throws InvalidRoutingDataException {
    Map<String, List<String>> routingData = new HashMap<>();
    List<String> allRealmAddresses;
    try {
      allRealmAddresses = _zkClient.getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);
    } catch (ZkNoNodeException e) {
      throw new InvalidRoutingDataException(
          "Routing data directory ZNode " + MetadataStoreRoutingConstants.ROUTING_DATA_PATH
              + " does not exist. Routing ZooKeeper address: " + _zkAddress);
    }
    if (allRealmAddresses != null) {
      for (String realmAddress : allRealmAddresses) {
        ZNRecord record = _zkClient
            .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realmAddress, true);
        if (record != null) {
          List<String> shardingKeys =
              record.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY);
          routingData
              .put(realmAddress, shardingKeys != null ? shardingKeys : Collections.emptyList());
        }
      }
    }
    return routingData;
  }

  public synchronized void close() {
    _zkClient.unsubscribeAll();
    _zkClient.close();
  }

  @Override
  public synchronized void handleDataChange(String s, Object o) {
    if (_zkClient == null || _zkClient.isClosed()) {
      return;
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }

  @Override
  public synchronized void handleDataDeleted(String s) {
    // When a child node is deleted, this and handleChildChange will both be triggered, but the
    // behavior is safe
    handleResubscription();
  }

  @Override
  public synchronized void handleChildChange(String s, List<String> list) {
    handleResubscription();
  }

  @Override
  public synchronized void handleStateChanged(Watcher.Event.KeeperState state) {
    if (_zkClient == null || _zkClient.isClosed()) {
      return;
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }

  @Override
  public synchronized void handleNewSession(String sessionId) {
    if (_zkClient == null || _zkClient.isClosed()) {
      return;
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }

  @Override
  public synchronized void handleSessionEstablishmentError(Throwable error) {
    if (_zkClient == null || _zkClient.isClosed()) {
      return;
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }

  private void handleResubscription() {
    if (_zkClient == null || _zkClient.isClosed()) {
      return;
    }
    // Renew subscription
    _zkClient.unsubscribeAll();
    _zkClient.subscribeRoutingDataChanges(this, this);
    _routingDataListener.refreshRoutingData(_namespace);
  }
}
