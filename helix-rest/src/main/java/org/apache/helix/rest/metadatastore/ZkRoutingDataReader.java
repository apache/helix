package org.apache.helix.rest.metadatastore;

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
import java.util.Map;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.zookeeper.IZkStateListener;
import org.apache.helix.rest.metadatastore.constant.MetadataStoreRoutingConstants;
import org.apache.helix.rest.metadatastore.exceptions.InvalidRoutingDataException;
import org.apache.zookeeper.Watcher;


public class ZkRoutingDataReader implements MetadataStoreRoutingDataReader, IZkDataListener, IZkChildListener, IZkStateListener {
  private final String _namespace;
  private final String _zkAddress;
  private final HelixZkClient _zkClient;
  private final RoutingDataListener _routingDataListener;

  public ZkRoutingDataReader(String namespace, String zkAddress) {
    this(namespace, zkAddress, null);
  }

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
    _routingDataListener = routingDataListener;
    if (_routingDataListener != null) {
      // Subscribe child changes
      _zkClient.subscribeChildChanges(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, this);
      // Subscribe data changes
      for (String child : _zkClient.getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
        _zkClient
            .subscribeDataChanges(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + child,
                this);
      }
    }
  }

  /**
   * Returns (realm, list of ZK path sharding keys) mappings.
   * @return
   * @throws InvalidRoutingDataException
   */
  public Map<String, List<String>> getRoutingData()
      throws InvalidRoutingDataException {
    Map<String, List<String>> routingData = new HashMap<>();
    List<String> children;
    try {
      children = _zkClient.getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);
    } catch (ZkNoNodeException e) {
      throw new InvalidRoutingDataException(
          "Routing data directory ZNode " + MetadataStoreRoutingConstants.ROUTING_DATA_PATH
              + " does not exist. Routing ZooKeeper address: " + _zkAddress);
    }
    if (children == null || children.isEmpty()) {
      throw new InvalidRoutingDataException(
          "There are no metadata store realms defined. Routing ZooKeeper address: " + _zkAddress);
    }
    for (String child : children) {
      ZNRecord record =
          _zkClient.readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + child);
      List<String> shardingKeys =
          record.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY);
      if (shardingKeys == null || shardingKeys.isEmpty()) {
        throw new InvalidRoutingDataException(
            "Realm address ZNode " + MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + child
                + " does not have a value for key "
                + MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY
                + ". Routing ZooKeeper address: " + _zkAddress);
      }
      routingData.put(child, shardingKeys);
    }
    return routingData;
  }

  public synchronized void close() {
    _zkClient.unsubscribeAll();
    _zkClient.close();
  }

  @Override
  public synchronized void handleDataChange(String s, Object o)
      throws Exception {
    if (_zkClient.isClosed()) {
      return;
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }

  @Override
  public synchronized void handleDataDeleted(String s)
      throws Exception {
    if (_zkClient.isClosed()) {
      return;
    }

    // Renew subscription
    _zkClient.subscribeChildChanges(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, this);
    for (String child : _zkClient.getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
      _zkClient.subscribeDataChanges(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + child,
          this);
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }

  @Override
  public synchronized void handleChildChange(String s, List<String> list)
      throws Exception {
    if (_zkClient.isClosed()) {
      return;
    }

    // Subscribe data changes again because some children might have been deleted or added
    _zkClient.unsubscribeAll();
    for (String child : _zkClient.getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
      _zkClient.subscribeDataChanges(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + child,
          this);
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }

  @Override
  public synchronized void handleStateChanged(Watcher.Event.KeeperState state)
      throws Exception {
    if (_zkClient.isClosed()) {
      return;
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }

  @Override
  public synchronized void handleNewSession(String sessionId)
      throws Exception {
    if (_zkClient.isClosed()) {
      return;
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }

  @Override
  public synchronized void handleSessionEstablishmentError(Throwable error)
      throws Exception {
    if (_zkClient.isClosed()) {
      return;
    }
    _routingDataListener.refreshRoutingData(_namespace);
  }
}
