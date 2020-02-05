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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient.ZkClientConfig;
import org.apache.helix.manager.zk.zookeeper.IZkStateListener;
import org.apache.helix.rest.metadatastore.exceptions.InvalidRoutingDataException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ZK-based MetadataStoreDirectory that listens on the routing data in routing ZKs with a update callback.
 */
public class ZkMetadataStoreDirectory implements MetadataStoreDirectory, RoutingDataListener {
  private static final Logger LOG = LoggerFactory.getLogger(ZkMetadataStoreDirectory.class);

  // TODO: enable the line below when implementation is complete
  // The following maps' keys represent the namespace
  private final Map<String, MetadataStoreRoutingDataReader> _routingDataReaderMap;
  private final Map<String, MetadataStoreRoutingData> _routingDataMap;
  private final Map<String, String> _routingZkAddressMap;
  // (namespace, (realm, list of sharding keys)) mappping
  private final Map<String, Map<String, List<String>>> _realmToShardingKeysMap;

  /**
   * Creates a ZkMetadataStoreDirectory based on the given routing ZK addresses.
   * @param routingZkAddressMap (namespace, routing ZK connect string)
   * @throws InvalidRoutingDataException
   */
  public ZkMetadataStoreDirectory(Map<String, String> routingZkAddressMap)
      throws InvalidRoutingDataException {
    if (routingZkAddressMap == null || routingZkAddressMap.isEmpty()) {
      throw new InvalidRoutingDataException("Routing ZK Addresses given are invalid!");
    }
    _routingDataReaderMap = new HashMap<>();
    _routingZkAddressMap = routingZkAddressMap;
    _realmToShardingKeysMap = new ConcurrentHashMap<>();
    _routingDataMap = new ConcurrentHashMap<>();

    // Create RoutingDataReaders
    for (Map.Entry<String, String> routingEntry : _routingZkAddressMap.entrySet()) {
      _routingDataReaderMap.put(routingEntry.getKey(),
          new ZkRoutingDataReader(routingEntry.getKey(), routingEntry.getValue(), this));

      // Populate realmToShardingKeys with ZkRoutingDataReader
      _realmToShardingKeysMap.put(routingEntry.getKey(),
          _routingDataReaderMap.get(routingEntry.getKey()).getRoutingData());
    }
  }

  @Override
  public Collection<String> getAllNamespaces() {
    return Collections.unmodifiableCollection(_routingZkAddressMap.keySet());
  }

  @Override
  public Collection<String> getAllMetadataStoreRealms(String namespace) {
    if (!_realmToShardingKeysMap.containsKey(namespace)) {
      throw new NoSuchElementException("Namespace " + namespace + " does not exist!");
    }
    return Collections.unmodifiableCollection(_realmToShardingKeysMap.get(namespace).keySet());
  }

  @Override
  public Collection<String> getAllShardingKeys(String namespace) {
    if (!_realmToShardingKeysMap.containsKey(namespace)) {
      throw new NoSuchElementException("Namespace " + namespace + " does not exist!");
    }
    Set<String> allShardingKeys = new HashSet<>();
    _realmToShardingKeysMap.get(namespace).values().forEach(keys -> allShardingKeys.addAll(keys));
    return allShardingKeys;
  }

  @Override
  public Collection<String> getAllShardingKeysInRealm(String namespace, String realm) {
    if (!_realmToShardingKeysMap.containsKey(namespace)) {
      throw new NoSuchElementException("Namespace " + namespace + " does not exist!");
    }
    if (!_realmToShardingKeysMap.get(namespace).containsKey(realm)) {
      throw new NoSuchElementException(
          "Realm " + realm + " does not exist in namespace " + namespace);
    }
    return Collections.unmodifiableCollection(_realmToShardingKeysMap.get(namespace).get(realm));
  }

  @Override
  public Map<String, String> getAllMappingUnderPath(String namespace, String path) {
    // TODO: get it from routingData
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMetadataStoreRealm(String namespace, String shardingKey) {
    // TODO: get it from routingData
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addNamespace(String namespace) {
    // TODO implement when MetadataStoreRoutingDataWriter is ready
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteNamespace(String namespace) {
    // TODO implement when MetadataStoreRoutingDataWriter is ready
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addMetadataStoreRealm(String namespace, String realm) {
    // TODO implement when MetadataStoreRoutingDataWriter is ready
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteMetadataStoreRealm(String namespace, String realm) {
    // TODO implement when MetadataStoreRoutingDataWriter is ready
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addShardingKey(String namespace, String realm, String shardingKey) {
    // TODO implement when MetadataStoreRoutingDataWriter is ready
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteShardingKey(String namespace, String realm, String shardingKey) {
    // TODO implement when MetadataStoreRoutingDataWriter is ready
    throw new UnsupportedOperationException();
  }

  /**
   * Callback for updating the cached routing data.
   * Note: this method should not synchronize on the class or the map. We do not want namespaces blocking each other.
   * Threadsafe map is used for _realmToShardingKeysMap.
   * The global consistency of the in-memory routing data is not a requirement (eventual consistency is enough).
   * @param namespace
   */
  @Override
  public void updateRoutingData(String namespace) {
    // Safe to ignore the callback if routingDataMap is null.
    // If routingDataMap is null, then it will be populated by the constructor anyway
    // If routingDataMap is not null, then it's safe for the callback function to update it
    try {
      _realmToShardingKeysMap.put(namespace, _routingDataReaderMap.get(namespace).getRoutingData());
    } catch (InvalidRoutingDataException e) {
      LOG.error("Failed to get routing data for namespace: " + namespace + "!");
    }

    if (_routingDataMap != null) {
      MetadataStoreRoutingData newRoutingData =
          new TrieRoutingData(new TrieRoutingData.TrieNode(null, null, false, null));
      // TODO call constructRoutingData() here.
      _routingDataMap.put(namespace, newRoutingData);
    }
  }

  @Override
  public synchronized void close() {
    _routingDataReaderMap.values().forEach(MetadataStoreRoutingDataReader::close);
  }
}
