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

import org.apache.helix.rest.metadatastore.accessor.MetadataStoreRoutingDataReader;
import org.apache.helix.rest.metadatastore.accessor.MetadataStoreRoutingDataWriter;
import org.apache.helix.rest.metadatastore.accessor.ZkRoutingDataReader;
import org.apache.helix.rest.metadatastore.accessor.ZkRoutingDataWriter;
import org.apache.helix.rest.metadatastore.exceptions.InvalidRoutingDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZK-based MetadataStoreDirectory that listens on the routing data in routing ZKs with a update
 * callback.
 */
public class ZkMetadataStoreDirectory implements MetadataStoreDirectory, RoutingDataListener {
  private static final Logger LOG = LoggerFactory.getLogger(ZkMetadataStoreDirectory.class);

  // TODO: enable the line below when implementation is complete
  // The following maps' keys represent the namespace
  private final Map<String, MetadataStoreRoutingDataReader> _routingDataReaderMap;
  private final Map<String, MetadataStoreRoutingDataWriter> _routingDataWriterMap;
  private final Map<String, MetadataStoreRoutingData> _routingDataMap;
  private final Map<String, String> _routingZkAddressMap;
  // <namespace, <realm, <list of sharding keys>> mappings
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
    _routingDataWriterMap = new HashMap<>();
    _routingZkAddressMap = routingZkAddressMap;
    _realmToShardingKeysMap = new ConcurrentHashMap<>();
    _routingDataMap = new ConcurrentHashMap<>();

    // Create RoutingDataReaders and RoutingDataWriters
    for (Map.Entry<String, String> routingEntry : _routingZkAddressMap.entrySet()) {
      _routingDataReaderMap.put(routingEntry.getKey(),
          new ZkRoutingDataReader(routingEntry.getKey(), routingEntry.getValue(), this));
      _routingDataWriterMap.put(routingEntry.getKey(),
          new ZkRoutingDataWriter(routingEntry.getKey(), routingEntry.getValue()));

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
    if (!_routingDataMap.containsKey(namespace)) {
      throw new IllegalArgumentException(
          "Failed to get all mapping under path: Namespace " + namespace + " is not found!");
    }
    return _routingDataMap.get(namespace).getAllMappingUnderPath(path);
  }

  @Override
  public String getMetadataStoreRealm(String namespace, String shardingKey) {
    if (!_routingDataMap.containsKey(namespace)) {
      throw new IllegalArgumentException(
          "Failed to get all mapping under path: Namespace " + namespace + " is not found!");
    }
    return _routingDataMap.get(namespace).getMetadataStoreRealm(shardingKey);
  }

  @Override
  public boolean addMetadataStoreRealm(String namespace, String realm) {
    if (!_routingDataWriterMap.containsKey(namespace)) {
      throw new IllegalArgumentException(
          "Failed to add metadata store realm: Namespace " + namespace + " is not found!");
    }
    return _routingDataWriterMap.get(namespace).addMetadataStoreRealm(realm);
  }

  @Override
  public boolean deleteMetadataStoreRealm(String namespace, String realm) {
    if (!_routingDataWriterMap.containsKey(namespace)) {
      throw new IllegalArgumentException(
          "Failed to delete metadata store realm: Namespace " + namespace + " is not found!");
    }
    return _routingDataWriterMap.get(namespace).deleteMetadataStoreRealm(realm);
  }

  @Override
  public boolean addShardingKey(String namespace, String realm, String shardingKey) {
    if (!_routingDataWriterMap.containsKey(namespace)) {
      throw new IllegalArgumentException(
          "Failed to add sharding key: Namespace " + namespace + " is not found!");
    }
    return _routingDataWriterMap.get(namespace).addShardingKey(realm, shardingKey);
  }

  @Override
  public boolean deleteShardingKey(String namespace, String realm, String shardingKey) {
    if (!_routingDataWriterMap.containsKey(namespace)) {
      throw new IllegalArgumentException(
          "Failed to delete sharding key: Namespace " + namespace + " is not found!");
    }
    return _routingDataWriterMap.get(namespace).deleteShardingKey(realm, shardingKey);
  }

  /**
   * Callback for updating the cached routing data.
   * Note: this method should not synchronize on the class or the map. We do not want namespaces
   * blocking each other.
   * Threadsafe map is used for _realmToShardingKeysMap.
   * The global consistency of the in-memory routing data is not a requirement (eventual consistency
   * is enough).
   * @param namespace
   */
  @Override
  public void refreshRoutingData(String namespace) {
    // Safe to ignore the callback if any of the maps are null.
    // If routingDataMap is null, then it will be populated by the constructor anyway
    // If routingDataMap is not null, then it's safe for the callback function to update it
    if (_routingZkAddressMap == null || _routingDataMap == null || _realmToShardingKeysMap == null
        || _routingDataReaderMap == null || _routingDataWriterMap == null) {
      LOG.warn(
          "refreshRoutingData callback called before ZKMetadataStoreDirectory was fully initialized. Skipping refresh!");
      return;
    }

    // Check if namespace exists; otherwise, return as a NOP and log it
    if (!_routingZkAddressMap.containsKey(namespace)) {
      LOG.error(
          "Failed to refresh internally-cached routing data! Namespace not found: " + namespace);
    }

    try {
      Map<String, List<String>> rawRoutingData =
          _routingDataReaderMap.get(namespace).getRoutingData();
      _realmToShardingKeysMap.put(namespace, rawRoutingData);

      MetadataStoreRoutingData routingData = new TrieRoutingData(rawRoutingData);
      _routingDataMap.put(namespace, routingData);
    } catch (InvalidRoutingDataException e) {
      LOG.error("Failed to refresh cached routing data for namespace {}", namespace, e);
    }

  }

  @Override
  public synchronized void close() {
    _routingDataReaderMap.values().forEach(MetadataStoreRoutingDataReader::close);
    _routingDataWriterMap.values().forEach(MetadataStoreRoutingDataWriter::close);
  }
}
