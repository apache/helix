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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * MetadataStoreDirectory interface that provides methods that are used to route requests to appropriate metadata store realm.
 *
 * namespace: tied to a namespace used in Helix REST (Metadata Store Directory Service endpoints will be served by Helix REST deployables)
 * realm: a metadata store deployable/ensemble. for example, if an application wishes to use 3 ZK quorums, then each ZK quorum would be considered a realm (ZK realm)
 * metadata store path sharding key: assuming the metadata store uses a file system APIs, this sharding key denotes the key that maps to a particular metadata store realm. an example of a key is a cluster name mapping to a particular ZK realm (ZK address)
 */
public interface MetadataStoreDirectory extends AutoCloseable {

  /**
   * Retrieves all existing namespaces in the routing metadata store.
   * @return
   */
  Collection<String> getAllNamespaces();

  /**
   * Returns all metadata store realms in the given namespace.
   * @return
   */
  Collection<String> getAllMetadataStoreRealms(String namespace);

  /**
   * Returns all path-based sharding keys in the given namespace.
   * @return
   */
  Collection<String> getAllShardingKeys(String namespace);

  /**
   * Returns routing data in the given namespace.
   *
   * @param namespace namespace in metadata store directory.
   * @return Routing data map: realm -> List of sharding keys
   */
  Map<String, List<String>> getNamespaceRoutingData(String namespace);

  /**
   * Sets and overwrites routing data in the given namespace.
   *
   * @param namespace namespace in metadata store directory.
   * @param routingData Routing data map: realm -> List of sharding keys
   * @return true if successful; false otherwise.
   */
  boolean setNamespaceRoutingData(String namespace, Map<String, List<String>> routingData);

  /**
   * Returns all path-based sharding keys in the given namespace and the realm.
   * @param namespace
   * @param realm
   * @return
   */
  Collection<String> getAllShardingKeysInRealm(String namespace, String realm);

  /**
   * Returns all sharding keys that have the given path as the prefix substring.
   * E.g) Given that there are sharding keys: /a/b/c, /a/b/d, /a/e,
   * getAllShardingKeysUnderPath(namespace, "/a/b") returns ["/a/b/c": "realm", "/a/b/d": "realm].
   * @param namespace
   * @param path
   * @return
   */
  Map<String, String> getAllMappingUnderPath(String namespace, String path);

  /**
   * Returns the name of the metadata store realm based on the namespace and the sharding key given.
   * @param namespace
   * @param shardingKey
   * @return
   */
  String getMetadataStoreRealm(String namespace, String shardingKey)
      throws NoSuchElementException;

  /**
   * Creates a realm. If the namespace does not exist, it creates one.
   * @param namespace
   * @param realm
   * @return true if successful or if the realm already exists. false otherwise.
   */
  boolean addMetadataStoreRealm(String namespace, String realm);

  /**
   * Deletes a realm.
   * @param namespace
   * @param realm
   * @return true if successful or the realm or namespace does not exist. false otherwise.
   */
  boolean deleteMetadataStoreRealm(String namespace, String realm);

  /**
   * Creates a mapping between the sharding key to the realm in the given namespace.
   * @param namespace
   * @param realm
   * @param shardingKey
   * @return false if failed
   */
  boolean addShardingKey(String namespace, String realm, String shardingKey);

  /**
   * Deletes the mapping between the sharding key to the realm in the given namespace.
   * @param namespace
   * @param realm
   * @param shardingKey
   * @return false if failed; true if the deletion is successful or the key does not exist.
   */
  boolean deleteShardingKey(String namespace, String realm, String shardingKey);

  /**
   * Close MetadataStoreDirectory.
   */
  void close();
}
