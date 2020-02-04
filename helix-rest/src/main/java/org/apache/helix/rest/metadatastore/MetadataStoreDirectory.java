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

import java.util.List;
import java.util.Map;


public interface MetadataStoreDirectory {

  /**
   * Retrieves all existing namespaces in the routing metadata store.
   * @return
   */
  List<String> getAllNamespaces();

  /**
   * Returns all metadata store realms in the given namespace.
   * @return
   */
  List<String> getAllMetadataStoreRealms(String namespace);

  /**
   * Returns all path-based sharding keys in the given namespace.
   * @return
   */
  List<String> getAllShardingKeys(String namespace);

  /**
   * Returns all path-based sharding keys in the given namespace and the realm.
   * @param namespace
   * @param realm
   * @return
   */
  List<String> getAllShardingKeysInRealm(String namespace, String realm);

  /**
   * Returns all sharding keys that have the given path as the prefix substring.
   * E.g) Given that there are sharding keys: /a/b/c, /a/b/d, /a/e,
   * getAllShardingKeysUnderPath(namespace, "/a/b") returns ["/a/b/c", "/a/b/d"].
   * @param namespace
   * @param path
   * @return
   */
  Map<String, String> getAllShardingKeysUnderPath(String namespace, String path);

  /**
   * Returns the name of the metadata store realm based on the namespace and the sharding key given.
   * @param namespace
   * @param shardingKey
   * @return
   */
  String getMetadataStoreRealm(String namespace, String shardingKey);

  /**
   * NOTE: The following CRUD methods are idempotent.
   */
  /**
   * Creates a namespace in the routing metadata store.
   * @param namespace
   * @return true if successful or if the namespace already exists. false if not successful.
   */
  boolean addNamespace(String namespace);

  /**
   * Deletes a namespace in the routing metadata store.
   * @param namespace
   * @return true if successful or if the namespace does not exist. false otherwise.
   */
  boolean deleteNamespace(String namespace);

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
   * Updates the internally-cached routing data if available.
   */
  void updateRoutingData();
}
