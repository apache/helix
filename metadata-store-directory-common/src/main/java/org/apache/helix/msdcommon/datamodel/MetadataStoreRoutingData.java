package org.apache.helix.msdcommon.datamodel;

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

import java.util.Map;
import java.util.NoSuchElementException;


public interface MetadataStoreRoutingData {
  /**
   * Given a path, return all the "metadata store sharding key-metadata store realm address" pairs
   * where the sharding keys contain the given path. For example, given "/a/b", return {"/a/b/c":
   * "realm.address.c.com:1234", "/a/b/d": "realm.address.d.com:1234"} where "a/b/c" and "a/b/d" are
   * sharding keys and the urls are realm addresses. If the path is invalid, returns an empty
   * mapping.
   * @param path - the path where the search is conducted
   * @return all "sharding key-realm address" pairs where the sharding keys contain the given
   *         path if the path is valid; empty mapping otherwise
   * @throws IllegalArgumentException - when the path is invalid
   */
  Map<String, String> getAllMappingUnderPath(String path) throws IllegalArgumentException;

  /**
   * Given a path, return the realm address corresponding to the sharding key contained in the
   * path. If the path doesn't contain a sharding key, throw NoSuchElementException.
   * @param path - the path where the search is conducted
   * @return the realm address corresponding to the sharding key contained in the path
   * @throws IllegalArgumentException - when the path is invalid
   * @throws NoSuchElementException - when the path doesn't contain a sharding key
   */
  String getMetadataStoreRealm(String path) throws IllegalArgumentException, NoSuchElementException;

  /**
   * Given a path, return the sharding key contained in the path. If the path doesn't contain a
   * sharding key, throw NoSuchElementException.
   * @param path - the path that may contain a sharding key
   * @return the sharding key contained in the path
   * @throws IllegalArgumentException - when the path is invalid
   * @throws NoSuchElementException - when the path doesn't contain a sharding key
   */
  String getShardingKeyInPath(String path) throws IllegalArgumentException, NoSuchElementException;

  /**
   * Check if the provided sharding key can be inserted to the routing data. The insertion is
   * invalid if: 1. the sharding key is a parent key to an existing sharding key; 2. the sharding
   * key has a parent key that is an existing sharding key; 3. the sharding key already exists. In
   * any of these cases, inserting the sharding key will cause ambiguity among 2 sharding keys,
   * rendering the routing data invalid.
   * @param shardingKey - the sharding key to be inserted
   * @return true if the sharding key could be inserted, false otherwise
   */
  boolean isShardingKeyInsertionValid(String shardingKey);

  /**
   * Check if the provided sharding key and realm address pair exists in the routing data.
   * @param shardingKey - the sharding key checked
   * @param realmAddress - the realm address corresponding to the key
   * @return true if the sharding key and realm address pair exist in the routing data
   */
  boolean containsKeyRealmPair(String shardingKey, String realmAddress);
}