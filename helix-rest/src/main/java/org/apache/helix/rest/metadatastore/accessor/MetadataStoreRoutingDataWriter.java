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

import java.util.List;
import java.util.Map;


/**
 * An interface for a DAO that writes to the metadata store that stores routing data.
 * Note: Each data writer connects to a single namespace.
 */
public interface MetadataStoreRoutingDataWriter {

  /**
   * Creates a realm. If the namespace does not exist, it creates one.
   * @param realm
   * @return true if successful or if the realm already exists. false otherwise.
   */
  boolean addMetadataStoreRealm(String realm);

  /**
   * Deletes a realm.
   * @param realm
   * @return true if successful or the realm or namespace does not exist. false otherwise.
   */
  boolean deleteMetadataStoreRealm(String realm);

  /**
   * Creates a mapping between the sharding key to the realm. If realm doesn't exist, it will be created (this call is idempotent).
   * @param realm
   * @param shardingKey
   * @return false if failed
   */
  boolean addShardingKey(String realm, String shardingKey);

  /**
   * Deletes the mapping between the sharding key to the realm.
   * @param realm
   * @param shardingKey
   * @return false if failed; true if the deletion is successful or the key does not exist.
   */
  boolean deleteShardingKey(String realm, String shardingKey);

  /**
   * Sets (overwrites) the routing data with the given <realm, list of sharding keys> mapping.
   * WARNING: This overwrites all existing routing data. Use with care!
   * @param routingData
   * @return true if successful; false otherwise.
   */
  boolean setRoutingData(Map<String, List<String>> routingData);

  /**
   * Closes any stateful resources such as connections or threads.
   */
  void close();
}
