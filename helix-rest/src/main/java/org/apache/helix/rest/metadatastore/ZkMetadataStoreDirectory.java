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

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient.ZkClientConfig;
import org.apache.helix.manager.zk.zookeeper.IZkStateListener;
import org.apache.zookeeper.Watcher;


public class ZkMetadataStoreDirectory implements MetadataStoreDirectory {
  // TODO: enable the line below when implementation is complete
//  private final MetadataStoreRoutingDataAccessor _routingDataAccessor;
  private MetadataStoreRoutingData _routingData;

  public ZkMetadataStoreDirectory(String zkAddress) {
//    _routingDataAccessor = new ZkRoutingDataAccessor(zkAddress, this); // dependency injection
    // 1. Create a RoutingDataTrieNode using routingDataAccessor
    // _routingData = constructRoutingData(_routingDataAccessor);
  }

  @Override
  public List<String> getAllNamespaces() {
    return null;
  }

  @Override
  public List<String> getAllMetadataStoreRealms(String namespace) {
    return null;
  }

  @Override
  public List<String> getAllShardingKeys(String namespace) {
    return null;
  }

  @Override
  public List<String> getAllShardingKeysInRealm(String namespace, String realm) {
    return null;
  }

  @Override
  public Map<String, String> getAllShardingKeysUnderPath(String namespace, String path) {
    return null;
  }

  @Override
  public String getMetadataStoreRealm(String namespace, String shardingKey) {
    return null;
  }

  @Override
  public boolean addNamespace(String namespace) {
    return false;
  }

  @Override
  public boolean deleteNamespace(String namespace) {
    return false;
  }

  @Override
  public boolean addMetadataStoreRealm(String namespace, String realm) {
    return false;
  }

  @Override
  public boolean deleteMetadataStoreRealm(String namespace, String realm) {
    return false;
  }

  @Override
  public boolean addShardingKey(String namespace, String realm, String shardingKey) {
    return false;
  }

  @Override
  public boolean deleteShardingKey(String namespace, String realm, String shardingKey) {
    return false;
  }

  @Override
  public void updateRoutingData() {
    // call constructRoutingData() here.
  }

//  /**
//   * Reconstructs MetadataStoreRoutingData by reading from the metadata store.
//   * @param routingDataAccessor
//   * @return
//   */
//  private MetadataStoreRoutingData constructRoutingData(MetadataStoreRoutingDataAccessor routingDataAccessor) {
//    /**
//     * Trie construction logic
//     */
//
//    // 1. Construct
//    // 2. Update the in-memory reference when complete
//    _routingData = newRoutingData;
//  }
}
