package org.apache.helix.zookeeper.api.factory;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;


/**
 * Creates an instance of RealmAwareZkClient.
 */
public interface RealmAwareZkClientFactory {
  /**
   * Build a RealmAwareZkClient using specified connection config and client config.
   * @param connectionConfig
   * @param clientConfig
   * @param metadataStoreRoutingData
   * @return HelixZkClient
   */
  // TODO: remove MetadataStoreRoutingData
  RealmAwareZkClient buildZkClient(RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig,
      MetadataStoreRoutingData metadataStoreRoutingData);

  /**
   * Builds a RealmAwareZkClient using specified connection config and default client config.
   * @param connectionConfig
   * @param metadataStoreRoutingData
   * @return RealmAwareZkClient
   */
  // TODO: remove MetadataStoreRoutingData
  default RealmAwareZkClient buildZkClient(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      MetadataStoreRoutingData metadataStoreRoutingData) {
    return buildZkClient(connectionConfig, new RealmAwareZkClient.RealmAwareZkClientConfig(),
        metadataStoreRoutingData);
  }
}
