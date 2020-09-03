package org.apache.helix.zookeeper.routing;

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

import java.util.List;
import java.util.Map;

import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.datamodel.TrieRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.exception.MultiZkException;


public interface RoutingDataReader {
  /**
   * Returns an object form of metadata store routing data.
   * @param endpoint
   * @return
   */
  default MetadataStoreRoutingData getMetadataStoreRoutingData(String endpoint) {
    try {
      return new TrieRoutingData(getRawRoutingData(endpoint));
    } catch (InvalidRoutingDataException e) {
      throw new MultiZkException(e);
    }
  }

  /**
   * Returns a map form of metadata store routing data.
   * The map fields stand for metadata store realm address (key), and a corresponding list of ZK
   * path sharding keys (key).
   * @param endpoint
   * @return
   */
  Map<String, List<String>> getRawRoutingData(String endpoint);
}
