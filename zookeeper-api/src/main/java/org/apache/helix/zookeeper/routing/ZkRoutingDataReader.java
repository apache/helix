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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.exception.MultiZkException;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;


/**
 * Zk-based RoutingDataReader that establishes a ZK connection to the routing ZK to fetch routing
 * data.
 * The reading of routing data by nature should only be performed in cases of a Helix client
 * initialization or routing data reset. That means we do not have to maintain an active ZK
 * connection. To minimize the number of client-side ZK connections, ZkRoutingDataReader establishes
 * a ZK session temporarily only to read from ZK afresh and closes sessions upon read completion.
 */
public class ZkRoutingDataReader implements RoutingDataReader {

  /**
   * Returns a map form of metadata store routing data.
   * The map fields stand for metadata store realm address (key), and a corresponding list of ZK
   * path sharding keys (key).
   * @param endpoint
   * @return
   */
  @Override
  public Map<String, List<String>> getRawRoutingData(String endpoint) {
    ZkClient zkClient =
        new ZkClient.Builder().setZkServer(endpoint).setZkSerializer(new ZNRecordSerializer())
            .build();

    Map<String, List<String>> routingData = new HashMap<>();
    List<String> allRealmAddresses;
    try {
      allRealmAddresses = zkClient.getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);
    } catch (ZkNoNodeException e) {
      throw new MultiZkException(
          "Routing data directory ZNode " + MetadataStoreRoutingConstants.ROUTING_DATA_PATH
              + " does not exist. Routing ZooKeeper address: " + endpoint);
    }
    if (allRealmAddresses != null) {
      for (String realmAddress : allRealmAddresses) {
        ZNRecord record = zkClient
            .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realmAddress, true);
        if (record != null) {
          List<String> shardingKeys =
              record.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY);
          routingData
              .put(realmAddress, shardingKeys != null ? shardingKeys : Collections.emptyList());
        }
      }
    }

    zkClient.close();
    return routingData;
  }
}
