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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.rest.metadatastore.exceptions.InvalidRoutingDataException;

public class ZkRoutingDataReader implements MetadataStoreRoutingDataReader {
  static final String ROUTING_DATA_PATH = "/METADATA_STORE_ROUTING_DATA";
  static final String ZNRECORD_LIST_FIELD_KEY = "ZK_PATH_SHARDING_KEYS";

  private final String _zkAddress;
  private final HelixZkClient _zkClient;

  public ZkRoutingDataReader(String zkAddress) {
    _zkAddress = zkAddress;
    _zkClient = DedicatedZkClientFactory.getInstance().buildZkClient(
        new HelixZkClient.ZkConnectionConfig(zkAddress),
        new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));
  }

  public Map<String, List<String>> getRoutingData() throws InvalidRoutingDataException {
    Map<String, List<String>> result = new HashMap<>();
    if (!_zkClient.exists(ROUTING_DATA_PATH)) {
      throw new InvalidRoutingDataException("Routing data directory node " + ROUTING_DATA_PATH
          + " does not exist. Routing ZooKeeper address: " + _zkAddress);
    }
    List<String> children = _zkClient.getChildren(ROUTING_DATA_PATH);
    if (children.isEmpty()) {
      throw new InvalidRoutingDataException("Routing data directory node " + ROUTING_DATA_PATH
          + " does not have any child node. Routing ZooKeeper address: " + _zkAddress);
    }
    for (String child : children) {
      ZNRecord record = _zkClient.readData(ROUTING_DATA_PATH + "/" + child);
      List<String> shardingKeys = record.getListField(ZNRECORD_LIST_FIELD_KEY);
      if (shardingKeys == null || shardingKeys.isEmpty()) {
        throw new InvalidRoutingDataException("Realm address node " + ROUTING_DATA_PATH + "/"
            + child + " does not have a value for key " + ZNRECORD_LIST_FIELD_KEY
            + ". Routing ZooKeeper address: " + _zkAddress);
      }
      result.put(child, shardingKeys);
    }
    return result;
  }
}
