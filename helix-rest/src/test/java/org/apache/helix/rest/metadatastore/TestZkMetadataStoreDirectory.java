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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.apache.helix.rest.metadatastore.constant.MetadataStoreRoutingConstants;
import org.apache.helix.rest.metadatastore.exceptions.InvalidRoutingDataException;
import org.apache.helix.rest.server.AbstractTestClass;
import org.testng.annotations.BeforeClass;


public class TestZkMetadataStoreDirectory extends AbstractTestClass {
  private List<String> _zkList;
  private Map<String, String> _routingZkAddrMap;
  private MetadataStoreDirectory _metadataStoreDirectory;

  @BeforeClass
  public void beforeClass()
      throws InvalidRoutingDataException {
    _zkList = new ArrayList<>(_zkServerMap.keySet());

    // Populate routingZkAddrMap
    int namespaceIndex = 0;
    String namespacePrefix = "namespace_";
    for (String zk : _zkList) {
      _routingZkAddrMap.put(namespacePrefix + namespaceIndex, zk);
    }

    // Create metadataStoreDirectory
    _metadataStoreDirectory = new ZkMetadataStoreDirectory(_routingZkAddrMap);

    // Write dummy mappings in ZK
    // Create a node that represents a realm address and add 3 sharding keys to it
    ZNRecord testRoutingInfo = new ZNRecord("testRoutingInfo");
    List<String> testShardingKeys =
        Arrays.asList("/sharding/key/1/a", "/sharding/key/1/b", "/sharding/key/1/c");
    testRoutingInfo
        .setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY, testShardingKeys);
    _zkList.forEach(zk -> )
  }
}
