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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.rest.metadatastore.exceptions.InvalidRoutingDataException;
import org.apache.helix.rest.server.AbstractTestClass;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.helix.rest.metadatastore.ZkRoutingDataReader.ROUTING_DATA_PATH;
import static org.apache.helix.rest.metadatastore.ZkRoutingDataReader.ZNRECORD_LIST_FIELD_KEY;

public class TestZkRoutingDataReader extends AbstractTestClass {
  @Test
  public void testGetRoutingData() {
    // Create a node that represents a realm address and add 3 sharding keys to it
    ZNRecord testZnRecord1 = new ZNRecord("testZnRecord1");
    List<String> testShardingKeys1 =
        Arrays.asList("/sharding/key/1/a", "/sharding/key/1/b", "/sharding/key/1/c");
    testZnRecord1.setListField(ZNRECORD_LIST_FIELD_KEY, testShardingKeys1);

    // Create another node that represents a realm address and add 3 sharding keys to it
    ZNRecord testZnRecord2 = new ZNRecord("testZnRecord2");
    List<String> testShardingKeys2 = Arrays.asList("/sharding/key/2/a", "/sharding/key/2/b",
        "/sharding/key/2/c", "/sharding/key/2/d");
    testZnRecord2.setListField(ZNRECORD_LIST_FIELD_KEY, testShardingKeys2);

    // Add both nodes as children nodes to ROUTING_DATA_PATH
    _baseAccessor.create(ROUTING_DATA_PATH + "/testRealmAddress1", testZnRecord1,
        AccessOption.PERSISTENT);
    _baseAccessor.create(ROUTING_DATA_PATH + "/testRealmAddress2", testZnRecord2,
        AccessOption.PERSISTENT);

    MetadataStoreRoutingDataReader zkRoutingDataReader = new ZkRoutingDataReader(ZK_ADDR);
    try {
      Map<String, List<String>> routingData = zkRoutingDataReader.getRoutingData();
      Assert.assertEquals(routingData.size(), 2);
      Assert.assertEquals(routingData.get("testRealmAddress1"), testShardingKeys1);
      Assert.assertEquals(routingData.get("testRealmAddress2"), testShardingKeys2);
    } catch (InvalidRoutingDataException e) {
      Assert.fail("Not expecting InvalidRoutingDataException");
    }

    _baseAccessor.remove(ROUTING_DATA_PATH, AccessOption.PERSISTENT);
  }

  @Test
  public void testGetRoutingDataMissingMSRD() {
    MetadataStoreRoutingDataReader zkRoutingDataReader = new ZkRoutingDataReader(ZK_ADDR);
    try {
      zkRoutingDataReader.getRoutingData();
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(e.getMessage().contains("Routing data directory node " + ROUTING_DATA_PATH
          + " does not exist. Routing ZooKeeper address: " + ZK_ADDR));
    }
  }

  @Test
  public void testGetRoutingDataMissingMSRDChildren() {
    _baseAccessor.create(ROUTING_DATA_PATH, new ZNRecord("test"), AccessOption.PERSISTENT);
    MetadataStoreRoutingDataReader zkRoutingDataReader = new ZkRoutingDataReader(ZK_ADDR);
    try {
      zkRoutingDataReader.getRoutingData();
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(e.getMessage().contains("Routing data directory node " + ROUTING_DATA_PATH
          + " does not have any child node. Routing ZooKeeper address: " + ZK_ADDR));
    }

    _baseAccessor.remove(ROUTING_DATA_PATH, AccessOption.PERSISTENT);
  }

  @Test
  public void testGetRoutingDataMSRDChildEmptyValue() {
    ZNRecord testZnRecord1 = new ZNRecord("testZnRecord1");
    testZnRecord1.setListField(ZNRECORD_LIST_FIELD_KEY, Collections.emptyList());
    _baseAccessor.create(ROUTING_DATA_PATH + "/testRealmAddress1", testZnRecord1,
        AccessOption.PERSISTENT);
    MetadataStoreRoutingDataReader zkRoutingDataReader = new ZkRoutingDataReader(ZK_ADDR);
    try {
      zkRoutingDataReader.getRoutingData();
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(e.getMessage()
          .contains("Realm address node " + ROUTING_DATA_PATH
              + "/testRealmAddress1 does not have a value for key " + ZNRECORD_LIST_FIELD_KEY
              + ". Routing ZooKeeper address: " + ZK_ADDR));
    }

    _baseAccessor.remove(ROUTING_DATA_PATH, AccessOption.PERSISTENT);
  }
}
