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
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.rest.server.AbstractTestClass;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkRoutingDataAccessor extends AbstractTestClass {
  @Test
  public void testGetRoutingData() {
    ZNRecord testZnRecord1 = new ZNRecord("testZnRecord1");
    List<String> testShardingKeys1 = Arrays.asList("/sharding/key/1/a", "/sharding/key/1/b", "/sharding/key/1/c");
    testZnRecord1.setListField("ZK_PATH_SHARDING_KEYS", testShardingKeys1);
    ZNRecord testZnRecord2 = new ZNRecord("testZnRecord2");
    List<String> testShardingKeys2 = Arrays.asList("/sharding/key/2/a", "/sharding/key/2/b", "/sharding/key/2/c", "/sharding/key/2/d");
    testZnRecord2.setListField("ZK_PATH_SHARDING_KEYS", testShardingKeys2);
    ZNRecord testZnRecord3 = new ZNRecord("testZnRecord3");
    _baseAccessor.create("/METADATA_STORE_ROUTING_DATA/testRealmAddress1", testZnRecord1, AccessOption.PERSISTENT);
    _baseAccessor.create("/METADATA_STORE_ROUTING_DATA/testRealmAddress2", testZnRecord2, AccessOption.PERSISTENT);
    _baseAccessor.create("/METADATA_STORE_ROUTING_DATA/testRealmAddress3", testZnRecord3, AccessOption.PERSISTENT);

    ZkRoutingDataAccessor zkRoutingDataAccessor = new ZkRoutingDataAccessor(ZK_ADDR);
    Map<String, List<String>> routingData = zkRoutingDataAccessor.getRoutingData();
    Assert.assertEquals(routingData.size(), 2);
    Assert.assertEquals(routingData.get("testRealmAddress1"), testShardingKeys1);
    Assert.assertEquals(routingData.get("testRealmAddress2"), testShardingKeys2);
  }
}
