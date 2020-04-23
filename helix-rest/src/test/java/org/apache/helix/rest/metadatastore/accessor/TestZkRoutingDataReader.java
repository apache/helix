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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.helix.TestHelper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.server.AbstractTestClass;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZkRoutingDataReader extends AbstractTestClass {
  private MetadataStoreRoutingDataReader _zkRoutingDataReader;

  @BeforeClass
  public void beforeClass() throws Exception {
    _zkRoutingDataReader = new ZkRoutingDataReader(TEST_NAMESPACE, _zkAddrTestNS, null);
    clearRoutingDataPath();
  }

  @AfterClass
  public void afterClass() throws Exception {
    _zkRoutingDataReader.close();
    clearRoutingDataPath();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    clearRoutingDataPath();
  }

  @Test
  public void testGetRoutingData() {
    // Create a node that represents a realm address and add 3 sharding keys to it
    ZNRecord testZnRecord1 = new ZNRecord("testZnRecord1");
    List<String> testShardingKeys1 =
        Arrays.asList("/sharding/key/1/a", "/sharding/key/1/b", "/sharding/key/1/c");
    testZnRecord1
        .setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY, testShardingKeys1);

    // Create another node that represents a realm address and add 3 sharding keys to it
    ZNRecord testZnRecord2 = new ZNRecord("testZnRecord2");
    List<String> testShardingKeys2 = Arrays
        .asList("/sharding/key/2/a", "/sharding/key/2/b", "/sharding/key/2/c", "/sharding/key/2/d");
    testZnRecord2
        .setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY, testShardingKeys2);

    // Add both nodes as children nodes to ZkRoutingDataReader.ROUTING_DATA_PATH
    _gZkClientTestNS
        .createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/testRealmAddress1",
            testZnRecord1);

    _gZkClientTestNS
        .createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/testRealmAddress2",
            testZnRecord2);

    try {
      Map<String, List<String>> routingData = _zkRoutingDataReader.getRoutingData();
      Assert.assertEquals(routingData.size(), 2);
      Assert.assertEquals(routingData.get("testRealmAddress1"), testShardingKeys1);
      Assert.assertEquals(routingData.get("testRealmAddress2"), testShardingKeys2);
    } catch (InvalidRoutingDataException e) {
      Assert.fail("Not expecting InvalidRoutingDataException");
    }
  }

  @Test(dependsOnMethods = "testGetRoutingData")
  public void testGetRoutingDataMissingMSRDChildren() {
    try {
      Map<String, List<String>> routingData = _zkRoutingDataReader.getRoutingData();
      Assert.assertEquals(routingData.size(), 0);
    } catch (InvalidRoutingDataException e) {
      Assert.fail("Not expecting InvalidRoutingDataException");
    }
  }

  @Test(dependsOnMethods = "testGetRoutingData")
  public void testGetRoutingDataMSRDChildEmptyValue() {
    ZNRecord testZnRecord1 = new ZNRecord("testZnRecord1");
    testZnRecord1.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
        Collections.emptyList());
    _gZkClientTestNS
        .createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/testRealmAddress1",
            testZnRecord1);

    try {
      Map<String, List<String>> routingData = _zkRoutingDataReader.getRoutingData();
      Assert.assertEquals(routingData.size(), 1);
      Assert.assertTrue(routingData.get("testRealmAddress1").isEmpty());
    } catch (InvalidRoutingDataException e) {
      Assert.fail("Not expecting InvalidRoutingDataException");
    }
  }

  private void clearRoutingDataPath() throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      for (String zkRealm : _gZkClientTestNS
          .getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
        _gZkClientTestNS.delete(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm);
      }

      return _gZkClientTestNS.getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH).isEmpty();
    }, TestHelper.WAIT_DURATION), "Routing data path should be deleted after the tests.");
  }
}
