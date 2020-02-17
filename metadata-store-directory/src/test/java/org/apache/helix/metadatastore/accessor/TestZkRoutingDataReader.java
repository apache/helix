package org.apache.helix.metadatastore.accessor;

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

import org.apache.helix.metadatastore.AbstractTestClass;
import org.apache.helix.metadatastore.TestHelper;
import org.apache.helix.metadatastore.constant.MetadataStoreRoutingConstants;
import org.apache.helix.metadatastore.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZkRoutingDataReader extends AbstractTestClass {
  private static final String DUMMY_NAMESPACE = "NAMESPACE";
  private MetadataStoreRoutingDataReader _zkRoutingDataReader;

  @BeforeClass
  public void beforeClass() throws Exception {
    deleteRoutingDataPath();
    _zkRoutingDataReader = new ZkRoutingDataReader(DUMMY_NAMESPACE, ZK_ADDR, null);

    // Set the ZkSerializer
    _gZkClient.setZkSerializer(new ZNRecordSerializer());
  }

  @AfterClass
  public void afterClass() throws Exception {
    _zkRoutingDataReader.close();
    deleteRoutingDataPath();
  }

  @AfterMethod
  public void afterMethod() {
    _gZkClient.deleteRecursively(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);
  }

  @Test
  public void testGetRoutingData() {
    try {
      _gZkClient.createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, true);
    } catch (ZkException e) {
      // Okay if it exists already
    }

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
    _gZkClient.create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/testRealmAddress1",
        testZnRecord1, CreateMode.PERSISTENT);
    _gZkClient.create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/testRealmAddress2",
        testZnRecord2, CreateMode.PERSISTENT);

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
  public void testGetRoutingDataMissingMSRD() {
    try {
      _zkRoutingDataReader.getRoutingData();
      Assert.fail("Expecting InvalidRoutingDataException");
    } catch (InvalidRoutingDataException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Routing data directory ZNode " + MetadataStoreRoutingConstants.ROUTING_DATA_PATH
              + " does not exist. Routing ZooKeeper address: " + ZK_ADDR));
    }
  }

  @Test(dependsOnMethods = "testGetRoutingDataMissingMSRD")
  public void testGetRoutingDataMissingMSRDChildren() {
    _gZkClient.create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, new ZNRecord("test"),
        CreateMode.PERSISTENT);
    try {
      Map<String, List<String>> routingData = _zkRoutingDataReader.getRoutingData();
      Assert.assertEquals(routingData.size(), 0);
    } catch (InvalidRoutingDataException e) {
      Assert.fail("Not expecting InvalidRoutingDataException");
    }
  }

  @Test(dependsOnMethods = "testGetRoutingDataMissingMSRDChildren")
  public void testGetRoutingDataMSRDChildEmptyValue() {
    ZNRecord testZnRecord1 = new ZNRecord("testZnRecord1");
    testZnRecord1.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
        Collections.emptyList());
    try {
      _gZkClient.createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, true);
    } catch (ZkException e) {
      // Okay if it exists already
    }
    _gZkClient.create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/testRealmAddress1",
        testZnRecord1, CreateMode.PERSISTENT);
    try {
      Map<String, List<String>> routingData = _zkRoutingDataReader.getRoutingData();
      Assert.assertEquals(routingData.size(), 1);
      Assert.assertTrue(routingData.get("testRealmAddress1").isEmpty());
    } catch (InvalidRoutingDataException e) {
      Assert.fail("Not expecting InvalidRoutingDataException");
    }
  }

  private void deleteRoutingDataPath() throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      ZK_SERVER_MAP.get(ZK_ADDR).getZkClient()
          .deleteRecursively(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);

      if (ZK_SERVER_MAP.get(ZK_ADDR).getZkClient()
          .exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
        return false;
      }

      return true;
    }, TestHelper.WAIT_DURATION), "Routing data path should be deleted after the tests.");
  }
}
