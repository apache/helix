package org.apache.helix.msdserver.accessor;

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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.msdserver.AbstractTestClass;
import org.apache.helix.msdserver.TestHelper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZkRoutingDataWriter extends AbstractTestClass {
  private static final String DUMMY_NAMESPACE = "NAMESPACE";
  private static final String DUMMY_REALM = "REALM";
  private static final String DUMMY_SHARDING_KEY = "SHARDING_KEY";
  private static final String DUMMY_REALM_PATH =
      MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM;
  private MetadataStoreRoutingDataWriter _zkRoutingDataWriter;

  @BeforeClass
  public void beforeClass() {
    _zkRoutingDataWriter = new ZkRoutingDataWriter(DUMMY_NAMESPACE, ZK_ADDR);

    // Set ZNRecordSerializer
    _gZkClient.setZkSerializer(new ZNRecordSerializer());

    // Create the root path so that we do not get ZkNoNodeException for ZK writes
    try {
      _gZkClient.createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, true);
    } catch (ZkException e) {
      // Okay if it exists already
    }
  }

  @AfterClass
  public void afterClass() throws Exception {
    // Clean up the root path
    Assert.assertTrue(TestHelper.verify(() -> {
      _gZkClient.deleteRecursively(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);
      return !_gZkClient.exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);
    }, TestHelper.WAIT_DURATION));

    _zkRoutingDataWriter.close();
  }

  @Test
  public void testAddMetadataStoreRealm() {
    _zkRoutingDataWriter.addMetadataStoreRealm(DUMMY_REALM);
    ZNRecord znRecord = _gZkClient.readData(DUMMY_REALM_PATH);
    Assert.assertNotNull(znRecord);
  }

  @Test(dependsOnMethods = "testAddMetadataStoreRealm")
  public void testDeleteMetadataStoreRealm() {
    _zkRoutingDataWriter.deleteMetadataStoreRealm(DUMMY_REALM);
    Assert.assertFalse(_gZkClient.exists(DUMMY_REALM_PATH));
  }

  @Test(dependsOnMethods = "testDeleteMetadataStoreRealm")
  public void testAddShardingKey() {
    _zkRoutingDataWriter.addShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    ZNRecord znRecord = _gZkClient.readData(DUMMY_REALM_PATH);
    Assert.assertNotNull(znRecord);
    Assert.assertTrue(znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(DUMMY_SHARDING_KEY));
  }

  @Test(dependsOnMethods = "testAddShardingKey")
  public void testDeleteShardingKey() {
    _zkRoutingDataWriter.deleteShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    ZNRecord znRecord = _gZkClient.readData(DUMMY_REALM_PATH);
    Assert.assertNotNull(znRecord);
    Assert.assertFalse(znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(DUMMY_SHARDING_KEY));
  }

  @Test(dependsOnMethods = "testDeleteShardingKey")
  public void testSetRoutingData() {
    Map<String, List<String>> testRoutingDataMap =
        ImmutableMap.of(DUMMY_REALM, Collections.singletonList(DUMMY_SHARDING_KEY));
    _zkRoutingDataWriter.setRoutingData(testRoutingDataMap);
    ZNRecord znRecord = _gZkClient.readData(DUMMY_REALM_PATH);
    Assert.assertNotNull(znRecord);
    Assert.assertEquals(
        znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY).size(), 1);
    Assert.assertTrue(znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(DUMMY_SHARDING_KEY));
  }
}
