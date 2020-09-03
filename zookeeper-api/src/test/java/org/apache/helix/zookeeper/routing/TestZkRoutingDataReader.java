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

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.zookeeper.constant.TestConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZkRoutingDataReader extends ZkTestBase {
  private static final String ROUTING_ZK_ADDR = "localhost:2358";
  private ZkServer _routingZk;

  @BeforeClass
  public void beforeClass() {
    // Start a separate ZK for isolation
    _routingZk = startZkServer(ROUTING_ZK_ADDR);

    // Create ZK realm routing data ZNRecord
    ZNRecord znRecord = new ZNRecord(ROUTING_ZK_ADDR);
    znRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
        TestConstants.TEST_KEY_LIST_1);

    // Write raw routing data to ZK
    ZkClient zkClient = _routingZk.getZkClient();
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, null, CreateMode.PERSISTENT);
    zkClient
        .create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + ROUTING_ZK_ADDR, znRecord,
            CreateMode.PERSISTENT);
  }

  @AfterClass
  public void afterClass() {
    _routingZk.shutdown();
  }

  @Test
  public void testGetRawRoutingData() {
    Map<String, List<String>> rawRoutingData =
        new ZkRoutingDataReader().getRawRoutingData(ROUTING_ZK_ADDR);

    // Check that the returned content matches up with what we expect (1 realm, 3 keys)
    Assert.assertEquals(rawRoutingData.size(), 1);
    Assert.assertTrue(rawRoutingData.containsKey(ROUTING_ZK_ADDR));
    Assert.assertEquals(rawRoutingData.get(ROUTING_ZK_ADDR), TestConstants.TEST_KEY_LIST_1);
  }
}
