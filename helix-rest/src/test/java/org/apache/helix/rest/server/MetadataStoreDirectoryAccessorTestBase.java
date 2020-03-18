package org.apache.helix.rest.server;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.metadatastore.accessor.MetadataStoreRoutingDataReader;
import org.apache.helix.rest.metadatastore.accessor.ZkRoutingDataReader;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public class MetadataStoreDirectoryAccessorTestBase extends AbstractTestClass {
  /*
   * The following are constants to be used for testing.
   */
  protected static final String TEST_NAMESPACE_URI_PREFIX = "/namespaces/" + TEST_NAMESPACE;
  protected static final String NON_EXISTING_NAMESPACE_URI_PREFIX =
      "/namespaces/not-existed-namespace/metadata-store-realms/";
  protected static final String TEST_REALM_1 = "testRealm1";
  protected static final List<String> TEST_SHARDING_KEYS_1 =
      Arrays.asList("/sharding/key/1/a", "/sharding/key/1/b", "/sharding/key/1/c");
  protected static final String TEST_REALM_2 = "testRealm2";
  protected static final List<String> TEST_SHARDING_KEYS_2 =
      Arrays.asList("/sharding/key/1/d", "/sharding/key/1/e", "/sharding/key/1/f");
  protected static final String TEST_REALM_3 = "testRealm3";
  protected static final String TEST_SHARDING_KEY = "/sharding/key/1/x";
  protected static final String INVALID_TEST_SHARDING_KEY = "sharding/key/1/x";

  // List of all ZK addresses, each of which corresponds to a namespace/routing ZK
  protected List<String> _zkList;
  protected MetadataStoreRoutingDataReader _routingDataReader;

  @BeforeClass
  public void beforeClass() throws Exception {
    _zkList = new ArrayList<>(ZK_SERVER_MAP.keySet());

    clearRoutingData();

    // Write dummy mappings in ZK
    // Create a node that represents a realm address and add 3 sharding keys to it
    ZNRecord znRecord = new ZNRecord("RoutingInfo");

    _zkList.forEach(zk -> {
      ZK_SERVER_MAP.get(zk).getZkClient().setZkSerializer(new ZNRecordSerializer());
      // Write first realm and sharding keys pair
      znRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
          TEST_SHARDING_KEYS_1);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_1,
              true);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_1,
              znRecord);

      // Create another realm and sharding keys pair
      znRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
          TEST_SHARDING_KEYS_2);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_2,
              true);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_2,
              znRecord);
    });

    _routingDataReader = new ZkRoutingDataReader(TEST_NAMESPACE, _zkAddrTestNS, null);

    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY,
        getBaseUri().getHost());
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY,
        Integer.toString(getBaseUri().getPort()));
  }

  @AfterClass
  public void afterClass() throws Exception {
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY);
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY);
    _routingDataReader.close();
    clearRoutingData();
  }

  protected void clearRoutingData() throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      for (String zk : _zkList) {
        ZkClient zkClient = ZK_SERVER_MAP.get(zk).getZkClient();
        if (zkClient.exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
          for (String zkRealm : zkClient
              .getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
            zkClient.delete(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm);
          }
        }
      }

      for (String zk : _zkList) {
        ZkClient zkClient = ZK_SERVER_MAP.get(zk).getZkClient();
        if (zkClient.exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH) && !zkClient
            .getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH).isEmpty()) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION), "Routing data path should be deleted after the tests.");
  }

  // Uses routingDataReader to get the latest realms in test-namespace; returns a modifiable copy
  // because it'll be modified in test cases
  protected Set<String> getAllRealms() throws InvalidRoutingDataException {
    return new HashSet<>(_routingDataReader.getRoutingData().keySet());
  }

  // Uses routingDataReader to get the latest sharding keys in test-namespace, testRealm1
  protected Set<String> getAllShardingKeysInTestRealm1() throws InvalidRoutingDataException {
    return new HashSet<>(_routingDataReader.getRoutingData().get(TEST_REALM_1));
  }

  protected Map<String, List<String>> getRawRoutingData() throws InvalidRoutingDataException {
    return _routingDataReader.getRoutingData();
  }
}
