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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.server.AbstractTestClass;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZkMetadataStoreDirectory extends AbstractTestClass {
  /**
   * The following are constants to be used for testing.
   */
  private static final String TEST_REALM_1 = "testRealm1";
  private static final List<String> TEST_SHARDING_KEYS_1 =
      Arrays.asList("/sharding/key/1/a", "/sharding/key/1/b", "/sharding/key/1/c");
  private static final String TEST_REALM_2 = "testRealm2";
  private static final List<String> TEST_SHARDING_KEYS_2 =
      Arrays.asList("/sharding/key/1/d", "/sharding/key/1/e", "/sharding/key/1/f");
  private static final String TEST_REALM_3 = "testRealm3";
  private static final List<String> TEST_SHARDING_KEYS_3 =
      Arrays.asList("/sharding/key/1/x", "/sharding/key/1/y", "/sharding/key/1/z");

  // List of all ZK addresses, each of which corresponds to a namespace/routing ZK
  private List<String> _zkList;
  // <Namespace, ZkAddr> mapping
  private Map<String, String> _routingZkAddrMap;
  private MetadataStoreDirectory _metadataStoreDirectory;

  @BeforeClass
  public void beforeClass() throws Exception {
    _zkList = new ArrayList<>(ZK_SERVER_MAP.keySet());

    clearRoutingData();

    // Populate routingZkAddrMap
    _routingZkAddrMap = new LinkedHashMap<>();
    int namespaceIndex = 0;
    String namespacePrefix = "namespace_";
    for (String zk : _zkList) {
      _routingZkAddrMap.put(namespacePrefix + namespaceIndex, zk);
    }

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

    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY,
        getBaseUri().getHost());
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY,
        Integer.toString(getBaseUri().getPort()));

    // Create metadataStoreDirectory
    for (Map.Entry<String, String> entry : _routingZkAddrMap.entrySet()) {
      _metadataStoreDirectory =
          ZkMetadataStoreDirectory.getInstance(entry.getKey(), entry.getValue());
    }
  }

  @AfterClass
  public void afterClass() throws Exception {
    _metadataStoreDirectory.close();
    clearRoutingData();
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY);
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY);
  }

  @Test
  public void testGetAllNamespaces() {
    Assert.assertTrue(
        _metadataStoreDirectory.getAllNamespaces().containsAll(_routingZkAddrMap.keySet()));
  }

  @Test(dependsOnMethods = "testGetAllNamespaces")
  public void testGetAllMetadataStoreRealms() {
    Set<String> realms = new HashSet<>();
    realms.add(TEST_REALM_1);
    realms.add(TEST_REALM_2);

    for (String namespace : _routingZkAddrMap.keySet()) {
      Assert.assertEquals(_metadataStoreDirectory.getAllMetadataStoreRealms(namespace), realms);
    }
  }

  @Test(dependsOnMethods = "testGetAllMetadataStoreRealms")
  public void testGetAllShardingKeys() {
    Set<String> allShardingKeys = new HashSet<>();
    allShardingKeys.addAll(TEST_SHARDING_KEYS_1);
    allShardingKeys.addAll(TEST_SHARDING_KEYS_2);

    for (String namespace : _routingZkAddrMap.keySet()) {
      Assert.assertEquals(_metadataStoreDirectory.getAllShardingKeys(namespace), allShardingKeys);
    }
  }

  @Test(dependsOnMethods = "testGetAllShardingKeys")
  public void testGetNamespaceRoutingData() {
    Map<String, List<String>> routingDataMap = new HashMap<>();
    routingDataMap.put(TEST_REALM_1, TEST_SHARDING_KEYS_1);
    routingDataMap.put(TEST_REALM_2, TEST_SHARDING_KEYS_2);

    for (String namespace : _routingZkAddrMap.keySet()) {
      Assert
          .assertEquals(_metadataStoreDirectory.getNamespaceRoutingData(namespace), routingDataMap);
    }
  }

  @Test(dependsOnMethods = "testGetNamespaceRoutingData")
  public void testSetNamespaceRoutingData() {
    Map<String, List<String>> routingDataMap = new HashMap<>();
    routingDataMap.put(TEST_REALM_1, TEST_SHARDING_KEYS_2);
    routingDataMap.put(TEST_REALM_2, TEST_SHARDING_KEYS_1);

    for (String namespace : _routingZkAddrMap.keySet()) {
      _metadataStoreDirectory.setNamespaceRoutingData(namespace, routingDataMap);
      Assert
          .assertEquals(_metadataStoreDirectory.getNamespaceRoutingData(namespace), routingDataMap);
    }

    // Revert it back to the original state
    Map<String, List<String>> originalRoutingDataMap = new HashMap<>();
    originalRoutingDataMap.put(TEST_REALM_1, TEST_SHARDING_KEYS_1);
    originalRoutingDataMap.put(TEST_REALM_2, TEST_SHARDING_KEYS_2);

    for (String namespace : _routingZkAddrMap.keySet()) {
      _metadataStoreDirectory.setNamespaceRoutingData(namespace, originalRoutingDataMap);
      Assert.assertEquals(_metadataStoreDirectory.getNamespaceRoutingData(namespace),
          originalRoutingDataMap);
    }
  }

  @Test(dependsOnMethods = "testGetNamespaceRoutingData")
  public void testGetAllShardingKeysInRealm() {
    for (String namespace : _routingZkAddrMap.keySet()) {
      // Test two realms independently
      Assert
          .assertEquals(_metadataStoreDirectory.getAllShardingKeysInRealm(namespace, TEST_REALM_1),
              TEST_SHARDING_KEYS_1);
      Assert
          .assertEquals(_metadataStoreDirectory.getAllShardingKeysInRealm(namespace, TEST_REALM_2),
              TEST_SHARDING_KEYS_2);
    }
  }

  @Test(dependsOnMethods = "testGetAllShardingKeysInRealm")
  public void testGetAllMappingUnderPath() {
    Map<String, String> mappingFromRoot = new HashMap<>();
    TEST_SHARDING_KEYS_1.forEach(shardingKey -> {
      mappingFromRoot.put(shardingKey, TEST_REALM_1);
    });
    TEST_SHARDING_KEYS_2.forEach(shardingKey -> {
      mappingFromRoot.put(shardingKey, TEST_REALM_2);
    });

    Map<String, String> mappingFromLeaf = new HashMap<>(1);
    mappingFromLeaf.put("/sharding/key/1/a", TEST_REALM_1);

    for (String namespace : _routingZkAddrMap.keySet()) {
      Assert.assertEquals(_metadataStoreDirectory.getAllMappingUnderPath(namespace, "/"),
          mappingFromRoot);
      Assert.assertEquals(
          _metadataStoreDirectory.getAllMappingUnderPath(namespace, "/sharding/key/1/a"),
          mappingFromLeaf);
    }
  }

  @Test(dependsOnMethods = "testGetAllMappingUnderPath")
  public void testGetMetadataStoreRealm() {
    for (String namespace : _routingZkAddrMap.keySet()) {
      Assert.assertEquals(
          _metadataStoreDirectory.getMetadataStoreRealm(namespace, "/sharding/key/1/a/x/y/z"),
          TEST_REALM_1);
      Assert.assertEquals(
          _metadataStoreDirectory.getMetadataStoreRealm(namespace, "/sharding/key/1/d/x/y/z"),
          TEST_REALM_2);
    }
  }

  @Test(dependsOnMethods = "testGetMetadataStoreRealm")
  public void testDataChangeCallback() throws Exception {
    // For all namespaces (Routing ZKs), add an extra sharding key to TEST_REALM_1
    String newKey = "/a/b/c/d/e";
    _zkList.forEach(zk -> {
      ZNRecord znRecord = ZK_SERVER_MAP.get(zk).getZkClient()
          .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_1);
      znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY).add(newKey);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_1,
              znRecord);
    });

    // Verify that the sharding keys field have been updated
    Assert.assertTrue(TestHelper.verify(() -> {
      for (String namespace : _routingZkAddrMap.keySet()) {
        try {
          return _metadataStoreDirectory.getAllShardingKeys(namespace).contains(newKey)
              && _metadataStoreDirectory.getAllShardingKeysInRealm(namespace, TEST_REALM_1)
              .contains(newKey);
        } catch (NoSuchElementException e) {
          // Pass - wait until callback is called
        }
      }
      return false;
    }, TestHelper.WAIT_DURATION));
  }

  @Test(dependsOnMethods = "testDataChangeCallback")
  public void testChildChangeCallback() throws Exception {
    // For all namespaces (Routing ZKs), add a realm with a sharding key list
    _zkList.forEach(zk -> {
      ZK_SERVER_MAP.get(zk).getZkClient()
          .createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_3,
              true);
      ZNRecord znRecord = new ZNRecord("RoutingInfo");
      znRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
          TEST_SHARDING_KEYS_3);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_3,
              znRecord);
    });

    // Verify that the new realm and sharding keys have been updated in-memory via callback
    Assert.assertTrue(TestHelper.verify(() -> {
      for (String namespace : _routingZkAddrMap.keySet()) {
        try {
          return _metadataStoreDirectory.getAllMetadataStoreRealms(namespace).contains(TEST_REALM_3)
              && _metadataStoreDirectory.getAllShardingKeysInRealm(namespace, TEST_REALM_3)
              .containsAll(TEST_SHARDING_KEYS_3);
        } catch (NoSuchElementException e) {
          // Pass - wait until callback is called
        }
      }
      return false;
    }, TestHelper.WAIT_DURATION));

    // Since there was a child change callback, make sure data change works on the new child (realm) as well by adding a key
    // This tests removing all subscriptions and subscribing with new children list
    // For all namespaces (Routing ZKs), add an extra sharding key to TEST_REALM_3
    String newKey = "/x/y/z";
    _zkList.forEach(zk -> {
      ZNRecord znRecord = ZK_SERVER_MAP.get(zk).getZkClient()
          .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_3);
      znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY).add(newKey);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_3,
              znRecord);
    });

    // Verify that the sharding keys field have been updated
    Assert.assertTrue(TestHelper.verify(() -> {
      for (String namespace : _routingZkAddrMap.keySet()) {
        try {
          return _metadataStoreDirectory.getAllShardingKeys(namespace).contains(newKey)
              && _metadataStoreDirectory.getAllShardingKeysInRealm(namespace, TEST_REALM_3)
              .contains(newKey);
        } catch (NoSuchElementException e) {
          // Pass - wait until callback is called
        }
      }
      return false;
    }, TestHelper.WAIT_DURATION));
  }

  @Test(dependsOnMethods = "testChildChangeCallback")
  public void testDataDeletionCallback() throws Exception {
    // For all namespaces, delete all routing data
    _zkList.forEach(zk -> {
        ZkClient zkClient = ZK_SERVER_MAP.get(zk).getZkClient();
        if (zkClient.exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
          for (String zkRealm : zkClient
              .getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
            zkClient.delete(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm);
          }
        }
      }
    );

    // Confirm that TrieRoutingData has been removed due to missing routing data
    Assert.assertTrue(TestHelper.verify(() -> {
      for (String namespace : _routingZkAddrMap.keySet()) {
        try {
          _metadataStoreDirectory.getMetadataStoreRealm(namespace, "anyKey");
          return false;
        } catch (IllegalStateException e) {
          if (!e.getMessage().equals("Failed to get metadata store realm: Namespace " + namespace
              + " contains either empty or invalid routing data!")) {
            return false;
          }
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  private void clearRoutingData() throws Exception {
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
}
