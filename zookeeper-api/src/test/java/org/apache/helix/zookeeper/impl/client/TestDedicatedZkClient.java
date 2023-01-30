package org.apache.helix.zookeeper.impl.client;

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

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.constant.RoutingSystemPropertyKeys;
import org.apache.helix.zookeeper.constant.TestConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDedicatedZkClient extends RealmAwareZkClientFactoryTestBase {

  @BeforeClass
  public void beforeClass() throws IOException, InvalidRoutingDataException {
    super.beforeClass();
    // Set the factory to DedicatedZkClientFactory
    _realmAwareZkClientFactory = DedicatedZkClientFactory.getInstance();
  }


  /**
   * This tests the routing data update feature only enabled when
   * RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS is set to true.
   * Routing data source is MSDS.
   */
  @Test
  public void testUpdateRoutingDataOnCacheMissMSDS()
      throws IOException, InvalidRoutingDataException {
    // Enable routing data update upon cache miss
    System.setProperty(RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS, "true");
    // Set the routing data update interval to 0 so there's no delay in testing
    System.setProperty(RoutingSystemPropertyKeys.ROUTING_DATA_UPDATE_INTERVAL_MS, "0");

    RoutingDataManager.getInstance().getMetadataStoreRoutingData();
    _msdsServer.stopServer();
    /*
     * Test is 2-tiered because cache update is 2-tiered
     * Case 1:
     * - RoutingDataManager (in-memory) does not have the key
     * - MSDS has the key
     * This simulates a case where DedicatedZkClient must do a I/O based update.
     */
    // Start MSDS with a new key
    String newShardingKey = "/sharding-key-9";
    String zkRealm = "localhost:2127";
    Map<String, Collection<String>> rawRoutingData = new HashMap<>();
    rawRoutingData.put(zkRealm, new ArrayList<>());
    rawRoutingData.get(zkRealm).add(newShardingKey); // Add a new key
    _msdsServer = new MockMetadataStoreDirectoryServer(MSDS_HOSTNAME, MSDS_PORT, MSDS_NAMESPACE,
        rawRoutingData);
    _msdsServer.startServer();

    // Verify that RoutingDataManager does not have the key
    MetadataStoreRoutingData routingData =
        RoutingDataManager.getInstance().getMetadataStoreRoutingData();
    try {
      routingData.getMetadataStoreRealm(newShardingKey);
      Assert.fail("Must throw NoSuchElementException!");
    } catch (NoSuchElementException e) {
      // Expected
    }

    // Create a new DedicatedZkClient
    DedicatedZkClient dedicatedZkClient = new DedicatedZkClient(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP.name())
            .setRoutingDataSourceEndpoint(
                "http://" + MSDS_HOSTNAME + ":" + MSDS_PORT + "/admin/v2/namespaces/"
                    + MSDS_NAMESPACE)
            .setZkRealmShardingKey(newShardingKey).build(), new RealmAwareZkClient.RealmAwareZkClientConfig());

    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance().getMetadataStoreRoutingData()
        .getMetadataStoreRealm(newShardingKey));

    /*
     * Case 2:
     * - RoutingDataManager has the key
     * - MSDS does not have the key
     */
    _msdsServer.stopServer();
    // Create an MSDS with the key and reset MSDS so it doesn't contain the key
    String newShardingKey2 = "/sharding-key-10";
    rawRoutingData.get(zkRealm).add(newShardingKey2);
    _msdsServer = new MockMetadataStoreDirectoryServer(MSDS_HOSTNAME, MSDS_PORT, MSDS_NAMESPACE,
        rawRoutingData);
    _msdsServer.startServer();

    // Make sure RoutingDataManager has the key
    RoutingDataManager.getInstance().reset();
    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance().getMetadataStoreRoutingData()
        .getMetadataStoreRealm(newShardingKey2));

    // Reset MSDS so it doesn't contain the key
    _msdsServer.stopServer();
    _msdsServer = new MockMetadataStoreDirectoryServer(MSDS_HOSTNAME, MSDS_PORT, MSDS_NAMESPACE,
        TestConstants.FAKE_ROUTING_DATA); // FAKE_ROUTING_DATA doesn't contain the key
    _msdsServer.startServer();

    dedicatedZkClient = new DedicatedZkClient(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP.name())
            .setRoutingDataSourceEndpoint(
                "http://" + MSDS_HOSTNAME + ":" + MSDS_PORT + "/admin/v2/namespaces/"
                    + MSDS_NAMESPACE)
            .setZkRealmShardingKey(newShardingKey2).build(), new RealmAwareZkClient.RealmAwareZkClientConfig());

    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance().getMetadataStoreRoutingData()
        .getMetadataStoreRealm(newShardingKey2));
    // Also check that MSDS does not have the new sharding key through resetting RoutingDataManager
    // and re-reading from MSDS
    RoutingDataManager.getInstance().reset();
    try {
      RoutingDataManager.getInstance().getMetadataStoreRoutingData()
          .getMetadataStoreRealm(newShardingKey2);
      Assert.fail("NoSuchElementException expected!");
    } catch (NoSuchElementException e) {
      // Expected because MSDS does not contain the key
    }

    // Clean up dedicatedZkClient
    dedicatedZkClient.close();
    // Shut down MSDS
    _msdsServer.stopServer();
    // Disable System property
    System.clearProperty(RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS);
    System.clearProperty(RoutingSystemPropertyKeys.ROUTING_DATA_UPDATE_INTERVAL_MS);
  }

  /**
   * This tests the routing data update feature only enabled when
   * RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS is set to true.
   * Routing data source is ZK.
   */
  @Test(dependsOnMethods = "testUpdateRoutingDataOnCacheMissMSDS")
  public void testUpdateRoutingDataOnCacheMissZK() throws IOException, InvalidRoutingDataException {
    // Set up routing data in ZK with empty sharding key list
    String zkRealm = "localhost:2127";
    String newShardingKey = "/sharding-key-9";
    String newShardingKey2 = "/sharding-key-10";
    ZkClient zkClient =
        new ZkClient.Builder().setZkServer(zkRealm).setZkSerializer(new ZNRecordSerializer())
            .build();
    zkClient.create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, null, CreateMode.PERSISTENT);
    ZNRecord zkRealmRecord = new ZNRecord(zkRealm);
    List<String> keyList =
        new ArrayList<>(TestConstants.TEST_KEY_LIST_1); // Need a non-empty keyList
    zkRealmRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY, keyList);
    zkClient.create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm, zkRealmRecord,
        CreateMode.PERSISTENT);

    // Enable routing data update upon cache miss
    System.setProperty(RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS, "true");
    // Set the routing data update interval to 0 so there's no delay in testing
    System.setProperty(RoutingSystemPropertyKeys.ROUTING_DATA_UPDATE_INTERVAL_MS, "0");

    RoutingDataManager.getInstance().reset();
    RoutingDataManager.getInstance().getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm);
    /*
     * Test is 2-tiered because cache update is 2-tiered
     * Case 1:
     * - RoutingDataManager does not have the key
     * - ZK has the key
     * This simulates a case where DedicatedZkClient must do a I/O based update (must read from ZK).
     */
    // Add the key to ZK
    zkRealmRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .add(newShardingKey);
    zkClient
        .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm, zkRealmRecord);

    // Verify that RoutingDataManager does not have the key
    MetadataStoreRoutingData routingData = RoutingDataManager.getInstance()
        .getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm);
    try {
      routingData.getMetadataStoreRealm(newShardingKey);
      Assert.fail("Must throw NoSuchElementException!");
    } catch (NoSuchElementException e) {
      // Expected
    }

    // Create a new DedicatedZkClient
    DedicatedZkClient dedicatedZkClient = new DedicatedZkClient(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.ZK.name())
            .setRoutingDataSourceEndpoint(zkRealm)
            .setZkRealmShardingKey(newShardingKey).build(),
        new RealmAwareZkClient.RealmAwareZkClientConfig());

    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance()
        .getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm)
        .getMetadataStoreRealm(newShardingKey));

    /*
     * Case 2:
     * - RoutingDataManager has the key
     * - ZK does not have the key
     */
    // Add newShardingKey2 to ZK's routing data (in order to give RoutingDataManager the key)
    zkRealmRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .add(newShardingKey2);
    zkClient
        .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm, zkRealmRecord);

    // Update RoutingDataManager so it has the key
    RoutingDataManager.getInstance().reset();
    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance()
        .getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm)
        .getMetadataStoreRealm(newShardingKey2));

    // Remove newShardingKey2 from ZK
    zkRealmRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .remove(newShardingKey2);
    zkClient
        .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm, zkRealmRecord);

    dedicatedZkClient = new DedicatedZkClient(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.ZK.name())
            .setRoutingDataSourceEndpoint(zkRealm)
            .setZkRealmShardingKey(newShardingKey2).build(),
        new RealmAwareZkClient.RealmAwareZkClientConfig());

    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance()
        .getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm)
        .getMetadataStoreRealm(newShardingKey2));
    // Also check that ZK does not have the new sharding key through resetting RoutingDataManager
    // and re-reading from ZK
    RoutingDataManager.getInstance().reset();
    try {
      RoutingDataManager.getInstance()
          .getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm)
          .getMetadataStoreRealm(newShardingKey2);
      Assert.fail("NoSuchElementException expected!");
    } catch (NoSuchElementException e) {
      // Expected because ZK does not contain the key
    }

    // Clean up dedicatedZkClient
    dedicatedZkClient.close();
    // Clean up ZK writes and ZkClient
    zkClient.deleteRecursively(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);
    zkClient.close();
    // Disable System property
    System.clearProperty(RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS);
    System.clearProperty(RoutingSystemPropertyKeys.ROUTING_DATA_UPDATE_INTERVAL_MS);
  }

  /**
   * Test that throttle based on last reset timestamp works correctly. Here, we use ZK as the
   * routing data source.
   * Test scenario: set the throttle value to a high value and check that routing data update from
   * the routing data source does NOT happen (because it would be throttled).
   */
  @Test(dependsOnMethods = "testUpdateRoutingDataOnCacheMissZK")
  public void testRoutingDataUpdateThrottle() throws InvalidRoutingDataException {
    // Call reset to set the last reset() timestamp in RoutingDataManager
    RoutingDataManager.getInstance().reset();

    // Set up routing data in ZK with empty sharding key list
    String zkRealm = "localhost:2127";
    String newShardingKey = "/throttle";
    ZkClient zkClient =
        new ZkClient.Builder().setZkServer(zkRealm).setZkSerializer(new ZNRecordSerializer())
            .build();
    zkClient.create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH, null, CreateMode.PERSISTENT);
    ZNRecord zkRealmRecord = new ZNRecord(zkRealm);
    zkRealmRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
        new ArrayList<>(TestConstants.TEST_KEY_LIST_1));
    zkClient.create(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm, zkRealmRecord,
        CreateMode.PERSISTENT);

    // Enable routing data update upon cache miss
    System.setProperty(RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS, "true");
    // Set the throttle value to a very long value
    System.setProperty(RoutingSystemPropertyKeys.ROUTING_DATA_UPDATE_INTERVAL_MS,
        String.valueOf(Integer.MAX_VALUE));

    // Create a new DedicatedZkClient, whose _routingDataUpdateInterval should be MAX_VALUE
    DedicatedZkClient dedicatedZkClient = new DedicatedZkClient(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.ZK.name())
            .setRoutingDataSourceEndpoint(zkRealm)
            .setZkRealmShardingKey(TestConstants.TEST_KEY_LIST_1.get(0)).build(),
        new RealmAwareZkClient.RealmAwareZkClientConfig());

    // Add newShardingKey to ZK's routing data
    zkRealmRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .add(newShardingKey);
    zkClient
        .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm, zkRealmRecord);

    try {
      dedicatedZkClient = new DedicatedZkClient(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.ZK.name())
            .setRoutingDataSourceEndpoint(zkRealm)
            .setZkRealmShardingKey(newShardingKey).build(),
        new RealmAwareZkClient.RealmAwareZkClientConfig());
      Assert.fail("NoSuchElementException expected!");
    } catch (NoSuchElementException e) {
      // Expected because it should not read from the routing data source because of the throttle
    }

    // Clean up
    zkClient.deleteRecursively(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);
    zkClient.close();
    System.clearProperty(RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS);
    System.clearProperty(RoutingSystemPropertyKeys.ROUTING_DATA_UPDATE_INTERVAL_MS);
  }
}
