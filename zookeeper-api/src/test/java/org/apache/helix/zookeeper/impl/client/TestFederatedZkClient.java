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
import java.util.concurrent.TimeUnit;

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
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.ContainerManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestFederatedZkClient extends RealmAwareZkClientTestBase {
  private static final String TEST_SHARDING_KEY_PREFIX = ZK_SHARDING_KEY_PREFIX;
  private static final String TEST_REALM_ONE_VALID_PATH = TEST_SHARDING_KEY_PREFIX + "/1/a/b/c";
  private static final String TEST_REALM_TWO_VALID_PATH = TEST_SHARDING_KEY_PREFIX + "/2/x/y/z";
  private static final String TEST_INVALID_PATH = TEST_SHARDING_KEY_PREFIX + "invalid/a/b/c";
  private static final String UNSUPPORTED_OPERATION_MESSAGE =
      "Session-aware operation is not supported by FederatedZkClient.";


  @BeforeClass
  public void beforeClass() throws IOException, InvalidRoutingDataException {
    System.out.println("Starting " + TestFederatedZkClient.class.getSimpleName());

    // Feed the raw routing data into TrieRoutingData to construct an in-memory representation
    // of routing information.
    _realmAwareZkClient =
        new FederatedZkClient(new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().build(),
            new RealmAwareZkClient.RealmAwareZkClientConfig());
  }

  @AfterClass
  public void afterClass() {
    // Close it as it is created in before class.
    _realmAwareZkClient.close();
    System.out.println("Ending " + TestFederatedZkClient.class.getSimpleName());
  }

  /*
   * Tests that an unsupported operation should throw an UnsupportedOperationException.
   */
  @Test
  public void testUnsupportedOperations() throws IOException, InvalidRoutingDataException {
    // Test creating ephemeral.
    try {
      _realmAwareZkClient.create(TEST_REALM_ONE_VALID_PATH, "Hello", CreateMode.EPHEMERAL);
      Assert.fail("Ephemeral node should not be created.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage().startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    // Test creating ephemeral sequential.
    try {
      _realmAwareZkClient
          .create(TEST_REALM_ONE_VALID_PATH, "Hello", CreateMode.EPHEMERAL_SEQUENTIAL);
      Assert.fail("Ephemeral node should not be created.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage().startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    try {
      _realmAwareZkClient.getSessionId();
      Assert.fail("getSessionId() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage().startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    try {
      _realmAwareZkClient.getServers();
      Assert.fail("getServers() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage().startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    try {
      _realmAwareZkClient.waitUntilConnected(5L, TimeUnit.SECONDS);
      Assert.fail("getServers() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage().startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    // Test state change subscription.
    IZkStateListener listener = new IZkStateListener() {
      @Override
      public void handleStateChanged(Watcher.Event.KeeperState state) {
        System.out.println("Handle new state: " + state);
      }

      @Override
      public void handleNewSession(String sessionId) {
        System.out.println("Handle new session: " + sessionId);
      }

      @Override
      public void handleSessionEstablishmentError(Throwable error) {
        System.out.println("Handle session establishment error: " + error);
      }
    };

    try {
      _realmAwareZkClient.subscribeStateChanges(listener);
      Assert.fail("getServers() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage().startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    try {
      _realmAwareZkClient.unsubscribeStateChanges(listener);
      Assert.fail("getServers() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage().startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }
  }

  /**
   * Test creating a container node.
   */
  @Test(dependsOnMethods = "testUnsupportedOperations")
  public void testRealmAwareZkClientCreateContainer() {
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
    _realmAwareZkClient.setZkSerializer(new ZNRecordSerializer());

    // Create a dummy ZNRecord
    ZNRecord znRecord = new ZNRecord("DummyRecord");
    znRecord.setSimpleField("Dummy", "Value");

    // Test with createParents = true
    _realmAwareZkClient.createContainer(TEST_VALID_PATH, true);
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_VALID_PATH));

    // Test writing and reading data
    String childPath = TEST_VALID_PATH + "/child";
    _realmAwareZkClient.createContainer(childPath, znRecord);
    ZNRecord retrievedRecord = _realmAwareZkClient.readData(childPath);
    Assert.assertEquals(znRecord.getSimpleField("Dummy"),
        retrievedRecord.getSimpleField("Dummy"));

    // Clean up
    _realmAwareZkClient.deleteRecursively(TEST_VALID_PATH);
    System.clearProperty("zookeeper.extendedTypesEnabled");
  }

  /**
   * Test creating a sequential TTL node.
   */
  @Test(dependsOnMethods = "testRealmAwareZkClientCreateContainer")
  public void testRealmAwareZkClientCreateSequentialWithTTL() throws InterruptedException {
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
    // Create a dummy ZNRecord
    ZNRecord znRecord = new ZNRecord("DummyRecord");
    znRecord.setSimpleField("Dummy", "Value");

    // Test writing and reading data
    _realmAwareZkClient.createPersistent(TEST_VALID_PATH, true);
    long ttl = 1L;
    String childPath = TEST_VALID_PATH + "/child";
    _realmAwareZkClient.createPersistentSequentialWithTTL(childPath, znRecord, ttl);
    ZNRecord retrievedRecord = _realmAwareZkClient.readData(childPath + "0000000000");
    Assert.assertEquals(znRecord.getSimpleField("Dummy"),
        retrievedRecord.getSimpleField("Dummy"));

    // Check if the TTL znode expires or not.
    advanceFakeElapsedTime(2000);
    ContainerManager containerManager = _zkServerContainerManagerMap.get(_zkServerMap.get(ZkTestBase.ZK_ADDR));
    containerManager.checkContainers();
    Assert.assertFalse(_realmAwareZkClient.exists(childPath));

    // Clean up
    _realmAwareZkClient.deleteRecursively(TEST_VALID_PATH);
    System.clearProperty("zookeeper.extendedTypesEnabled");
  }

  /**
   * Test creating a TTL node.
   */
  @Test(dependsOnMethods = "testRealmAwareZkClientCreateSequentialWithTTL")
  public void testRealmAwareZkClientCreateWithTTL() throws InterruptedException {
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
    // Create a dummy ZNRecord
    ZNRecord znRecord = new ZNRecord("DummyRecord");
    znRecord.setSimpleField("Dummy", "Value");

    // Test with createParents = true
    long ttl = 1L;
    _realmAwareZkClient.createPersistentWithTTL(TEST_VALID_PATH, true, ttl);
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_VALID_PATH));

    // Test writing and reading data
    String childPath = TEST_VALID_PATH + "/child";
    _realmAwareZkClient.createPersistentWithTTL(childPath, znRecord, ttl);
    ZNRecord retrievedRecord = _realmAwareZkClient.readData(childPath);
    Assert.assertEquals(znRecord.getSimpleField("Dummy"),
        retrievedRecord.getSimpleField("Dummy"));

    // Check if the TTL znode expires or not.
    advanceFakeElapsedTime(2000);
    ContainerManager containerManager = _zkServerContainerManagerMap.get(_zkServerMap.get(ZkTestBase.ZK_ADDR));
    containerManager.checkContainers();
    Assert.assertFalse(_realmAwareZkClient.exists(childPath));

    // Clean up
    _realmAwareZkClient.deleteRecursively(TEST_VALID_PATH);
    System.clearProperty("zookeeper.extendedTypesEnabled");
  }

  /*
   * Tests the persistent create() call against a valid path and an invalid path.
   * Valid path is one that belongs to the realm designated by the sharding key.
   * Invalid path is one that does not belong to the realm designated by the sharding key.
   */
  @Test(dependsOnMethods = "testRealmAwareZkClientCreateWithTTL")
  public void testCreatePersistent() {
    // Create a dummy ZNRecord
    ZNRecord znRecord = new ZNRecord("DummyRecord");
    znRecord.setSimpleField("Dummy", "Value");

    // Test writing and reading against the validPath
    _realmAwareZkClient.createPersistent(TEST_REALM_ONE_VALID_PATH, true);
    _realmAwareZkClient.writeData(TEST_REALM_ONE_VALID_PATH, znRecord);
    Assert.assertEquals(_realmAwareZkClient.readData(TEST_REALM_ONE_VALID_PATH), znRecord);

    // Test writing and reading against the invalid path
    try {
      _realmAwareZkClient.createPersistent(TEST_INVALID_PATH, true);
      Assert.fail("Create() should not succeed on an invalid path!");
    } catch (NoSuchElementException ex) {
      Assert.assertEquals(ex.getMessage(),
          "No sharding key found within the provided path. Path: " + TEST_INVALID_PATH);
    }
  }

  /*
   * Tests that exists() works on valid path and fails on invalid path.
   */
  @Test(dependsOnMethods = "testCreatePersistent")
  public void testExists() {
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_REALM_ONE_VALID_PATH));

    try {
      _realmAwareZkClient.exists(TEST_INVALID_PATH);
      Assert.fail("Exists() should not succeed on an invalid path!");
    } catch (NoSuchElementException ex) {
      Assert.assertEquals(ex.getMessage(),
          "No sharding key found within the provided path. Path: " + TEST_INVALID_PATH);
    }
  }

  /*
   * Tests that delete() works on valid path and fails on invalid path.
   */
  @Test(dependsOnMethods = "testExists")
  public void testDelete() {
    try {
      _realmAwareZkClient.delete(TEST_INVALID_PATH);
      Assert.fail("Exists() should not succeed on an invalid path!");
    } catch (NoSuchElementException ex) {
      Assert.assertEquals(ex.getMessage(),
          "No sharding key found within the provided path. Path: " + TEST_INVALID_PATH);
    }

    Assert.assertTrue(_realmAwareZkClient.delete(TEST_REALM_ONE_VALID_PATH));
    Assert.assertFalse(_realmAwareZkClient.exists(TEST_REALM_ONE_VALID_PATH));
  }

  /**
   * Tests that delete() works on version match and fails on version mismatch
   */
  @Test(dependsOnMethods = "testDelete")
  public void testDeleteExpectedVersion() {
    // Create a ZNode for testing
    _realmAwareZkClient.createPersistent(TEST_VALID_PATH, true);
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_VALID_PATH));
    int expectedVersion = _realmAwareZkClient.getStat(TEST_VALID_PATH).getVersion();

    // Test delete with invalid version, should fail to delete
    try {
      _realmAwareZkClient.delete(TEST_VALID_PATH, expectedVersion + 100);
      Assert.fail("Should hvae thrown bad version exception");
    } catch (ZkBadVersionException expectedException) {
      // Expected exception, continue
    }

    // Assert delete with expected version successful
    Assert.assertTrue(_realmAwareZkClient.delete(TEST_VALID_PATH, expectedVersion));
    Assert.assertFalse(_realmAwareZkClient.exists(TEST_VALID_PATH));
  }

  /*
   * Tests that multi-realm feature.
   */
  @Test(dependsOnMethods = "testDeleteExpectedVersion")
  public void testMultiRealmCRUD() {
    ZNRecord realmOneZnRecord = new ZNRecord("realmOne");
    realmOneZnRecord.setSimpleField("realmOne", "Value");

    ZNRecord realmTwoZnRecord = new ZNRecord("realmTwo");
    realmTwoZnRecord.setSimpleField("realmTwo", "Value");

    // Writing on realmOne.
    _realmAwareZkClient.createPersistent(TEST_REALM_ONE_VALID_PATH, true);
    _realmAwareZkClient.writeData(TEST_REALM_ONE_VALID_PATH, realmOneZnRecord);

    // RealmOne path is created but realmTwo path is not.
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_REALM_ONE_VALID_PATH));
    Assert.assertFalse(_realmAwareZkClient.exists(TEST_REALM_TWO_VALID_PATH));

    // Writing on realmTwo.
    _realmAwareZkClient.createPersistent(TEST_REALM_TWO_VALID_PATH, true);
    _realmAwareZkClient.writeData(TEST_REALM_TWO_VALID_PATH, realmTwoZnRecord);

    // RealmTwo path is created.
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_REALM_TWO_VALID_PATH));

    // Reading on both realms.
    Assert.assertEquals(_realmAwareZkClient.readData(TEST_REALM_ONE_VALID_PATH), realmOneZnRecord);
    Assert.assertEquals(_realmAwareZkClient.readData(TEST_REALM_TWO_VALID_PATH), realmTwoZnRecord);

    Assert.assertTrue(_realmAwareZkClient.delete(TEST_REALM_ONE_VALID_PATH));
    Assert.assertFalse(_realmAwareZkClient.exists(TEST_REALM_ONE_VALID_PATH));

    // Deleting on realmOne does not delete on realmTwo.
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_REALM_TWO_VALID_PATH));

    // Deleting on realmTwo.
    Assert.assertTrue(_realmAwareZkClient.delete(TEST_REALM_TWO_VALID_PATH));
    Assert.assertFalse(_realmAwareZkClient.exists(TEST_REALM_TWO_VALID_PATH));
  }

  /**
   * This tests the routing data update feature only enabled when
   * RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS is set to true.
   * Routing data source is MSDS.
   */
  @Test(dependsOnMethods = "testMultiRealmCRUD")
  public void testUpdateRoutingDataOnCacheMissMSDS()
      throws IOException, InvalidRoutingDataException {
    // Enable routing data update upon cache miss
    System.setProperty(RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS, "true");
    // Set the routing data update interval to 0 so there's no delay in testing
    System.setProperty(RoutingSystemPropertyKeys.ROUTING_DATA_UPDATE_INTERVAL_MS, "0");
    RoutingDataManager.getInstance().parseRoutingDataUpdateInterval();

    RoutingDataManager.getInstance().getMetadataStoreRoutingData();
    _msdsServer.stopServer();
    /*
     * Test is 2-tiered because cache update is 2-tiered
     * Case 1:
     * - RoutingDataManager (in-memory) does not have the key
     * - MSDS has the key
     * This simulates a case where FederatedZkClient must do a I/O based update.
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

    // Create a new FederatedZkClient
    FederatedZkClient federatedZkClient = new FederatedZkClient(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP.name())
            .setRoutingDataSourceEndpoint(
                "http://" + MSDS_HOSTNAME + ":" + MSDS_PORT + "/admin/v2/namespaces/"
                    + MSDS_NAMESPACE).build(), new RealmAwareZkClient.RealmAwareZkClientConfig());

    // exists() must succeed and RoutingDataManager should now have the key (cache update must have
    // happened)
    // False expected for the following call because the znode does not exist and we are checking
    // whether the call succeeds or not
    Assert.assertFalse(federatedZkClient.exists(newShardingKey));
    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance().getMetadataStoreRoutingData()
        .getMetadataStoreRealm(newShardingKey));

    /*
     * Case 2:
     * - RoutingDataManager has the key
     * - MSDS does not have the key
     * - continue using the same ZkClient because we want an existing federated client that does
     * not have the key
     */
    _msdsServer.stopServer();
    // Create an MSDS with the key and reset MSDS so it doesn't contain the key
    String newShardingKey2 = "/sharding-key-10";
    rawRoutingData.get(zkRealm).add(newShardingKey2);
    _msdsServer = new MockMetadataStoreDirectoryServer(MSDS_HOSTNAME, MSDS_PORT, MSDS_NAMESPACE,
        rawRoutingData);
    _msdsServer.startServer();

    // Make sure RoutingDataManager has the key
    RoutingDataManager.getInstance().reset(true);
    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance().getMetadataStoreRoutingData()
        .getMetadataStoreRealm(newShardingKey2));

    // Reset MSDS so it doesn't contain the key
    _msdsServer.stopServer();
    _msdsServer = new MockMetadataStoreDirectoryServer(MSDS_HOSTNAME, MSDS_PORT, MSDS_NAMESPACE,
        TestConstants.FAKE_ROUTING_DATA); // FAKE_ROUTING_DATA doesn't contain the key
    _msdsServer.startServer();

    // exists() must succeed and RoutingDataManager should still have the key
    // This means that we do not do a hard update (I/O based update) because in-memory cache already
    // has the key
    // False expected for the following call because the znode does not exist and we are checking
    // whether the call succeeds or not
    Assert.assertFalse(federatedZkClient.exists(newShardingKey2));
    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance().getMetadataStoreRoutingData()
        .getMetadataStoreRealm(newShardingKey2));
    // Also check that MSDS does not have the new sharding key through resetting RoutingDataManager
    // and re-reading from MSDS
    RoutingDataManager.getInstance().reset(true);
    try {
      RoutingDataManager.getInstance().getMetadataStoreRoutingData()
          .getMetadataStoreRealm(newShardingKey2);
      Assert.fail("NoSuchElementException expected!");
    } catch (NoSuchElementException e) {
      // Expected because MSDS does not contain the key
    }

    // Clean up federatedZkClient
    federatedZkClient.close();
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
    RoutingDataManager.getInstance().parseRoutingDataUpdateInterval();

    RoutingDataManager.getInstance().reset(true);
    RoutingDataManager.getInstance().getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm);
    /*
     * Test is 2-tiered because cache update is 2-tiered
     * Case 1:
     * - RoutingDataManager does not have the key
     * - ZK has the key
     * This simulates a case where FederatedZkClient must do a I/O based update (must read from ZK).
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

    // Create a new FederatedZkClient
    FederatedZkClient federatedZkClient = new FederatedZkClient(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.ZK.name())
            .setRoutingDataSourceEndpoint(zkRealm).build(),
        new RealmAwareZkClient.RealmAwareZkClientConfig());

    // exists() must succeed and RoutingDataManager should now have the key (cache update must
    // have happened)
    // False expected for the following call because the znode does not exist and we are checking
    // whether the call succeeds or not
    Assert.assertFalse(federatedZkClient.exists(newShardingKey));
    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance()
        .getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm)
        .getMetadataStoreRealm(newShardingKey));

    /*
     * Case 2:
     * - RoutingDataManager has the key
     * - ZK does not have the key
     * - continue using the same ZkClient because we want an existing federated client that does
     * not have the key
     */
    // Add newShardingKey2 to ZK's routing data (in order to give RoutingDataManager the key)
    zkRealmRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .add(newShardingKey2);
    zkClient
        .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm, zkRealmRecord);

    // Update RoutingDataManager so it has the key
    RoutingDataManager.getInstance().reset(true);
    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance()
        .getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm)
        .getMetadataStoreRealm(newShardingKey2));

    // Remove newShardingKey2 from ZK
    zkRealmRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .remove(newShardingKey2);
    zkClient
        .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm, zkRealmRecord);

    // exists() must succeed and RoutingDataManager should still have the key
    // This means that we do not do a hard update (I/O based update) because in-memory cache already
    // has the key
    // False expected for the following call because the znode does not exist and we are checking
    // whether the call succeeds or not
    Assert.assertFalse(federatedZkClient.exists(newShardingKey2));
    Assert.assertEquals(zkRealm, RoutingDataManager.getInstance()
        .getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm)
        .getMetadataStoreRealm(newShardingKey2));
    // Also check that ZK does not have the new sharding key through resetting RoutingDataManager
    // and re-reading from ZK
    RoutingDataManager.getInstance().reset(true);
    try {
      RoutingDataManager.getInstance()
          .getMetadataStoreRoutingData(RoutingDataReaderType.ZK, zkRealm)
          .getMetadataStoreRealm(newShardingKey2);
      Assert.fail("NoSuchElementException expected!");
    } catch (NoSuchElementException e) {
      // Expected because ZK does not contain the key
    }

    // Clean up federatedZkClient
    federatedZkClient.close();
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
    RoutingDataManager.getInstance().reset(true);

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
    RoutingDataManager.getInstance().parseRoutingDataUpdateInterval();

    // Create a new FederatedZkClient, whose _routingDataUpdateInterval should be MAX_VALUE
    FederatedZkClient federatedZkClient = new FederatedZkClient(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.ZK.name())
            .setRoutingDataSourceEndpoint(zkRealm).build(),
        new RealmAwareZkClient.RealmAwareZkClientConfig());

    // Add newShardingKey to ZK's routing data
    zkRealmRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .add(newShardingKey);
    zkClient
        .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm, zkRealmRecord);

    try {
      Assert.assertFalse(federatedZkClient.exists(newShardingKey));
      Assert.fail("NoSuchElementException expected!");
    } catch (NoSuchElementException e) {
      // Expected because it should not read from the routing data source because of the throttle
    }

    // Clean up
    zkClient.deleteRecursively(MetadataStoreRoutingConstants.ROUTING_DATA_PATH);
    zkClient.close();
    federatedZkClient.close();
    System.clearProperty(RoutingSystemPropertyKeys.UPDATE_ROUTING_DATA_ON_CACHE_MISS);
    System.clearProperty(RoutingSystemPropertyKeys.ROUTING_DATA_UPDATE_INTERVAL_MS);
  }

  /*
   * Tests that close() works.
   * TODO: test that all raw zkClients are closed after FederatedZkClient close() is called. This
   *  could help avoid ZkClient leakage.
   */
  @Test(dependsOnMethods = "testRoutingDataUpdateThrottle")
  public void testClose() {
    Assert.assertFalse(_realmAwareZkClient.isClosed());

    _realmAwareZkClient.close();

    Assert.assertTrue(_realmAwareZkClient.isClosed());

    // Client is closed, so operation should not be executed.
    try {
      _realmAwareZkClient.createPersistent(TEST_REALM_ONE_VALID_PATH);
      Assert
          .fail("createPersistent() should not be executed because RealmAwareZkClient is closed.");
    } catch (IllegalStateException ex) {
      Assert.assertEquals(ex.getMessage(), "FederatedZkClient is closed!");
    }
  }

  @Override
  public void testMultiSetup() throws InvalidRoutingDataException {
    super.testMultiSetup();
    if (!_realmAwareZkClient.exists(ZK_SHARDING_KEY_PREFIX)) {
      _realmAwareZkClient.createPersistent(ZK_SHARDING_KEY_PREFIX);
    }
  }
}
