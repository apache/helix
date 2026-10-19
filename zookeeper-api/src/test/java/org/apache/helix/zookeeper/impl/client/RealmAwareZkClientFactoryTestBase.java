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
import java.util.NoSuchElementException;

import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.zookeeper.server.ContainerManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test Base for DedicatedZkClient and SharedZkClient, which are implementations of
 * RealmAwareZkClient.
 * This class allows TestDedicatedZkClient and TestSharedZkClient to share the common test logic by
 * just swapping out the factory classes.
 */
public abstract class RealmAwareZkClientFactoryTestBase extends RealmAwareZkClientTestBase {
  // The following RealmAwareZkClientFactory is to be defined in subclasses
  protected RealmAwareZkClient _realmAwareZkClient;
  private static final ZNRecord DUMMY_RECORD = new ZNRecord("DummyRecord");

  @BeforeClass
  public void beforeClass() throws IOException, InvalidRoutingDataException {
    DUMMY_RECORD.setSimpleField("Dummy", "Value");
  }

  @AfterClass
  public void afterClass() {
    if (_realmAwareZkClient != null && !_realmAwareZkClient.isClosed()) {
      _realmAwareZkClient.close();
      _realmAwareZkClient = null;
    }
  }

  /**
   * 1. Create a RealmAwareZkClient with a non-existing sharding key (for which there is no valid ZK realm)
   * -> This should fail with an exception
   * 2. Create a RealmAwareZkClient with a valid sharding key
   * -> This should pass
   */
  @Test
  public void testRealmAwareZkClientCreation() {
    // Create a RealmAwareZkClient
    String invalidShardingKey = "InvalidShardingKeyNoLeadingSlash";
    RealmAwareZkClient.RealmAwareZkClientConfig clientConfig =
        new RealmAwareZkClient.RealmAwareZkClientConfig();

    // Create a connection config with the invalid sharding key
    RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder builder =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
    RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
        builder.setZkRealmShardingKey(invalidShardingKey).build();

    try {
      _realmAwareZkClient =
          _realmAwareZkClientFactory.buildZkClient(connectionConfig, clientConfig);
      Assert.fail("Should not succeed with an invalid sharding key!");
    } catch (IllegalArgumentException e) {
      // Expected because invalid sharding key would cause an IllegalArgumentException to be thrown
    } catch (Exception e) {
      Assert.fail("Should not see any other types of Exceptions: " + e);
    }

    // Create a connection config with a valid sharding key, but one that does not exist in
    // the routing data
    String nonExistentShardingKey = "/NonExistentShardingKey";
    connectionConfig = builder.setZkRealmShardingKey(nonExistentShardingKey).build();
    try {
      _realmAwareZkClient =
          _realmAwareZkClientFactory.buildZkClient(connectionConfig, clientConfig);
      Assert.fail("Should not succeed with a non-existent sharding key!");
    } catch (NoSuchElementException e) {
      // Expected non-existent sharding key would cause a NoSuchElementException to be thrown
    } catch (Exception e) {
      Assert.fail("Should not see any other types of Exceptions: " + e);
    }

    // Use a valid sharding key this time around
    connectionConfig = builder.setZkRealmShardingKey(ZK_SHARDING_KEY_PREFIX).build();
    try {
      _realmAwareZkClient =
          _realmAwareZkClientFactory.buildZkClient(connectionConfig, clientConfig);
    } catch (Exception e) {
      Assert.fail("All other exceptions not allowed: " + e);
    }
  }

  /**
   * Test creating a container node.
   */
  @Test(dependsOnMethods = "testRealmAwareZkClientCreation")
  public void testRealmAwareZkClientCreateContainer() {
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
    _realmAwareZkClient.setZkSerializer(new ZNRecordSerializer());

    // Test with createParents = true
    _realmAwareZkClient.createContainer(TEST_VALID_PATH, true);
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_VALID_PATH));

    // Test writing and reading data
    String childPath = TEST_VALID_PATH + "/child";
    _realmAwareZkClient.createContainer(childPath, DUMMY_RECORD);
    ZNRecord retrievedRecord = _realmAwareZkClient.readData(childPath);
    Assert.assertEquals(DUMMY_RECORD.getSimpleField("Dummy"),
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
    // Test writing and reading data
    _realmAwareZkClient.createPersistent(TEST_VALID_PATH, true);
    long ttl = 1L;
    String childPath = TEST_VALID_PATH + "/child";
    _realmAwareZkClient.createPersistentSequentialWithTTL(childPath, DUMMY_RECORD, ttl);
    ZNRecord retrievedRecord = _realmAwareZkClient.readData(childPath + "0000000000");
    Assert.assertEquals(DUMMY_RECORD.getSimpleField("Dummy"),
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
    // Test with createParents = true
    long ttl = 1L;
    _realmAwareZkClient.createPersistentWithTTL(TEST_VALID_PATH, true, ttl);
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_VALID_PATH));

    // Test writing and reading data
    String childPath = TEST_VALID_PATH + "/child";
    _realmAwareZkClient.createPersistentWithTTL(childPath, DUMMY_RECORD, ttl);
    ZNRecord retrievedRecord = _realmAwareZkClient.readData(childPath);
    Assert.assertEquals(DUMMY_RECORD.getSimpleField("Dummy"),
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
   * Test the persistent create() call against a valid path and an invalid path.
   * Valid path is one that belongs to the realm designated by the sharding key.
   * Invalid path is one that does not belong to the realm designated by the sharding key.
   */
  @Test(dependsOnMethods = "testRealmAwareZkClientCreateWithTTL")
  public void testRealmAwareZkClientCreatePersistent() {
    // Test writing and reading against the validPath
    _realmAwareZkClient.createPersistent(TEST_VALID_PATH, true);
    _realmAwareZkClient.writeData(TEST_VALID_PATH, DUMMY_RECORD);
    Assert.assertEquals(_realmAwareZkClient.readData(TEST_VALID_PATH), DUMMY_RECORD);

    // Test writing and reading against the invalid path
    try {
      _realmAwareZkClient.createPersistent(TEST_INVALID_PATH, true);
      Assert.fail("Create() should not succeed on an invalid path!");
    } catch (IllegalArgumentException e) {
      // Okay - expected
    }
  }

  /**
   * Test that exists() works on valid path and fails on invalid path.
   */
  @Test(dependsOnMethods = "testRealmAwareZkClientCreatePersistent")
  public void testExists() {
    // Create a ZNode for testing
    _realmAwareZkClient.createPersistent(TEST_VALID_PATH, true);
    _realmAwareZkClient.writeData(TEST_VALID_PATH, DUMMY_RECORD);
    Assert.assertEquals(_realmAwareZkClient.readData(TEST_VALID_PATH), DUMMY_RECORD);

    // Test exists()
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_VALID_PATH));

    try {
      _realmAwareZkClient.exists(TEST_INVALID_PATH);
      Assert.fail("Exists() should not succeed on an invalid path!");
    } catch (IllegalArgumentException e) {
      // Okay - expected
    }
  }

  /**
   * Test that delete() works on valid path and fails on invalid path.
   */
  @Test(dependsOnMethods = "testExists")
  public void testDelete() {
    // Create a ZNode for testing
    _realmAwareZkClient.createPersistent(TEST_VALID_PATH, true);
    _realmAwareZkClient.writeData(TEST_VALID_PATH, DUMMY_RECORD);
    Assert.assertEquals(_realmAwareZkClient.readData(TEST_VALID_PATH), DUMMY_RECORD);

    try {
      _realmAwareZkClient.delete(TEST_INVALID_PATH);
      Assert.fail("delete() should not succeed on an invalid path!");
    } catch (IllegalArgumentException e) {
      // Okay - expected
    }

    Assert.assertTrue(_realmAwareZkClient.delete(TEST_VALID_PATH));
    Assert.assertFalse(_realmAwareZkClient.exists(TEST_VALID_PATH));
  }

  /**
   * Test that delete() with expected version fails on version mismatch. Succeeds on version match.
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
      Assert.fail("Should have thrown bad version exception");
    } catch (ZkBadVersionException expectedException) {
      // Expected exception, continue
    }

    // Assert delete with expected version successful
    Assert.assertTrue(_realmAwareZkClient.delete(TEST_VALID_PATH, expectedVersion));
    Assert.assertFalse(_realmAwareZkClient.exists(TEST_VALID_PATH));
  }
}
