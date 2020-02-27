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

import java.util.NoSuchElementException;

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.api.factory.RealmAwareZkClientFactory;
import org.apache.helix.zookeeper.constant.TestConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public abstract class RealmAwareZkClientTestBase extends ZkTestBase {
  private static final String ZK_SHARDING_KEY_PREFIX = "/sharding-key-0";
  private static final String TEST_VALID_PATH = ZK_SHARDING_KEY_PREFIX + "/a/b/c";
  private static final String TEST_INVALID_PATH = ZK_SHARDING_KEY_PREFIX + "_invalid" + "/a/b/c";

  // The following RealmAwareZkClientFactory is to be defined in subclasses
  protected RealmAwareZkClientFactory _realmAwareZkClientFactory;
  private RealmAwareZkClient _realmAwareZkClient;

  // Create a MockMSDS for testing
  private MockMetadataStoreDirectoryServer _msdsServer;
  private static final String MSDS_HOSTNAME = "localhost";
  private static final int MSDS_PORT = 1111;
  private static final String MSDS_NAMESPACE = "test";

  @BeforeClass
  public void beforeClass() throws Exception {
    // Create a mock MSDS so that HttpRoudingDataReader could fetch the routing data
    _msdsServer = new MockMetadataStoreDirectoryServer(MSDS_HOSTNAME, MSDS_PORT, MSDS_NAMESPACE,
        TestConstants.FAKE_ROUTING_DATA);
    _msdsServer.startServer();

    // Register the MSDS endpoint as a System variable
    String msdsEndpoint =
        "http://" + MSDS_HOSTNAME + ":" + MSDS_PORT + "/admin/v2/namespaces/" + MSDS_NAMESPACE;
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, msdsEndpoint);
  }

  @AfterClass
  public void afterClass() {
    if (_realmAwareZkClient != null && !_realmAwareZkClient.isClosed()) {
      _realmAwareZkClient.close();
    }
    if (_msdsServer != null) {
      _msdsServer.stopServer();
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
    String validShardingKey = ZK_SHARDING_KEY_PREFIX + "_" + 0; // Use TEST_SHARDING_KEY_0
    builder.setZkRealmShardingKey(validShardingKey);
    connectionConfig = builder.build();

    try {
      _realmAwareZkClient =
          _realmAwareZkClientFactory.buildZkClient(connectionConfig, clientConfig);
    } catch (Exception e) {
      Assert.fail("All other exceptions not allowed: " + e);
    }
  }

  /**
   * Test the persistent create() call against a valid path and an invalid path.
   * Valid path is one that belongs to the realm designated by the sharding key.
   * Invalid path is one that does not belong to the realm designated by the sharding key.
   */
  @Test(dependsOnMethods = "testRealmAwareZkClientCreation")
  public void testRealmAwareZkClientCreatePersistent() {
    _realmAwareZkClient.setZkSerializer(new ZNRecordSerializer());

    // Create a dummy ZNRecord
    ZNRecord znRecord = new ZNRecord("DummyRecord");
    znRecord.setSimpleField("Dummy", "Value");

    // Test writing and reading against the validPath
    _realmAwareZkClient.createPersistent(TEST_VALID_PATH, true);
    _realmAwareZkClient.writeData(TEST_VALID_PATH, znRecord);
    Assert.assertEquals(_realmAwareZkClient.readData(TEST_VALID_PATH), znRecord);

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
    try {
      _realmAwareZkClient.delete(TEST_INVALID_PATH);
      Assert.fail("Exists() should not succeed on an invalid path!");
    } catch (IllegalArgumentException e) {
      // Okay - expected
    }

    Assert.assertTrue(_realmAwareZkClient.delete(TEST_VALID_PATH));
    Assert.assertFalse(_realmAwareZkClient.exists(TEST_VALID_PATH));
  }
}