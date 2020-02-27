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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.datamodel.TrieRoutingData;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.api.factory.RealmAwareZkClientFactory;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public abstract class RealmAwareZkClientTestBase extends ZkTestBase {
  protected static final String ZK_SHARDING_KEY_PREFIX = "/TEST_SHARDING_KEY";
  protected static final String TEST_VALID_PATH = ZK_SHARDING_KEY_PREFIX + "_" + 0 + "/a/b/c";
  protected static final String TEST_INVALID_PATH = ZK_SHARDING_KEY_PREFIX + "_invalid" + "/a/b/c";

  // <Realm, List of sharding keys> Mapping
  private static final Map<String, List<String>> RAW_ROUTING_DATA = new HashMap<>();

  // The following RealmAwareZkClientFactory is to be defined in subclasses
  protected RealmAwareZkClientFactory _realmAwareZkClientFactory;
  protected RealmAwareZkClient _realmAwareZkClient;
  private MetadataStoreRoutingData _metadataStoreRoutingData;

  @BeforeClass
  public void beforeClass() throws Exception {
    // Populate RAW_ROUTING_DATA
    for (int i = 0; i < _numZk; i++) {
      List<String> shardingKeyList = new ArrayList<>();
      shardingKeyList.add(ZK_SHARDING_KEY_PREFIX + "_" + i);
      String realmName = ZK_PREFIX + (ZK_START_PORT + i);
      RAW_ROUTING_DATA.put(realmName, shardingKeyList);
    }

    // Feed the raw routing data into TrieRoutingData to construct an in-memory representation of routing information
    _metadataStoreRoutingData = new TrieRoutingData(RAW_ROUTING_DATA);
  }

  @AfterClass
  public void afterClass() {
    if (_realmAwareZkClient != null && !_realmAwareZkClient.isClosed()) {
      _realmAwareZkClient.close();
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
    String invalidShardingKey = "InvalidShardingKey";
    RealmAwareZkClient.RealmAwareZkClientConfig clientConfig =
        new RealmAwareZkClient.RealmAwareZkClientConfig();

    // Create a connection config with the invalid sharding key
    RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig(invalidShardingKey);

    try {
      _realmAwareZkClient = _realmAwareZkClientFactory
          .buildZkClient(connectionConfig, clientConfig, _metadataStoreRoutingData);
      Assert.fail("Should not succeed with an invalid sharding key!");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    // Use a valid sharding key this time around
    String validShardingKey = ZK_SHARDING_KEY_PREFIX + "_" + 0; // Use TEST_SHARDING_KEY_0
    connectionConfig = new RealmAwareZkClient.RealmAwareZkConnectionConfig(validShardingKey);
    _realmAwareZkClient = _realmAwareZkClientFactory
        .buildZkClient(connectionConfig, clientConfig, _metadataStoreRoutingData);
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