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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.apache.helix.msdcommon.datamodel.TrieRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestFederatedZkClient extends ZkTestBase {
  private static final String TEST_SHARDING_KEY_PREFIX = "/test_sharding_key_";
  private static final String TEST_VALID_PATH = TEST_SHARDING_KEY_PREFIX + "0/a/b/c";
  private static final String TEST_INVALID_PATH = TEST_SHARDING_KEY_PREFIX + "invalid/a/b/c";
  private static final String UNSUPPORTED_OPERATION_MESSAGE =
      "Session-aware operation is not supported by FederatedZkClient.";

  private RealmAwareZkClient _realmAwareZkClient;

  @BeforeClass
  public void beforeClass() throws InvalidRoutingDataException {
    // Populate rawRoutingData
    // <Realm, List of sharding keys> Mapping
    Map<String, List<String>> rawRoutingData = new HashMap<>();
    for (int i = 0; i < _numZk; i++) {
      List<String> shardingKeyList = Collections.singletonList(TEST_SHARDING_KEY_PREFIX + i);
      String realmName = ZK_PREFIX + (ZK_START_PORT + i);
      rawRoutingData.put(realmName, shardingKeyList);
    }

    // Feed the raw routing data into TrieRoutingData to construct an in-memory representation
    // of routing information.
    _realmAwareZkClient = new FederatedZkClient(new RealmAwareZkClient.RealmAwareZkClientConfig(),
        new TrieRoutingData(rawRoutingData));
  }

  @AfterClass
  public void afterClass() {
    // Close it as it is created in before class.
    _realmAwareZkClient.close();
  }

  /*
   * Tests that an unsupported operation should throw an UnsupportedOperationException.
   */
  @Test
  public void testUnsupportedOperations() {
    // Test creating ephemeral.
    try {
      _realmAwareZkClient.create(TEST_VALID_PATH, "Hello", CreateMode.EPHEMERAL);
      Assert.fail("Ephemeral node should not be created.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage()
          .startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    // Test creating ephemeral sequential.
    try {
      _realmAwareZkClient.create(TEST_VALID_PATH, "Hello", CreateMode.EPHEMERAL_SEQUENTIAL);
      Assert.fail("Ephemeral node should not be created.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage()
          .startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    List<Op> ops = Arrays.asList(
        Op.create(TEST_VALID_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
        Op.delete(TEST_VALID_PATH, -1));
    try {
      _realmAwareZkClient.multi(ops);
      Assert.fail("multi() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage()
          .startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    try {
      _realmAwareZkClient.getSessionId();
      Assert.fail("getSessionId() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage()
          .startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    try {
      _realmAwareZkClient.getServers();
      Assert.fail("getServers() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage()
          .startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    try {
      _realmAwareZkClient.waitUntilConnected(5L, TimeUnit.SECONDS);
      Assert.fail("getServers() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage()
          .startsWith(UNSUPPORTED_OPERATION_MESSAGE));
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
      Assert.assertTrue(ex.getMessage()
          .startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }

    try {
      _realmAwareZkClient.unsubscribeStateChanges(listener);
      Assert.fail("getServers() should not be supported.");
    } catch (UnsupportedOperationException ex) {
      Assert.assertTrue(ex.getMessage()
          .startsWith(UNSUPPORTED_OPERATION_MESSAGE));
    }
  }

  /*
   * Tests the persistent create() call against a valid path and an invalid path.
   * Valid path is one that belongs to the realm designated by the sharding key.
   * Invalid path is one that does not belong to the realm designated by the sharding key.
   */
  @Test(dependsOnMethods = "testUnsupportedOperations")
  public void testCreatePersistent() {
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
    } catch (NoSuchElementException ex) {
      Assert
          .assertEquals(ex.getMessage(), "Cannot find ZK realm for the path: " + TEST_INVALID_PATH);
    }
  }

  /*
   * Tests that exists() works on valid path and fails on invalid path.
   */
  @Test(dependsOnMethods = "testCreatePersistent")
  public void testExists() {
    Assert.assertTrue(_realmAwareZkClient.exists(TEST_VALID_PATH));

    try {
      _realmAwareZkClient.exists(TEST_INVALID_PATH);
      Assert.fail("Exists() should not succeed on an invalid path!");
    } catch (NoSuchElementException ex) {
      Assert
          .assertEquals(ex.getMessage(), "Cannot find ZK realm for the path: " + TEST_INVALID_PATH);
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
      Assert
          .assertEquals(ex.getMessage(), "Cannot find ZK realm for the path: " + TEST_INVALID_PATH);
    }

    Assert.assertTrue(_realmAwareZkClient.delete(TEST_VALID_PATH));
    Assert.assertFalse(_realmAwareZkClient.exists(TEST_VALID_PATH));
  }

  /*
   * Tests that close() works.
   */
  @Test(dependsOnMethods = "testDelete")
  public void testClose() {
    Assert.assertFalse(_realmAwareZkClient.isClosed());

    _realmAwareZkClient.close();

    Assert.assertTrue(_realmAwareZkClient.isClosed());

    // Client is closed, so operation should not be executed.
    try {
      _realmAwareZkClient.createPersistent(TEST_VALID_PATH);
      Assert.fail("createPersistent() should not be executed because RealmAwareZkClient is closed.");
    } catch (IllegalStateException ex) {
      Assert.assertEquals(ex.getMessage(), "FederatedZkClient is closed!");
    }
  }
}
