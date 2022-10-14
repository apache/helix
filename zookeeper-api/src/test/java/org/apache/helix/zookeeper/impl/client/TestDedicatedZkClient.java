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
import java.util.Arrays;
import java.util.List;

import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDedicatedZkClient extends RealmAwareZkClientFactoryTestBase {

  private static final String PARENT_PATH = ZK_SHARDING_KEY_PREFIX + "/TestDedicatedZkClient";
  public static RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder builder;
  public static RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig;

  @BeforeClass
  public void beforeClass() throws IOException, InvalidRoutingDataException {
    super.beforeClass();
    // Set the factory to DedicatedZkClientFactory
    _realmAwareZkClientFactory = DedicatedZkClientFactory.getInstance();
    // Create a RealmAwareZkClient
    builder = new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
    connectionConfig = builder.setZkRealmShardingKey(ZK_SHARDING_KEY_PREFIX).build();
    _realmAwareZkClient = _realmAwareZkClientFactory.buildZkClient(connectionConfig, new RealmAwareZkClient.RealmAwareZkClientConfig());
    _realmAwareZkClient.createPersistent(ZK_SHARDING_KEY_PREFIX, true);
  }

  @AfterClass
  public void afterClass() {
    _realmAwareZkClient.close();
  }

  /**
   * Test that zk multi works for op.create.
   */
  @Test
  public void test_multi_create() {
    String test_name = "/test_multi_create";

    //Create Nodes
    List<Op> ops = Arrays.asList(
            Op.create(PARENT_PATH, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.create(PARENT_PATH + test_name, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

    //Execute transactional support on operations and verify they were run
    List<OpResult> opResults = _realmAwareZkClient.multi(ops);
    Assert.assertTrue(opResults.get(0) instanceof OpResult.CreateResult);
    Assert.assertTrue(opResults.get(1) instanceof OpResult.CreateResult);

    cleanup();
  }

  /**
   * Multi should be an all or nothing transaction. Creating correct
   * paths and a singular bad one should all fail.
   */
  @Test
  public void test_multi_fail() {
    String test_name = "/test_multi_fail";
    //Create Nodes
    List<Op> ops = Arrays.asList(
            Op.create(PARENT_PATH, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.create(PARENT_PATH + test_name, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.create(TEST_INVALID_PATH, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
    try {
      _realmAwareZkClient.multi(ops);
      Assert.fail("Should have thrown an exception. Cannot run multi on incorrect path.");
    } catch (ZkNoNodeException e) {
      boolean pathExists = _realmAwareZkClient.exists(PARENT_PATH);
      Assert.assertFalse(pathExists, "Path should not have been created.");

      cleanup();
    }
  }

  /**
   * Test that zk multi works for delete.
   */
  @Test
  public void test_multi_delete() {
    String test_name = "/test_multi_delete";
    //Create Nodes
    List<Op> ops = Arrays.asList(
            Op.create(PARENT_PATH, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.create(PARENT_PATH + test_name, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.delete(PARENT_PATH + test_name, -1));

    List<OpResult> opResults = _realmAwareZkClient.multi(ops);
    Assert.assertTrue(opResults.get(0) instanceof OpResult.CreateResult);
    Assert.assertTrue(opResults.get(1) instanceof OpResult.CreateResult);
    Assert.assertTrue(opResults.get(2) instanceof OpResult.DeleteResult);

    cleanup();
  }

  /**
   * Test that zk multi works for set.
   */
  @Test
  public void test_multi_set() {
    String test_name = "/test_multi_set";

    List<Op> ops = Arrays.asList(
            Op.create(PARENT_PATH, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.create(PARENT_PATH + test_name, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.setData(PARENT_PATH + test_name, new byte[0],
                    -1));

    List<OpResult> opResults = _realmAwareZkClient.multi(ops);
    Assert.assertTrue(opResults.get(0) instanceof OpResult.CreateResult);
    Assert.assertTrue(opResults.get(1) instanceof OpResult.CreateResult);
    Assert.assertTrue(opResults.get(2) instanceof OpResult.SetDataResult);

    cleanup();
  }

  /**
   * Delete created paths to clean up zk for next test case.
   */
  public void cleanup() {
    //Delete Parent path and its children
    _realmAwareZkClient.deleteRecursively(PARENT_PATH);
    //Verify path has been deleted
    boolean pathExists = _realmAwareZkClient.exists(PARENT_PATH);
    Assert.assertFalse(pathExists, "Parent Path should have been removed.");
  }
}
