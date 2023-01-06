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

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.api.factory.RealmAwareZkClientFactory;
import org.apache.helix.zookeeper.constant.TestConstants;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public abstract class RealmAwareZkClientTestBase extends ZkTestBase {
  protected static final String ZK_SHARDING_KEY_PREFIX = "/sharding-key-0";
  protected static final String TEST_VALID_PATH = ZK_SHARDING_KEY_PREFIX + "/a/b/c";
  protected static final String TEST_INVALID_PATH = ZK_SHARDING_KEY_PREFIX + "_invalid" + "/a/b/c";

  // Create a MockMSDS for testing
  protected static MockMetadataStoreDirectoryServer _msdsServer;
  protected static final String MSDS_HOSTNAME = "localhost";
  protected static final int MSDS_PORT = 19910;
  protected static final String MSDS_NAMESPACE = "test";
  protected static String PARENT_PATH = ZK_SHARDING_KEY_PREFIX + "/RealmAwareZkClient";
  protected RealmAwareZkClient _realmAwareZkClient;
  protected RealmAwareZkClientFactory _realmAwareZkClientFactory;

  @BeforeClass
  public void beforeClass() throws IOException, InvalidRoutingDataException {
    // Create a mock MSDS so that HttpRoutingDataReader could fetch the routing data
    if (_msdsServer == null) {
      // Do not create again if Mock MSDS server has already been created by other tests
      _msdsServer = new MockMetadataStoreDirectoryServer(MSDS_HOSTNAME, MSDS_PORT, MSDS_NAMESPACE,
              TestConstants.FAKE_ROUTING_DATA);
      _msdsServer.startServer();
    }

    // Register the MSDS endpoint as a System variable
    String msdsEndpoint =
            "http://" + MSDS_HOSTNAME + ":" + MSDS_PORT + "/admin/v2/namespaces/" + MSDS_NAMESPACE;
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, msdsEndpoint);
  }

  @AfterClass
  public void afterClass() {
    if (_msdsServer != null) {
      _msdsServer.stopServer();
    }
  }
  /**
   * Initialize requirement for testing multi support.
   */
  @Test
  public void testMultiSetup() {
    // Create a connection config with a valid sharding key
    RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder builder =
            new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
    RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
            builder.setZkRealmShardingKey(ZK_SHARDING_KEY_PREFIX).build();
    try {
      _realmAwareZkClient = new FederatedZkClient(connectionConfig,
              new RealmAwareZkClient.RealmAwareZkClientConfig());
    } catch (IllegalArgumentException e) {
      Assert.fail("Invalid Sharding Key.");
    } catch (Exception e) {
      Assert.fail("Should not see any other types of Exceptions: " + e);
    }
  }

  /**
   * Test that zk multi works for create.
   */
  @Test(dependsOnMethods = "testMultiSetup")
  public void testMultiCreate() {
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

    //Verify that the znodes were created
    Assert.assertTrue(_realmAwareZkClient.exists(PARENT_PATH), "Path has not been created.");
    Assert.assertTrue(_realmAwareZkClient.exists(PARENT_PATH + test_name), "Path has not been created.");

    cleanup();
  }

  /**
   * Multi should be an all or nothing transaction. Creating correct
   * paths and a singular bad one should all fail.
   */
  @Test(dependsOnMethods = "testMultiCreate")
  public void testMultiFail() {
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
    } catch (Exception e) {
      boolean pathExists = _realmAwareZkClient.exists(PARENT_PATH);
      Assert.assertFalse(pathExists, "Path should not have been created.");

      cleanup();
    }
  }

  /**
   * Test that zk multi works for delete.
   */
  @Test(dependsOnMethods = "testMultiFail")
  public void testMultiDelete() {
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

    Assert.assertTrue(_realmAwareZkClient.exists(PARENT_PATH), "Path has not been created.");
    Assert.assertFalse(_realmAwareZkClient.exists(PARENT_PATH + test_name), "Path should have been removed.");

    cleanup();
  }

  /**
   * Test that zk multi works for set.
   */
  @Test(dependsOnMethods = "testMultiDelete")
  public void testMultiSet() {
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

    Assert.assertTrue(_realmAwareZkClient.exists(PARENT_PATH), "Path has not been created.");
    Assert.assertTrue(_realmAwareZkClient.exists(PARENT_PATH + test_name), "Path has not been created.");

    cleanup();
  }

  /**
   * Delete created paths to clean up zk for next multi test case.
   */
  public void cleanup() {
    //Delete Parent path and its children
    _realmAwareZkClient.deleteRecursively(PARENT_PATH);
    //Verify path has been deleted
    boolean pathExists = _realmAwareZkClient.exists(PARENT_PATH);
    Assert.assertFalse(pathExists, "Parent Path should have been removed.");
  }
}