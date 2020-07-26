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

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.zookeeper.constant.TestConstants;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public abstract class RealmAwareZkClientTestBase extends ZkTestBase {
  protected static final String ZK_SHARDING_KEY_PREFIX = "/sharding-key-0";
  protected static final String TEST_VALID_PATH = ZK_SHARDING_KEY_PREFIX + "/a/b/c";
  protected static final String TEST_INVALID_PATH = ZK_SHARDING_KEY_PREFIX + "_invalid" + "/a/b/c";

  // Create a MockMSDS for testing
  protected static MockMetadataStoreDirectoryServer _msdsServer;
  protected static final String MSDS_HOSTNAME = "localhost";
  protected static final int MSDS_PORT = 19910;
  protected static final String MSDS_NAMESPACE = "test";

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
}