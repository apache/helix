package org.apache.helix.zookeeper.routing;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.zookeeper.constant.TestConstants;
import org.apache.helix.zookeeper.exception.MultiZkException;
import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHttpZkFallbackRoutingDataReader extends ZkTestBase {
  private MockMetadataStoreDirectoryServer _msdsServer;
  private static final String HOST = "localhost";
  private static final int PORT = 1992;
  private static final String NAMESPACE = "TestHttpZkFallbackRoutingDataReader";
  private static final String MSDS_ENDPOINT =
      "http://" + HOST + ":" + PORT + "/admin/v2/namespaces/" + NAMESPACE;

  @BeforeClass
  public void beforeClass() throws IOException {
    // Start MockMSDS
    _msdsServer = new MockMetadataStoreDirectoryServer(HOST, PORT, NAMESPACE,
        TestConstants.FAKE_ROUTING_DATA);
    _msdsServer.startServer();
  }

  @Test
  public void testGetRawRoutingData() {
    HttpZkFallbackRoutingDataReader reader = new HttpZkFallbackRoutingDataReader();

    // This read should read from HTTP
    String endpointString = MSDS_ENDPOINT + "," + ZK_ADDR;
    Map<String, List<String>> rawRoutingData = reader.getRawRoutingData(endpointString);
    TestConstants.FAKE_ROUTING_DATA.forEach((realm, keys) -> Assert
        .assertEquals(new HashSet(rawRoutingData.get(realm)), new HashSet(keys)));

    // Shut down MSDS so that it would read from ZK (fallback)
    _msdsServer.stopServer();
    try {
      reader.getRawRoutingData(endpointString);
      Assert.fail("Must encounter a MultiZkException since the path in ZK does not exist!");
    } catch (MultiZkException e) {
      // Check the exception message to ensure that it's reading from ZK
      Assert.assertTrue(e.getMessage()
          .contains("Routing data directory ZNode /METADATA_STORE_ROUTING_DATA does not exist"));
    }
  }
}
