package org.apache.helix.msdcommon.mock;

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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.constant.TestConstants;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMockMetadataStoreDirectoryServer {
  @Test
  public void testMockMetadataStoreDirectoryServer() throws IOException {
    // Start MockMSDS
    String host = "localhost";
    int port = 11000;
    String endpoint = "http://" + host + ":" + port;
    String namespace = "MY-HELIX-NAMESPACE";

    MockMetadataStoreDirectoryServer server =
        new MockMetadataStoreDirectoryServer(host, port, namespace,
            TestConstants.FAKE_ROUTING_DATA);
    server.startServer();
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      // Send a GET request for all routing data
      HttpGet getRequest = new HttpGet(
          endpoint + MockMetadataStoreDirectoryServer.REST_PREFIX + namespace
              + MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT);

      CloseableHttpResponse getResponse = httpClient.execute(getRequest);
      Map<String, Object> resultMap = MockMetadataStoreDirectoryServer.OBJECT_MAPPER
          .readValue(getResponse.getEntity().getContent(), Map.class);
      List<Map<String, Object>> routingDataList =
          (List<Map<String, Object>>) resultMap.get(MetadataStoreRoutingConstants.ROUTING_DATA);
      Collection<String> allRealms = routingDataList.stream().map(mapEntry -> (String) mapEntry
          .get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM))
          .collect(Collectors.toSet());
      Assert.assertEquals(new HashSet(allRealms), TestConstants.FAKE_ROUTING_DATA.keySet());
      Map<String, List<String>> retrievedRoutingData = routingDataList.stream().collect(Collectors
          .toMap(mapEntry -> (String) mapEntry
                  .get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM),
              mapEntry -> (List<String>) mapEntry
                  .get(MetadataStoreRoutingConstants.SHARDING_KEYS)));
      Assert.assertEquals(retrievedRoutingData, TestConstants.FAKE_ROUTING_DATA);

      // Send a GET request for all realms
      getRequest = new HttpGet(endpoint + MockMetadataStoreDirectoryServer.REST_PREFIX + namespace
          + MockMetadataStoreDirectoryServer.ZK_REALM_ENDPOINT);
      getResponse = httpClient.execute(getRequest);
      Map<String, Collection<String>> allRealmsMap = MockMetadataStoreDirectoryServer.OBJECT_MAPPER
          .readValue(getResponse.getEntity().getContent(), Map.class);
      Assert.assertTrue(
          allRealmsMap.containsKey(MetadataStoreRoutingConstants.METADATA_STORE_REALMS));
      allRealms = allRealmsMap.get(MetadataStoreRoutingConstants.METADATA_STORE_REALMS);
      Assert.assertEquals(allRealms, TestConstants.FAKE_ROUTING_DATA.keySet());

      // Send a GET request for testZkRealm
      String testZkRealm = "zk-0";
      getRequest = new HttpGet(endpoint + MockMetadataStoreDirectoryServer.REST_PREFIX + namespace
          + MockMetadataStoreDirectoryServer.ZK_REALM_ENDPOINT + "/" + testZkRealm);
      getResponse = httpClient.execute(getRequest);
      Map<String, Object> shardingKeysMap = MockMetadataStoreDirectoryServer.OBJECT_MAPPER
          .readValue(getResponse.getEntity().getContent(), Map.class);
      Assert.assertTrue(
          shardingKeysMap.containsKey(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM));
      Assert.assertTrue(shardingKeysMap.containsKey(MetadataStoreRoutingConstants.SHARDING_KEYS));
      String zkRealm =
          (String) shardingKeysMap.get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM);
      Collection<String> shardingKeyList =
          (Collection) shardingKeysMap.get(MetadataStoreRoutingConstants.SHARDING_KEYS);
      Assert.assertEquals(zkRealm, testZkRealm);
      Assert.assertEquals(shardingKeyList, TestConstants.FAKE_ROUTING_DATA.get(testZkRealm));

      // Try sending a POST request (not supported)
      HttpPost postRequest = new HttpPost(
          endpoint + MockMetadataStoreDirectoryServer.REST_PREFIX + namespace
              + MockMetadataStoreDirectoryServer.ZK_REALM_ENDPOINT + "/" + testZkRealm);
      CloseableHttpResponse postResponse = httpClient.execute(postRequest);
    } finally {
      // Shutdown
      server.stopServer();
    }
  }
}
