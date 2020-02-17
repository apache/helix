package org.apache.helix.msdserver.mock;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Test;
import org.testng.Assert;


public class TestMockMetadataStoreDirectoryServer {
  @Test
  public void testMockMetadataStoreDirectoryServer() throws IOException {
    // Create fake routing data
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put("zk-0", ImmutableList.of("sharding-key-0", "sharding-key-1", "sharding-key-2"));
    routingData.put("zk-1", ImmutableList.of("sharding-key-3", "sharding-key-4", "sharding-key-5"));
    routingData.put("zk-2", ImmutableList.of("sharding-key-6", "sharding-key-7", "sharding-key-8"));

    // Start MockMSDS
    String host = "localhost";
    int port = 11000;
    String endpoint = "http://" + host + ":" + port;
    String namespace = "MY-HELIX-NAMESPACE";
    MockMetadataStoreDirectoryServer server =
        new MockMetadataStoreDirectoryServer(host, port, namespace, routingData);
    server.startServer();
    CloseableHttpClient httpClient = HttpClients.createDefault();

    // Send a GET request
    String testZkRealm = "zk-0";
    HttpGet getRequest = new HttpGet(
        endpoint + MockMetadataStoreDirectoryServer.REST_PREFIX + namespace
            + MockMetadataStoreDirectoryServer.ZK_REALM_ENDPOINT + testZkRealm);
    try {
      CloseableHttpResponse getResponse = httpClient.execute(getRequest);
      List<String> shardingKeyList = MockMetadataStoreDirectoryServer.OBJECT_MAPPER
          .readValue(getResponse.getEntity().getContent(), List.class);
      Assert.assertEquals(shardingKeyList, routingData.get(testZkRealm));
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Try sending a POST request (not supported)
    HttpPost postRequest = new HttpPost(
        endpoint + MockMetadataStoreDirectoryServer.REST_PREFIX + namespace
            + MockMetadataStoreDirectoryServer.ZK_REALM_ENDPOINT + testZkRealm);
    try {
      CloseableHttpResponse postResponse = httpClient.execute(postRequest);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Shutdown
    server.stopServer();
  }
}
