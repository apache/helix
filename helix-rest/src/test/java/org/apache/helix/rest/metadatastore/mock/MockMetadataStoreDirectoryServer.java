package org.apache.helix.rest.metadatastore.mock;

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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.testng.Assert;


/**
 * Mock HTTP server that serves GET of metadata store routing data only.
 * Helix applications may use this to write unit/integration tests without having to set up the routing ZooKeeper and creating routing data ZNodes.
 */
public class MockMetadataStoreDirectoryServer {

  private static final String REST_PREFIX = "/admin/v2/namespaces/";
  private static final String ZK_REALM_ENDPOINT = "/zk-realm/";
  private static final int NOT_FOUND = 501;
  private static final int OK = 200;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String _hostname;
  private final int _mockServerPort;
  private final Map<String, List<String>> _routingDataMap;
  private final String _namespace;
  private HttpServer _server;
  private final ThreadPoolExecutor _executor =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

  private enum SupportedHttpVerbs {
    GET
  }

  /**
   * Constructs a Mock MSDS.
   * A sample GET might look like the following:
   *     curl localhost:11000/admin/v2/namespaces/MY-HELIX-APP/zk-realm/zk-1
   * @param hostname hostname for the REST server. E.g.) "localhost"
   * @param port port to use. E.g.) 11000
   * @param namespace the Helix REST namespace to mock. E.g.) "MY-HELIX-APP"
   * @param routingData <ZK realm, List of ZK path sharding keys>
   */
  public MockMetadataStoreDirectoryServer(String hostname, int port, String namespace,
      Map<String, List<String>> routingData) {
    _hostname = hostname;
    _mockServerPort = port;
    _namespace = namespace;
    _routingDataMap = routingData;
  }

  public void startServer()
      throws IOException {
    _server = HttpServer.create(new InetSocketAddress(_hostname, _mockServerPort), 0);
    generateContexts();
    _server.setExecutor(_executor);
    _server.start();
    System.out.println("Started Mock MSDS at " + _hostname + ":" + _mockServerPort + "!");
  }

  public void stopServer() {
    _server.stop(0);
    _executor.shutdown();
  }

  /**
   * Dynamically generates HTTP server contexts based on the routing data given.
   */
  private void generateContexts() {
    _routingDataMap.forEach((zkRealm, shardingKeyList) -> _server
        .createContext(REST_PREFIX + _namespace + ZK_REALM_ENDPOINT + zkRealm, httpExchange -> {
          OutputStream outputStream = httpExchange.getResponseBody();
          String htmlResponse;
          if (SupportedHttpVerbs.GET.name().equals(httpExchange.getRequestMethod())) {
            htmlResponse = OBJECT_MAPPER.writeValueAsString(shardingKeyList);
            httpExchange.sendResponseHeaders(OK, htmlResponse.length());
          } else {
            htmlResponse = httpExchange.getRequestMethod() + " is not supported!\n";
            httpExchange.sendResponseHeaders(NOT_FOUND, htmlResponse.length());
          }
          outputStream.write(htmlResponse.getBytes());
          outputStream.flush();
          outputStream.close();
        }));
  }

  /**
   * Spins up MockMetadataStoreDirectoryServer and performs tests.
   * A sample curl GET query might look like the following:
   *     curl localhost:11000/admin/v2/namespaces/MY-HELIX-APP/zk-realm/zk-1
   * @param args
   * @throws IOException
   */
  public static void main(String[] args)
      throws IOException {
    // Create fake routing data
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put("zk-0", ImmutableList.of("sharding-key-0", "sharding-key-1", "sharding-key-2"));
    routingData.put("zk-1", ImmutableList.of("sharding-key-3", "sharding-key-4", "sharding-key-5"));
    routingData.put("zk-2", ImmutableList.of("sharding-key-6", "sharding-key-7", "sharding-key-8"));

    // Start MockMSDS
    String host = "localhost";
    int port = 11000;
    String namespace = "MY-HELIX-APP";
    MockMetadataStoreDirectoryServer server =
        new MockMetadataStoreDirectoryServer(host, port, namespace, routingData);
    server.startServer();
    CloseableHttpClient httpClient = HttpClients.createDefault();

    // Send a GET request
    String testZkRealm = "zk-0";
    HttpGet getRequest = new HttpGet(
        "http://localhost:" + port + REST_PREFIX + namespace + ZK_REALM_ENDPOINT + testZkRealm);
    try {
      CloseableHttpResponse getResponse = httpClient.execute(getRequest);
      System.out.println(getResponse.toString());
      List<String> shardingKeyList =
          OBJECT_MAPPER.readValue(getResponse.getEntity().getContent(), List.class);
      System.out.println(shardingKeyList);
      Assert.assertEquals(shardingKeyList, routingData.get(testZkRealm));
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Try sending a POST request (not supported)
    HttpPost postRequest = new HttpPost(
        "http://localhost:" + port + REST_PREFIX + namespace + ZK_REALM_ENDPOINT + testZkRealm);
    try {
      CloseableHttpResponse postResponse = httpClient.execute(postRequest);
      System.out.println(postResponse.toString());
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Shutdown
    server.stopServer();
    System.out.println("MockMetadataStoreDirectoryServer test passed!");
  }
}
