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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mock HTTP server that serves GET of metadata store routing data only.
 * Helix applications may use this to write unit/integration tests without having to set up the routing ZooKeeper and creating routing data ZNodes.
 */
public class MockMetadataStoreDirectoryServer {
  private static final Logger LOG = LoggerFactory.getLogger(MockMetadataStoreDirectoryServer.class);

  protected static final String REST_PREFIX = "/admin/v2/namespaces/";
  protected static final String ZK_REALM_ENDPOINT = "/METADATA_STORE_ROUTING_DATA/";
  protected static final int NOT_IMPLEMENTED = 501;
  protected static final int OK = 200;
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final String _hostname;
  protected final int _mockServerPort;
  protected final Map<String, List<String>> _routingDataMap;
  protected final String _namespace;
  protected HttpServer _server;
  protected final ThreadPoolExecutor _executor =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

  protected enum SupportedHttpVerbs {
    GET
  }

  /**
   * Constructs a Mock MSDS.
   * A sample GET might look like the following:
   *     curl localhost:11000/admin/v2/namespaces/MY-HELIX-NAMESPACE/METADATA_STORE_ROUTING_DATA/zk-1
   * @param hostname hostname for the REST server. E.g.) "localhost"
   * @param port port to use. E.g.) 11000
   * @param namespace the Helix REST namespace to mock. E.g.) "MY-HELIX-NAMESPACE"
   * @param routingData <ZK realm, List of ZK path sharding keys>
   */
  public MockMetadataStoreDirectoryServer(String hostname, int port, String namespace,
      Map<String, List<String>> routingData) {
    if (hostname == null || hostname.isEmpty()) {
      throw new IllegalArgumentException("hostname cannot be null or empty!");
    }
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("port is not a valid port!");
    }
    if (namespace == null || namespace.isEmpty()) {
      throw new IllegalArgumentException("namespace cannot be null or empty!");
    }
    if (routingData == null || routingData.isEmpty()) {
      throw new IllegalArgumentException("routingData cannot be null or empty!");
    }
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
    LOG.info(
        "Started MockMetadataStoreDirectoryServer at " + _hostname + ":" + _mockServerPort + "!");
  }

  public void stopServer() {
    _server.stop(0);
    _executor.shutdown();
    LOG.info(
        "Stopped MockMetadataStoreDirectoryServer at " + _hostname + ":" + _mockServerPort + "!");
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
            httpExchange.sendResponseHeaders(NOT_IMPLEMENTED, htmlResponse.length());
          }
          outputStream.write(htmlResponse.getBytes());
          outputStream.flush();
          outputStream.close();
        }));
  }
}
