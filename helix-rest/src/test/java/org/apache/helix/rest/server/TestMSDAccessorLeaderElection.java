package org.apache.helix.rest.server;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.TestHelper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.common.ServletType;
import org.apache.helix.rest.metadatastore.accessor.ZkRoutingDataWriter;
import org.apache.helix.rest.server.auditlog.AuditLogger;
import org.apache.helix.rest.server.filters.CORSFilter;
import org.apache.helix.rest.server.mock.MockMetadataStoreDirectoryAccessor;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMSDAccessorLeaderElection extends MetadataStoreDirectoryAccessorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestMSDAccessorLeaderElection.class);
  private static final String MOCK_URL_PREFIX = "/mock";

  private HelixRestServer _mockHelixRestServer;
  private String _mockBaseUri;
  private String _leaderBaseUri;
  private CloseableHttpClient _httpClient;
  private HelixZkClient _zkClient;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _leaderBaseUri = getBaseUri().toString();
    _leaderBaseUri = _leaderBaseUri.substring(0, _leaderBaseUri.length() - 1);
    int newPort = getBaseUri().getPort() + 1;

    // Start a second server for testing Distributed Leader Election for writes
    _mockBaseUri = HttpConstants.HTTP_PROTOCOL_PREFIX + getBaseUri().getHost() + ":" + newPort;
    try {
      List<HelixRestNamespace> namespaces = new ArrayList<>();
      // Add test namespace
      namespaces.add(new HelixRestNamespace(TEST_NAMESPACE,
          HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, _zkAddrTestNS, false));
      _mockHelixRestServer = new MockHelixRestServer(namespaces, newPort, getBaseUri().getPath(),
          Collections.singletonList(_auditLogger));
      _mockHelixRestServer.start();
    } catch (InterruptedException e) {
      LOG.error("MockHelixRestServer starting encounter an exception.", e);
    }

    // Calling the original endpoint to create an instance of MetadataStoreDirectory in case
    // it didn't exist yet.
    get(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms", null,
        Response.Status.OK.getStatusCode(), true);

    // Set the new uri to be used in leader election
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY,
        getBaseUri().getHost());
    System
        .setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY, Integer.toString(newPort));

    // Start http client for testing
    _httpClient = HttpClients.createDefault();

    // Start zkclient to verify leader election behavior
    _zkClient = TestHelper.createZkClient(_zkAddrTestNS);
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
    _mockHelixRestServer.shutdown();
    _httpClient.close();
    _zkClient.close();
  }

  @Test
  public void testAddMetadataStoreRealmRequestForwarding()
      throws InvalidRoutingDataException, IOException {
    Set<String> expectedRealmsSet = getAllRealms();
    Assert.assertFalse(expectedRealmsSet.contains(TEST_REALM_3),
        "Metadata store directory should not have realm: " + TEST_REALM_3);
    HttpUriRequest request =
        buildRequest("/metadata-store-realms/" + TEST_REALM_3, HttpConstants.RestVerbs.PUT, "");
    sendRequestAndValidate(request, Response.Status.CREATED.getStatusCode());
    expectedRealmsSet.add(TEST_REALM_3);
    Assert.assertEquals(getAllRealms(), expectedRealmsSet);
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
  }

  @Test(dependsOnMethods = "testAddMetadataStoreRealmRequestForwarding")
  public void testDeleteMetadataStoreRealmRequestForwarding()
      throws InvalidRoutingDataException, IOException {
    Set<String> expectedRealmsSet = getAllRealms();
    HttpUriRequest request =
        buildRequest("/metadata-store-realms/" + TEST_REALM_3, HttpConstants.RestVerbs.DELETE, "");
    sendRequestAndValidate(request, Response.Status.OK.getStatusCode());
    expectedRealmsSet.remove(TEST_REALM_3);
    Assert.assertEquals(getAllRealms(), expectedRealmsSet);
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
  }

  @Test(dependsOnMethods = "testDeleteMetadataStoreRealmRequestForwarding")
  public void testAddShardingKeyRequestForwarding()
      throws InvalidRoutingDataException, IOException {
    Set<String> expectedShardingKeysSet = getAllShardingKeysInTestRealm1();
    Assert.assertFalse(expectedShardingKeysSet.contains(TEST_SHARDING_KEY),
        "Realm does not have sharding key: " + TEST_SHARDING_KEY);
    HttpUriRequest request = buildRequest(
        "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys/" + TEST_SHARDING_KEY,
        HttpConstants.RestVerbs.PUT, "");
    sendRequestAndValidate(request, Response.Status.CREATED.getStatusCode());
    expectedShardingKeysSet.add(TEST_SHARDING_KEY);
    Assert.assertEquals(getAllShardingKeysInTestRealm1(), expectedShardingKeysSet);
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
  }

  @Test(dependsOnMethods = "testAddShardingKeyRequestForwarding")
  public void testDeleteShardingKeyRequestForwarding()
      throws InvalidRoutingDataException, IOException {
    Set<String> expectedShardingKeysSet = getAllShardingKeysInTestRealm1();
    HttpUriRequest request = buildRequest(
        "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys/" + TEST_SHARDING_KEY,
        HttpConstants.RestVerbs.DELETE, "");
    sendRequestAndValidate(request, Response.Status.OK.getStatusCode());
    expectedShardingKeysSet.remove(TEST_SHARDING_KEY);
    Assert.assertEquals(getAllShardingKeysInTestRealm1(), expectedShardingKeysSet);
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
  }

  @Test(dependsOnMethods = "testDeleteShardingKeyRequestForwarding")
  public void testSetRoutingDataRequestForwarding()
      throws InvalidRoutingDataException, IOException {
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put(TEST_REALM_1, TEST_SHARDING_KEYS_2);
    routingData.put(TEST_REALM_2, TEST_SHARDING_KEYS_1);
    String routingDataString = new ObjectMapper().writeValueAsString(routingData);
    HttpUriRequest request =
        buildRequest(MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT,
            HttpConstants.RestVerbs.PUT, routingDataString);
    sendRequestAndValidate(request, Response.Status.CREATED.getStatusCode());
    Assert.assertEquals(getRawRoutingData(), routingData);
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
  }

  private HttpUriRequest buildRequest(String urlSuffix, HttpConstants.RestVerbs requestMethod,
      String jsonEntity) {
    String url = _mockBaseUri + TEST_NAMESPACE_URI_PREFIX + MOCK_URL_PREFIX + urlSuffix;
    switch (requestMethod) {
      case PUT:
        HttpPut httpPut = new HttpPut(url);
        httpPut.setEntity(new StringEntity(jsonEntity, ContentType.APPLICATION_JSON));
        return httpPut;
      case DELETE:
        return new HttpDelete(url);
      default:
        throw new IllegalArgumentException("Unsupported requestMethod: " + requestMethod);
    }
  }

  private void sendRequestAndValidate(HttpUriRequest request, int expectedResponseCode)
      throws IllegalArgumentException, IOException {
    HttpResponse response = _httpClient.execute(request);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), expectedResponseCode);

    // Validate leader election behavior
    List<String> leaderSelectionNodes =
        _zkClient.getChildren(MetadataStoreRoutingConstants.LEADER_ELECTION_ZNODE);
    leaderSelectionNodes.sort(Comparator.comparing(String::toString));
    Assert.assertEquals(leaderSelectionNodes.size(), 2);
    ZNRecord firstEphemeralNode = _zkClient.readData(
        MetadataStoreRoutingConstants.LEADER_ELECTION_ZNODE + "/" + leaderSelectionNodes.get(0));
    ZNRecord secondEphemeralNode = _zkClient.readData(
        MetadataStoreRoutingConstants.LEADER_ELECTION_ZNODE + "/" + leaderSelectionNodes.get(1));
    Assert.assertEquals(ZkRoutingDataWriter.buildEndpointFromLeaderElectionNode(firstEphemeralNode),
        _leaderBaseUri);
    Assert
        .assertEquals(ZkRoutingDataWriter.buildEndpointFromLeaderElectionNode(secondEphemeralNode),
            _mockBaseUri);

    // Make sure the operation is not done by the follower instance
    Assert.assertFalse(MockMetadataStoreDirectoryAccessor.operatedOnZk);
  }

  /**
   * A class that mocks HelixRestServer for testing. It overloads getResourceConfig to inject
   * MockMetadataStoreDirectoryAccessor as a servlet.
   */
  class MockHelixRestServer extends HelixRestServer {
    public MockHelixRestServer(List<HelixRestNamespace> namespaces, int port, String urlPrefix,
        List<AuditLogger> auditLoggers) {
      super(namespaces, port, urlPrefix, auditLoggers);
    }

    public MockHelixRestServer(String zkAddr, int port, String urlPrefix) {
      super(zkAddr, port, urlPrefix);
    }

    @Override
    protected ResourceConfig getResourceConfig(HelixRestNamespace namespace, ServletType type) {
      ResourceConfig cfg = new ResourceConfig();
      List<String> packages = new ArrayList<>(Arrays.asList(type.getServletPackageArray()));
      packages.add(MockMetadataStoreDirectoryAccessor.class.getPackage().getName());
      cfg.packages(packages.toArray(new String[0]));
      cfg.setApplicationName(namespace.getName());
      cfg.property(ContextPropertyKeys.SERVER_CONTEXT.name(),
          new ServerContext(namespace.getMetadataStoreAddress()));
      cfg.property(ContextPropertyKeys.METADATA.name(), namespace);
      cfg.register(new CORSFilter());
      return cfg;
    }
  }
}