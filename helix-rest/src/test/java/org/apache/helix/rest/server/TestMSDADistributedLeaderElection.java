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
import java.util.List;
import java.util.Set;
import javax.ws.rs.core.Response;

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.ServletType;
import org.apache.helix.rest.server.auditlog.AuditLogger;
import org.apache.helix.rest.server.filters.CORSFilter;
import org.apache.helix.rest.server.resources.mock.MockMetadataStoreDirectoryAccessor;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.glassfish.jersey.server.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants.LEADER_ELECTION_ZNODE;


public class TestMSDADistributedLeaderElection extends MetadataStoreDirectoryAccessorTestBase {
  private final String MOCK_URL_PREFIX = "/mock";

  private HelixRestServer _mockHelixRestServer;
  private String _mockBaseUri;
  private String _leaderBaseUri;
  private CloseableHttpClient _httpClient;
  private HelixZkClient _zkClient;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _leaderBaseUri = getBaseUri().toString();

    // Start a second server for testing Distributed Leader Election for writes
    _mockBaseUri =
        getBaseUri().getScheme() + "://" + getBaseUri().getHost() + ":" + (getBaseUri().getPort()
            + 1);
    try {
      List<HelixRestNamespace> namespaces = new ArrayList<>();
      // Add test namespace
      namespaces.add(new HelixRestNamespace(TEST_NAMESPACE,
          HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, _zkAddrTestNS, false));
      _mockHelixRestServer =
          new MockHelixRestServer(namespaces, getBaseUri().getPort() + 1, getBaseUri().getPath(),
              Collections.singletonList(_auditLogger));
      _mockHelixRestServer.start();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Calling the original endpoint to create an instance of MetadataStoreDirectory in case
    // it didn't exist yet.
    get(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms", null,
        Response.Status.OK.getStatusCode(), true);

    // Set the new uri to be used in leader election
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY, _mockBaseUri);

    // Start http client for testing
    _httpClient = HttpClients.createDefault();

    // Start zkclient to verify leader election behavior
    _zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(_zkAddrTestNS),
            new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));
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
    Set<String> expectedRealmsSet = getAllMetadataStoreRealmsHelper();
    Assert.assertFalse(expectedRealmsSet.contains(TEST_REALM_3),
        "Metadata store directory should not have realm: " + TEST_REALM_3);
    sendRequestAndValidate("/metadata-store-realms/" + TEST_REALM_3, "put",
        Response.Status.CREATED.getStatusCode());
    expectedRealmsSet.add(TEST_REALM_3);
    Assert.assertEquals(getAllMetadataStoreRealmsHelper(), expectedRealmsSet);
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
  }

  @Test(dependsOnMethods = "testAddMetadataStoreRealmRequestForwarding")
  public void testDeleteMetadataStoreRealmRequestForwarding()
      throws InvalidRoutingDataException, IOException {
    Set<String> expectedRealmsSet = getAllMetadataStoreRealmsHelper();
    sendRequestAndValidate("/metadata-store-realms/" + TEST_REALM_3, "delete",
        Response.Status.OK.getStatusCode());
    expectedRealmsSet.remove(TEST_REALM_3);
    Assert.assertEquals(getAllMetadataStoreRealmsHelper(), expectedRealmsSet);
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
  }

  @Test(dependsOnMethods = "testDeleteMetadataStoreRealmRequestForwarding")
  public void testAddShardingKeyRequestForwarding()
      throws InvalidRoutingDataException, IOException {
    Set<String> expectedShardingKeysSet = getShardingKeysInRealmHelper();
    Assert.assertFalse(expectedShardingKeysSet.contains(TEST_SHARDING_KEY),
        "Realm does not have sharding key: " + TEST_SHARDING_KEY);
    sendRequestAndValidate(
        "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys/" + TEST_SHARDING_KEY, "put",
        Response.Status.CREATED.getStatusCode());
    expectedShardingKeysSet.add(TEST_SHARDING_KEY);
    Assert.assertEquals(getShardingKeysInRealmHelper(), expectedShardingKeysSet);
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
  }

  @Test(dependsOnMethods = "testAddShardingKeyRequestForwarding")
  public void testDeleteShardingKeyRequestForwarding()
      throws InvalidRoutingDataException, IOException {
    Set<String> expectedShardingKeysSet = getShardingKeysInRealmHelper();
    sendRequestAndValidate(
        "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys/" + TEST_SHARDING_KEY, "delete",
        Response.Status.OK.getStatusCode());
    expectedShardingKeysSet.remove(TEST_SHARDING_KEY);
    Assert.assertEquals(getShardingKeysInRealmHelper(), expectedShardingKeysSet);
    MockMetadataStoreDirectoryAccessor._mockMSDInstance.close();
  }

  private void sendRequestAndValidate(String url_suffix, String request_method,
      int expectedResponseCode) throws IllegalArgumentException, IOException {
    String url = _mockBaseUri + TEST_NAMESPACE_URI_PREFIX + MOCK_URL_PREFIX + url_suffix;
    HttpUriRequest request;
    switch (request_method) {
      case "put":
        request = new HttpPut(url);
        break;
      case "delete":
        request = new HttpDelete(url);
        break;
      default:
        throw new IllegalArgumentException("Unsupported request_method: " + request_method);
    }
    HttpResponse response = _httpClient.execute(request);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), expectedResponseCode);

    // Validate leader election behavior
    List<String> leaderSelectionNodes = _zkClient.getChildren(LEADER_ELECTION_ZNODE);
    leaderSelectionNodes.sort(Comparator.comparing(String::toString));
    Assert.assertEquals(leaderSelectionNodes.size(), 2);
    ZNRecord firstEphemeralNode =
        _zkClient.readData(LEADER_ELECTION_ZNODE + "/" + leaderSelectionNodes.get(0));
    ZNRecord secondEphemeralNode =
        _zkClient.readData(LEADER_ELECTION_ZNODE + "/" + leaderSelectionNodes.get(1));
    Assert.assertEquals(firstEphemeralNode.getId(), _leaderBaseUri);
    Assert.assertEquals(secondEphemeralNode.getId(), _mockBaseUri);

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