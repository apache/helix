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
import java.util.Collections;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.helix.TestHelper;
import org.apache.helix.rest.acl.NoopAclRegister;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.authValidator.AuthValidator;
import org.apache.helix.rest.server.resources.helix.ClusterAccessor;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


public class TestAuthValidator extends AbstractTestClass {
  private String _mockBaseUri;
  private CloseableHttpClient _httpClient;

  private static String CLASSNAME_TEST_DEFAULT_AUTH = "testDefaultAuthValidator";
  private static String CLASSNAME_TEST_CST_AUTH = "testCustomAuthValidator";

  @AfterClass
  public void afterClass() {
    TestHelper.dropCluster(CLASSNAME_TEST_DEFAULT_AUTH, _gZkClient);
    TestHelper.dropCluster(CLASSNAME_TEST_CST_AUTH, _gZkClient);
  }

  @Test
  public void testDefaultAuthValidator() throws JsonProcessingException {
    put("clusters/" + CLASSNAME_TEST_DEFAULT_AUTH, null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());
    String body = get("clusters/", null, Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String clustersStr = node.get(ClusterAccessor.ClusterProperties.clusters.name()).toString();
    Assert.assertTrue(clustersStr.contains(CLASSNAME_TEST_DEFAULT_AUTH));
  }

  @Test(dependsOnMethods = "testDefaultAuthValidator")
  public void testCustomAuthValidator() throws IOException, InterruptedException {
    int newPort = getBaseUri().getPort() + 1;

    // Start a second server for testing Distributed Leader Election for writes
    _mockBaseUri = HttpConstants.HTTP_PROTOCOL_PREFIX + getBaseUri().getHost() + ":" + newPort;
    _httpClient = HttpClients.createDefault();

    AuthValidator mockAuthValidatorPass = Mockito.mock(AuthValidator.class);
    when(mockAuthValidatorPass.validate(any())).thenReturn(true);
    AuthValidator mockAuthValidatorReject = Mockito.mock(AuthValidator.class);
    when(mockAuthValidatorReject.validate(any())).thenReturn(false);

    List<HelixRestNamespace> namespaces = new ArrayList<>();
    namespaces.add(new HelixRestNamespace(HelixRestNamespace.DEFAULT_NAMESPACE_NAME,
        HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, true));

    // Create a server that allows operations based on namespace auth and rejects operations based
    // on cluster auth
    HelixRestServer server =
        new HelixRestServer(namespaces, newPort, getBaseUri().getPath(), Collections.emptyList(),
            mockAuthValidatorReject, mockAuthValidatorPass, new NoopAclRegister());
    server.start();

    HttpUriRequest request =
        buildRequest("/clusters/" + CLASSNAME_TEST_CST_AUTH, HttpConstants.RestVerbs.PUT, "");
    sendRequestAndValidate(request, Response.Status.CREATED.getStatusCode());
    request = buildRequest("/clusters/" + CLASSNAME_TEST_CST_AUTH, HttpConstants.RestVerbs.GET, "");
    sendRequestAndValidate(request, Response.Status.FORBIDDEN.getStatusCode());

    server.shutdown();
    _httpClient.close();

    // Create a server that rejects operations based on namespace auth and allows operations based
    // on cluster auth
    server =
        new HelixRestServer(namespaces, newPort, getBaseUri().getPath(), Collections.emptyList(),
            mockAuthValidatorPass, mockAuthValidatorReject, new NoopAclRegister());
    server.start();
    _httpClient = HttpClients.createDefault();

    request = buildRequest("/clusters/" + CLASSNAME_TEST_CST_AUTH, HttpConstants.RestVerbs.GET, "");
    sendRequestAndValidate(request, Response.Status.OK.getStatusCode());
    request = buildRequest("/clusters", HttpConstants.RestVerbs.GET, "");
    sendRequestAndValidate(request, Response.Status.FORBIDDEN.getStatusCode());

    server.shutdown();
    _httpClient.close();
  }

  private HttpUriRequest buildRequest(String urlSuffix, HttpConstants.RestVerbs requestMethod,
      String jsonEntity) {
    String url = _mockBaseUri + urlSuffix;
    switch (requestMethod) {
      case PUT:
        HttpPut httpPut = new HttpPut(url);
        httpPut.setEntity(new StringEntity(jsonEntity, ContentType.APPLICATION_JSON));
        return httpPut;
      case DELETE:
        return new HttpDelete(url);
      case GET:
        return new HttpGet(url);
      default:
        throw new IllegalArgumentException("Unsupported requestMethod: " + requestMethod);
    }
  }

  private void sendRequestAndValidate(HttpUriRequest request, int expectedResponseCode)
      throws IllegalArgumentException, IOException {
    HttpResponse response = _httpClient.execute(request);
    Assert.assertEquals(response.getStatusLine().getStatusCode(), expectedResponseCode);
  }


}
