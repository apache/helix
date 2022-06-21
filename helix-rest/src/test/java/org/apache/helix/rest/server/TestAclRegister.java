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
import org.apache.helix.TestHelper;
import org.apache.helix.rest.acl.AclRegister;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.authValidator.NoopAuthValidator;
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


public class TestAclRegister extends AbstractTestClass {
  private String _mockBaseUri;
  private CloseableHttpClient _httpClient;

  private static String CLASSNAME_TEST_DEFAULT_ACL_REGISTER = "testDefaultAclRegister";
  private static String CLASSNAME_TEST_CUSTOM_ACL_REGISTER = "testCustomACLRegister";

  @Test
  public void testDefaultAclRegister() {
    put("clusters/testCluster", null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());
    TestHelper.dropCluster("testCluster", _gZkClient);
  }

  @Test(dependsOnMethods = "testDefaultAclRegister")
  public void testCustomACLRegister() throws IOException, InterruptedException {
    int newPort = getBaseUri().getPort() + 1;

    _mockBaseUri = HttpConstants.HTTP_PROTOCOL_PREFIX + getBaseUri().getHost() + ":" + newPort;
    _httpClient = HttpClients.createDefault();

    AclRegister mockAclRegister = Mockito.mock(AclRegister.class);
    Mockito.doThrow(new RuntimeException()).when(mockAclRegister).createACL(any());

    List<HelixRestNamespace> namespaces = new ArrayList<>();
    namespaces.add(new HelixRestNamespace(HelixRestNamespace.DEFAULT_NAMESPACE_NAME,
        HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, true));

    // Create a server that passes acl resource creation
    HelixRestServer server =
        new HelixRestServer(namespaces, newPort, getBaseUri().getPath(), Collections.emptyList(),
            new NoopAuthValidator(), new NoopAuthValidator(), mockAclRegister);
    server.start();

    HttpUriRequest request =
        buildRequest("/clusters/testCluster", HttpConstants.RestVerbs.PUT, "");
    sendRequestAndValidate(request, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    request =
        buildRequest("/clusters/testCluster", HttpConstants.RestVerbs.GET, "");
    sendRequestAndValidate(request, Response.Status.NOT_FOUND.getStatusCode());

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
