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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.PropertyKey;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestNamespacedAPIAccess extends AbstractTestClass {
  ObjectMapper _mapper = new ObjectMapper();

  @Test
  public void testDefaultNamespaceDisabled() {
    String testClusterName = "testDefaultNamespaceDisabled";

    // "/namespaces/default" is disabled.
    get(String.format("/namespaces/%s", HelixRestNamespace.DEFAULT_NAMESPACE_NAME), null, Response.Status.NOT_FOUND.getStatusCode(), false);

    // Create a cluster.
    put(String.format("/clusters/%s", testClusterName), null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    get(String.format("/clusters/%s", testClusterName), null, Response.Status.OK.getStatusCode(), false);

    // Remove empty test cluster. Otherwise, it could fail ClusterAccessor tests
    delete(String.format("/clusters/%s", testClusterName), Response.Status.OK.getStatusCode());
  }

  @Test(dependsOnMethods = "testDefaultNamespaceDisabled")
  public void testNamespacedCRUD() throws IOException {
    String testClusterName = "testClusterForNamespacedCRUD";

    // Create cluster in test namespace and verify it's only appears in test namespace
    put(String.format("/namespaces/%s/clusters/%s", TEST_NAMESPACE, testClusterName), null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE), Response.Status.CREATED.getStatusCode());
    get(String.format("/namespaces/%s/clusters/%s", TEST_NAMESPACE, testClusterName), null,
        Response.Status.OK.getStatusCode(), false);
    get(String.format("/clusters/%s", testClusterName), null, Response.Status.NOT_FOUND.getStatusCode(), false);

    // Create a cluster with same name in a different namespace
    put(String.format("/clusters/%s", testClusterName), null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE), Response.Status.CREATED.getStatusCode());
    get(String.format("/clusters/%s", testClusterName), null, Response.Status.OK.getStatusCode(), false);

    // Modify cluster in default namespace
    post(String.format("/clusters/%s", testClusterName), ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode());

    // Verify the cluster in default namespace is modified, while the one in test namespace is not.
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(testClusterName);
    Assert.assertTrue(_baseAccessor.exists(keyBuilder.pause().getPath(), 0));
    Assert.assertFalse(_baseAccessorTestNS.exists(keyBuilder.pause().getPath(), 0));

    // Verify that deleting cluster in one namespace will not affect the other
    delete(String.format("/namespaces/%s/clusters/%s", TEST_NAMESPACE, testClusterName),
        Response.Status.OK.getStatusCode());
    get(String.format("/namespaces/%s/clusters/%s", TEST_NAMESPACE, testClusterName), null,
        Response.Status.NOT_FOUND.getStatusCode(), false);
    get(String.format("/clusters/%s", testClusterName), null, Response.Status.OK.getStatusCode(), false);
    // Remove empty test clusters. Otherwise, it could fail ClusterAccessor tests
    delete(String.format("/clusters/%s", testClusterName), Response.Status.OK.getStatusCode());
  }

  @Test(dependsOnMethods = "testNamespacedCRUD")
  public void testNamespaceServer() throws IOException {
    // Default endpoints should not have any namespace information returned
    get("/", null, Response.Status.NOT_FOUND.getStatusCode(), false);

    // Get invalid namespace should return not found
    get("/namespaces/invalid-namespace", null, Response.Status.NOT_FOUND.getStatusCode(), false);

    // list namespace should return a list of all namespaces
    String body = get("/namespaces", null, Response.Status.OK.getStatusCode(), true);
    List<Map<String, String>> namespaceMaps = _mapper
        .readValue(body, _mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
    Assert.assertEquals(namespaceMaps.size(), 2);

    Set<String> expectedNamespaceNames = new HashSet<>();
    expectedNamespaceNames.add(HelixRestNamespace.DEFAULT_NAMESPACE_NAME);
    expectedNamespaceNames.add(TEST_NAMESPACE);

    for (Map<String, String> namespaceMap : namespaceMaps) {
      String name = namespaceMap.get(HelixRestNamespace.HelixRestNamespaceProperty.NAME.name());
      boolean isDefault = Boolean.parseBoolean(
          namespaceMap.get(HelixRestNamespace.HelixRestNamespaceProperty.IS_DEFAULT.name()));
      switch (name) {
      case HelixRestNamespace.DEFAULT_NAMESPACE_NAME:
        Assert.assertTrue(isDefault);
        break;
      case TEST_NAMESPACE:
        Assert.assertFalse(isDefault);
        break;
      default:
        Assert.assertFalse(true, "Namespace " + name + " is not expected");
        break;
      }
      expectedNamespaceNames.remove(name);
    }
    Assert.assertTrue(expectedNamespaceNames.isEmpty());

    // "/namespaces/default" is disabled.
    get(String.format("/namespaces/%s", HelixRestNamespace.DEFAULT_NAMESPACE_NAME), null,
        Response.Status.NOT_FOUND.getStatusCode(), false);
  }
}
