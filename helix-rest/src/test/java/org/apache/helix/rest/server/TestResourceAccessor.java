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
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.rest.server.resources.ResourceAccessor;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResourceAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";
  private final static String RESOURCE_NAME = CLUSTER_NAME + "_db_0";

  @Test
  public void testGetResources() throws IOException {
    final Response response = target("clusters/" + CLUSTER_NAME + "/resources").request().get();
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertEquals(response.getMediaType().getType(), "application");

    String body = response.readEntity(String.class);
    Assert.assertNotNull(body);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String idealStates =
        node.get(ResourceAccessor.ResourceProperties.idealStates.name()).toString();
    Assert.assertNotNull(idealStates);

    Set<String> resources = OBJECT_MAPPER.readValue(idealStates,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(resources, _resourcesMap.get("TestCluster_0"),
        "Resources from response: " + resources + " vs clusters actually: " + _resourcesMap
            .get("TestCluster_0"));
  }

  @Test(dependsOnMethods = "testGetResources")
  public void testGetResource() throws IOException {
    final Response response =
        target("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME).request().get();

    // Get total resource
    String body = response.readEntity(String.class);
    Assert.assertNotNull(body);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String idealStateStr =
        node.get(ResourceAccessor.ResourceProperties.idealState.name()).toString();
    IdealState idealState = new IdealState(toZNRecord(idealStateStr));
    IdealState originIdealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
    Assert.assertEquals(idealState, originIdealState);
  }

  @Test(dependsOnMethods = "testGetResource")
  public void testAddResources() throws IOException {
    String newResourceName = "newResource";
    IdealState idealState = new IdealState(newResourceName);
    idealState.getRecord().getSimpleFields().putAll(
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME)
            .getRecord().getSimpleFields());

    // Add resource by IdealState
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(idealState.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    Response response =
        target("clusters/" + CLUSTER_NAME + "/resources/" + newResourceName).request().put(entity);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    Assert.assertEquals(idealState, _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, newResourceName));

    // Add resource by query param
    entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);

    Response responseWithoutIdealState =
        target("clusters/" + CLUSTER_NAME + "/resources/" + newResourceName + "0")
            .queryParam("numPartitions", "4").queryParam("stateModelRef", "OnlineOffline")
            .queryParam("rebalancerMode", "FULL_AUTO").request().put(entity);

    IdealState queryIdealState = new FullAutoModeISBuilder(newResourceName + 0).setNumPartitions(4)
        .setStateModel("OnlineOffline").setRebalancerMode(IdealState.RebalanceMode.FULL_AUTO)
        .setRebalanceStrategy("DEFAULT").build();
    Assert.assertEquals(responseWithoutIdealState.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertEquals(queryIdealState, _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, newResourceName + "0"));
  }

  @Test(dependsOnMethods = "testAddResources")
  public void testResourceConfig() throws IOException {
    final Response response =
        target("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/configs").request()
            .get();

    String body = response.readEntity(String.class);
    ResourceConfig resourceConfig = new ResourceConfig(toZNRecord(body));
    Assert.assertEquals(resourceConfig,
        _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME));
  }

  @Test(dependsOnMethods = "testAddResources")
  public void testIdealState() throws IOException {
    final Response response =
        target("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/idealState").request()
            .get();

    String body = response.readEntity(String.class);
    IdealState idealState = new IdealState(toZNRecord(body));
    Assert.assertEquals(idealState,
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME));
  }

  @Test(dependsOnMethods = "testAddResources")
  public void testExternalView() throws IOException {
    final Response response =
        target("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/externalView")
            .request().get();

    String body = response.readEntity(String.class);
    ExternalView externalView = new ExternalView(toZNRecord(body));
    Assert.assertEquals(externalView, _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, RESOURCE_NAME));
  }

}


