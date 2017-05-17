package org.apache.helix.rest.server.resources;

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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

@Path("/clusters/{clusterId}/resources")
@Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
public class ResourceAccessor extends AbstractResource {
  public enum ResourceProperties {
    idealState,
    idealStates,
    externalView,
    externalViews,
    resourceConfig
  }

  @GET
  public Response getResources(@PathParam("clusterId") String clusterId) {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), JsonNodeFactory.instance.textNode(clusterId));

    ZkClient zkClient = getZkClient();
    ObjectMapper mapper = new ObjectMapper();

    ArrayNode idealStatesNode = root.putArray(ResourceProperties.idealStates.name());
    ArrayNode externalViewsNode = root.putArray(ResourceProperties.externalViews.name());

    List<String> idealStates = zkClient.getChildren(PropertyPathBuilder.idealState(clusterId));
    List<String> externalViews = zkClient.getChildren(PropertyPathBuilder.externalView(clusterId));

    if (idealStates != null) {
      idealStatesNode.addAll((ArrayNode) mapper.valueToTree(idealStates));
    } else {
      return notFound();
    }

    if (externalViews != null) {
      externalViewsNode.addAll((ArrayNode) mapper.valueToTree(externalViews));
    }

    return JSONRepresentation(root);
  }

  @GET
  @Path("{resourceName}")
  public Response getResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) throws IOException {
    ConfigAccessor accessor = getConfigAccessor();
    HelixAdmin admin = getHelixAdmin();

    ResourceConfig resourceConfig = accessor.getResourceConfig(clusterId, resourceName);
    IdealState idealState = admin.getResourceIdealState(clusterId, resourceName);
    ExternalView externalView = admin.getResourceExternalView(clusterId, resourceName);

    Map<String, ZNRecord> resourceMap = new HashMap<>();
    if (idealState != null) {
      resourceMap.put(ResourceProperties.idealState.name(), idealState.getRecord());
    } else {
      return notFound();
    }

    resourceMap.put(ResourceProperties.resourceConfig.name(), null);
    resourceMap.put(ResourceProperties.externalView.name(), null);

    if (resourceConfig != null) {
      resourceMap.put(ResourceProperties.resourceConfig.name(), resourceConfig.getRecord());
    }

    if (externalView != null) {
      resourceMap.put(ResourceProperties.externalView.name(), externalView.getRecord());
    }

    return JSONRepresentation(resourceMap);
  }

  @GET
  @Path("{resourceName}/configs")
  public Response getResourceConfig(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {
    ConfigAccessor accessor = getConfigAccessor();
    ResourceConfig resourceConfig = accessor.getResourceConfig(clusterId, resourceName);
    if (resourceConfig != null) {
      return JSONRepresentation(resourceConfig.getRecord());
    }

    return notFound();
  }

  @GET
  @Path("{resourceName}/idealState")
  public Response getResourceIdealState(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {
    HelixAdmin admin = getHelixAdmin();
    IdealState idealState = admin.getResourceIdealState(clusterId, resourceName);
    if (idealState != null) {
      return JSONRepresentation(idealState.getRecord());
    }

    return notFound();
  }

  @GET
  @Path("{resourceName}/externalView")
  public Response getResourceExternalView(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {
    HelixAdmin admin = getHelixAdmin();
    ExternalView externalView = admin.getResourceExternalView(clusterId, resourceName);
    if (externalView != null) {
      return JSONRepresentation(externalView.getRecord());
    }

    return notFound();
  }
}
