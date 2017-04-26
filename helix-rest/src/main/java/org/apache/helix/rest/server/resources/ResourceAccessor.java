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
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

@Path("/clusters/{clusterId}/resources")
public class ResourceAccessor extends AbstractResource {
  private enum ResourceProperties {
    idealState,
    idealStates,
    externalView,
    externalViews,
    resourceConfig
  }

  @GET
  @Produces({ "application/json", "text/plain" })
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
      idealStatesNode.addAll(mapper.valueToTree(idealStates));
    } else {
      return notFound();
    }

    if (externalViews != null) {
      externalViewsNode.addAll(mapper.valueToTree(externalViews));
    }

    return JSONRepresentation(root);
  }

  @GET
  @Path("{resourceName}")
  @Produces({ "application/json", "text/plain" })
  public Response getResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) throws IOException {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    ConfigAccessor accessor = getConfigAccessor();
    HelixAdmin admin = getHelixAdmin();

    ResourceConfig resourceConfig = accessor.getResourceConfig(clusterId, resourceName);
    IdealState idealState = admin.getResourceIdealState(clusterId, resourceName);
    ExternalView externalView = admin.getResourceExternalView(clusterId, resourceName);

    if (idealState != null) {
      root.put(ResourceProperties.idealState.name(),
          JsonNodeFactory.instance.textNode(toJson(idealState.getRecord())));
    } else {
      return notFound();
    }

    TextNode resourceConfigNode = JsonNodeFactory.instance.textNode("");
    TextNode externalViewNode = JsonNodeFactory.instance.textNode("");

    if (resourceConfig != null) {
      resourceConfigNode = JsonNodeFactory.instance.textNode(toJson(resourceConfig.getRecord()));
    }

    if (externalView != null) {
      externalViewNode = JsonNodeFactory.instance.textNode(toJson(externalView.getRecord()));
    }

    root.put(ResourceProperties.resourceConfig.name(), resourceConfigNode);
    root.put(ResourceProperties.externalView.name(), externalViewNode);

    return JSONRepresentation(root);
  }

  @GET
  @Path("{resourceName}/configs")
  @Produces({ "application/json", "text/plain" })
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
  @Produces({ "application/json", "text/plain" })
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
  @Produces({ "application/json", "text/plain" })
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
