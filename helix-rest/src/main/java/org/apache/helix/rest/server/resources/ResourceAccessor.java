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
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

@Path("/clusters/{clusterId}/resources")
public class ResourceAccessor extends AbstractResource {
  private final static Logger _logger = Logger.getLogger(ResourceAccessor.class);
  public enum ResourceProperties {
    idealState,
    idealStates,
    externalView,
    externalViews,
    resourceConfig,
  }

  @GET
  public Response getResources(@PathParam("clusterId") String clusterId) {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), JsonNodeFactory.instance.textNode(clusterId));

    ZkClient zkClient = getZkClient();

    ArrayNode idealStatesNode = root.putArray(ResourceProperties.idealStates.name());
    ArrayNode externalViewsNode = root.putArray(ResourceProperties.externalViews.name());

    List<String> idealStates = zkClient.getChildren(PropertyPathBuilder.idealState(clusterId));
    List<String> externalViews = zkClient.getChildren(PropertyPathBuilder.externalView(clusterId));

    if (idealStates != null) {
      idealStatesNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(idealStates));
    } else {
      return notFound();
    }

    if (externalViews != null) {
      externalViewsNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(externalViews));
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

  @PUT
  @Path("{resourceName}")
  public Response addResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName,
      @DefaultValue("-1") @QueryParam("numPartitions") int numPartitions,
      @DefaultValue("") @QueryParam("stateModelRef") String stateModelRef,
      @DefaultValue("SEMI_AUTO") @QueryParam("rebalancerMode") String rebalancerMode,
      @DefaultValue("DEFAULT") @QueryParam("rebalanceStrategy") String rebalanceStrategy,
      @DefaultValue("0") @QueryParam("bucketSize") int bucketSize,
      @DefaultValue("-1") @QueryParam("maxPartitionsPerInstance") int maxPartitionsPerInstance,
      String content) {

    HelixAdmin admin = getHelixAdmin();

    try {
      if (content.length() != 0) {
        ZNRecord record;
        try {
          record = toZNRecord(content);
        } catch (IOException e) {
          _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
          return badRequest("Input is not a vaild ZNRecord!");
        }

        if (record.getSimpleFields() != null) {
          admin.addResource(clusterId, resourceName, new IdealState(record));
        }
      } else {
        admin.addResource(clusterId, resourceName, numPartitions, stateModelRef, rebalancerMode,
            rebalanceStrategy, bucketSize, maxPartitionsPerInstance);
      }
    } catch (Exception e) {
      _logger.error("Error in adding a resource: " + resourceName, e);
      return serverError(e);
    }

    return OK();
  }

  @POST
  @Path("{resourceName}")
  public Response updateResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName, @QueryParam("command") String command,
      @DefaultValue("-1") @QueryParam("replicas") int replicas,
      @DefaultValue("") @QueryParam("keyPrefix") String keyPrefix,
      @DefaultValue("") @QueryParam("group") String group){
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }

    HelixAdmin admin = getHelixAdmin();
    try {
      switch (cmd) {
      case enable:
        admin.enableResource(clusterId, resourceName, true);
        break;
      case disable:
        admin.enableResource(clusterId, resourceName, false);
        break;
      case rebalance:
        if (replicas == -1) {
          return badRequest("Number of replicas is needed for rebalancing!");
        }
        keyPrefix = keyPrefix.length() == 0 ? resourceName : keyPrefix;
        admin.rebalance(clusterId, resourceName, replicas, keyPrefix, group);
        break;
      default:
        _logger.error("Unsupported command :" + command);
        return badRequest("Unsupported command :" + command);
      }
    } catch (Exception e) {
      _logger.error("Failed in updating resource : " + resourceName, e);
      return badRequest(e.getMessage());
    }
    return OK();
  }

  @DELETE
  @Path("{resourceName}")
  public Response deleteResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {
    HelixAdmin admin = getHelixAdmin();
    try {
      admin.dropResource(clusterId, resourceName);
    } catch (Exception e) {
      _logger.error("Error in deleting a resource: " + resourceName, e);
      return serverError();
    }
    return OK();
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

  @POST
  @Path("{resourceName}/configs")
  public Response updateResourceConfig(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName, String content) {
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }
    ResourceConfig resourceConfig = new ResourceConfig(record);
    ConfigAccessor configAccessor = getConfigAccessor();
    try {
      configAccessor.updateResourceConfig(clusterId, resourceName, resourceConfig);
    } catch (HelixException ex) {
      return notFound(ex.getMessage());
    } catch (Exception ex) {
      _logger.error(
          "Failed to update cluster config, cluster " + clusterId + " new config: " + content
              + ", Exception: " + ex);
      return serverError(ex);
    }
    return OK();
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
