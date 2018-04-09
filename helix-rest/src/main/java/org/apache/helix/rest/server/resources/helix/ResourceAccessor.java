package org.apache.helix.rest.server.resources.helix;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;


@Path("/clusters/{clusterId}/resources")
public class ResourceAccessor extends AbstractHelixResource {
  private final static Logger _logger = LoggerFactory.getLogger(ResourceAccessor.class);

  public enum ResourceProperties {
    idealState, idealStates, externalView, externalViews, resourceConfig,
  }

  public enum HealthStatus {
    HEALTHY, PARTIAL_HEALTHY, UNHEALTHY
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

  /**
   * Returns health profile of all resources in the cluster
   *
   * @param clusterId
   * @return JSON result
   */
  @GET
  @Path("health")
  public Response getResourceHealth(@PathParam("clusterId") String clusterId) {

    ZkClient zkClient = getZkClient();

    List<String> resourcesInIdealState = zkClient.getChildren(PropertyPathBuilder.idealState(clusterId));
    List<String> resourcesInExternalView = zkClient.getChildren(PropertyPathBuilder.externalView(clusterId));

    Map<String, String> resourceHealthResult = new HashMap<>();

    for (String resourceName : resourcesInIdealState) {
      if (resourcesInExternalView.contains(resourceName)) {
        Map<String, String> partitionHealth = computePartitionHealth(clusterId, resourceName);

        if (partitionHealth.isEmpty() || partitionHealth.values().contains(HealthStatus.UNHEALTHY.name())) {
          // No partitions for a resource or there exists one or more UNHEALTHY partitions in this resource, UNHEALTHY
          resourceHealthResult.put(resourceName, HealthStatus.UNHEALTHY.name());
        } else if (partitionHealth.values().contains(HealthStatus.PARTIAL_HEALTHY.name())) {
          // No UNHEALTHY partition, but one or more partially healthy partitions, resource is partially healthy
          resourceHealthResult.put(resourceName, HealthStatus.PARTIAL_HEALTHY.name());
        } else {
          // No UNHEALTHY or partially healthy partitions and non-empty, resource is healthy
          resourceHealthResult.put(resourceName, HealthStatus.HEALTHY.name());
        }
      } else {
        // If a resource is not in ExternalView, then it is UNHEALTHY
        resourceHealthResult.put(resourceName, HealthStatus.UNHEALTHY.name());
      }
    }

    return JSONRepresentation(resourceHealthResult);
  }

  /**
   * Returns health profile of all partitions for the corresponding resource in the cluster
   *
   * @param clusterId
   * @param resourceName
   * @return JSON result
   * @throws IOException
   */
  @GET
  @Path("{resourceName}/health")
  public Response getPartitionHealth(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {

    return JSONRepresentation(computePartitionHealth(clusterId, resourceName));
  }

  @GET
  @Path("{resourceName}")
  public Response getResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName) {
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
  public Response addResource(@PathParam("clusterId") String clusterId, @PathParam("resourceName") String resourceName,
      @DefaultValue("-1") @QueryParam("numPartitions") int numPartitions,
      @DefaultValue("") @QueryParam("stateModelRef") String stateModelRef,
      @DefaultValue("SEMI_AUTO") @QueryParam("rebalancerMode") String rebalancerMode,
      @DefaultValue("DEFAULT") @QueryParam("rebalanceStrategy") String rebalanceStrategy,
      @DefaultValue("0") @QueryParam("bucketSize") int bucketSize,
      @DefaultValue("-1") @QueryParam("maxPartitionsPerInstance") int maxPartitionsPerInstance, String content) {

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
        admin.addResource(clusterId, resourceName, numPartitions, stateModelRef, rebalancerMode, rebalanceStrategy,
            bucketSize, maxPartitionsPerInstance);
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
      @DefaultValue("") @QueryParam("group") String group) {
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
      _logger.error(String.format("Error in update resource config for resource: %s", resourceName), ex);
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

  private Map<String, String> computePartitionHealth(String clusterId, String resourceName) {
    HelixAdmin admin = getHelixAdmin();
    IdealState idealState = admin.getResourceIdealState(clusterId, resourceName);
    ExternalView externalView = admin.getResourceExternalView(clusterId, resourceName);
    StateModelDefinition stateModelDef = admin.getStateModelDef(clusterId, idealState.getStateModelDefRef());
    String initialState = stateModelDef.getInitialState();
    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    statesPriorityList = statesPriorityList.subList(0,
        statesPriorityList.indexOf(initialState)); // Trim stateList to initialState and above
    int minActiveReplicas = idealState.getMinActiveReplicas();

    // Start the logic that determines the health status of each partition
    Map<String, String> partitionHealthResult = new HashMap<>();
    Set<String> allPartitionNames = idealState.getPartitionSet();
    if (!allPartitionNames.isEmpty()) {
      for (String partitionName : allPartitionNames) {
        int replicaCount = idealState.getReplicaCount(idealState.getPreferenceList(partitionName).size());
        // Simplify expectedStateCountMap by assuming that all instances are available to reduce computation load on this REST endpoint
        LinkedHashMap<String, Integer> expectedStateCountMap =
            stateModelDef.getStateCountMap(replicaCount, replicaCount);
        // Extract all states into Collections from ExternalView
        Map<String, String> stateMapInExternalView = externalView.getStateMap(partitionName);
        Collection<String> allReplicaStatesInExternalView =
            (stateMapInExternalView != null && !stateMapInExternalView.isEmpty()) ? stateMapInExternalView.values()
                : Collections.<String>emptyList();
        int numActiveReplicasInExternalView = 0;
        HealthStatus status = HealthStatus.HEALTHY;

        // Go through all states that are "active" states (higher priority than InitialState)
        for (int statePriorityIndex = 0; statePriorityIndex < statesPriorityList.size(); statePriorityIndex++) {
          String currentState = statesPriorityList.get(statePriorityIndex);
          int currentStateCountInIdealState = expectedStateCountMap.get(currentState);
          int currentStateCountInExternalView = Collections.frequency(allReplicaStatesInExternalView, currentState);
          numActiveReplicasInExternalView += currentStateCountInExternalView;
          // Top state counts must match, if not, unhealthy
          if (statePriorityIndex == 0 && currentStateCountInExternalView != currentStateCountInIdealState) {
            status = HealthStatus.UNHEALTHY;
            break;
          } else if (currentStateCountInExternalView < currentStateCountInIdealState) {
            // For non-top states, if count in ExternalView is less than count in IdealState, partially healthy
            status = HealthStatus.PARTIAL_HEALTHY;
          }
        }
        if (numActiveReplicasInExternalView < minActiveReplicas) {
          // If this partition does not satisfy the number of minimum active replicas, unhealthy
          status = HealthStatus.UNHEALTHY;
        }
        partitionHealthResult.put(partitionName, status.name());
      }
    }
    return partitionHealthResult;
  }
}