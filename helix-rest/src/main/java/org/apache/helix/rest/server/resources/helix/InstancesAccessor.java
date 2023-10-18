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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.clusterMaintenanceService.MaintenanceManagementService;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.common.StoppableInstancesSelector;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.rest.server.resources.exceptions.HelixHealthException;
import org.apache.helix.rest.server.service.ClusterService;
import org.apache.helix.rest.server.service.ClusterServiceImpl;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ClusterAuth
@Path("/clusters/{clusterId}/instances")
public class InstancesAccessor extends AbstractHelixResource {
  private final static Logger _logger = LoggerFactory.getLogger(InstancesAccessor.class);
  public enum InstancesProperties {
    instances,
    online,
    disabled,
    selection_base,
    zone_order,
    customized_values,
    instance_stoppable_parallel,
    instance_not_stoppable_with_reasons
  }

  public enum InstanceHealthSelectionBase {
    instance_based,
    zone_based,
    cross_zone_based
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  public Response getAllInstances(@PathParam("clusterId") String clusterId,
      @DefaultValue("getAllInstances") @QueryParam("command") String command) {
    // Get the command. If not provided, the default would be "getAllInstances"
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }

    HelixDataAccessor accessor = getDataAccssor(clusterId);
    List<String> instances = accessor.getChildNames(accessor.keyBuilder().instanceConfigs());
    if (instances == null) {
      return notFound();
    }

    switch (cmd) {
    case getAllInstances:
      ObjectNode root = JsonNodeFactory.instance.objectNode();
      root.put(Properties.id.name(), JsonNodeFactory.instance.textNode(clusterId));

      ArrayNode instancesNode =
          root.putArray(InstancesAccessor.InstancesProperties.instances.name());
      instancesNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(instances));
      ArrayNode onlineNode = root.putArray(InstancesAccessor.InstancesProperties.online.name());
      ArrayNode disabledNode = root.putArray(InstancesAccessor.InstancesProperties.disabled.name());

      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
      ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());

      for (String instanceName : instances) {
        InstanceConfig instanceConfig =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(instanceName));
        if (instanceConfig != null) {
          if (!InstanceValidationUtil.isInstanceEnabled(instanceConfig, clusterConfig)) {
            disabledNode.add(JsonNodeFactory.instance.textNode(instanceName));
          }

          if (liveInstances.contains(instanceName)) {
            onlineNode.add(JsonNodeFactory.instance.textNode(instanceName));
          }
        }
      }
      return JSONRepresentation(root);
    case validateWeight:
      // Validate all instances for WAGED rebalance
      HelixAdmin admin = getHelixAdmin();
      Map<String, Boolean> validationResultMap;
      try {
        validationResultMap = admin.validateInstancesForWagedRebalance(clusterId, instances);
      } catch (HelixException e) {
        return badRequest(e.getMessage());
      }
      return JSONRepresentation(validationResultMap);
    default:
      _logger.error("Unsupported command :" + command);
      return badRequest("Unsupported command :" + command);
    }
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  public Response instancesOperations(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String command,
      @QueryParam("continueOnFailures") boolean continueOnFailures,
      @QueryParam("skipZKRead") boolean skipZKRead,
      @QueryParam("skipHealthCheckCategories") String skipHealthCheckCategories,
      @QueryParam("random") boolean random, String content) {
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }

    Set<StoppableCheck.Category> skipHealthCheckCategorySet;
    try {
      skipHealthCheckCategorySet = skipHealthCheckCategories != null
          ? StoppableCheck.Category.categorySetFromCommaSeperatedString(skipHealthCheckCategories)
          : Collections.emptySet();
      if (!MaintenanceManagementService.SKIPPABLE_HEALTH_CHECK_CATEGORIES.containsAll(
          skipHealthCheckCategorySet)) {
        throw new IllegalArgumentException(
            "Some of the provided skipHealthCheckCategories are not skippable. The supported skippable categories are: "
                + MaintenanceManagementService.SKIPPABLE_HEALTH_CHECK_CATEGORIES);
      }
    } catch (Exception e) {
      return badRequest("Invalid skipHealthCheckCategories: " + skipHealthCheckCategories + "\n"
          + e.getMessage());
    }

    HelixAdmin admin = getHelixAdmin();
    try {
      JsonNode node = null;
      if (content.length() != 0) {
        node = OBJECT_MAPPER.readTree(content);
      }
      if (node == null) {
        return badRequest("Invalid input for content : " + content);
      }
      List<String> enableInstances = OBJECT_MAPPER
          .readValue(node.get(InstancesAccessor.InstancesProperties.instances.name()).toString(),
              OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
      switch (cmd) {
        case enable:
          admin.enableInstance(clusterId, enableInstances, true);
          break;
        case disable:
          admin.enableInstance(clusterId, enableInstances, false);
          break;
        case stoppable:
          return batchGetStoppableInstances(clusterId, node, skipZKRead, continueOnFailures,
              skipHealthCheckCategorySet, random);
        default:
          _logger.error("Unsupported command :" + command);
          return badRequest("Unsupported command :" + command);
      }
    } catch (HelixHealthException e) {
      _logger
          .error(String.format("Current cluster %s has issue with health checks!", clusterId), e);
      return serverError(e);
    } catch (Exception e) {
      _logger.error("Failed in updating instances : " + content, e);
      return badRequest(e.getMessage());
    }
    return OK();
  }

  private Response batchGetStoppableInstances(String clusterId, JsonNode node, boolean skipZKRead,
      boolean continueOnFailures, Set<StoppableCheck.Category> skipHealthCheckCategories,
      boolean random) throws IOException {
    try {
      // TODO: Process input data from the content
      InstancesAccessor.InstanceHealthSelectionBase selectionBase =
          InstancesAccessor.InstanceHealthSelectionBase.valueOf(
              node.get(InstancesAccessor.InstancesProperties.selection_base.name()).textValue());
      List<String> instances = OBJECT_MAPPER.readValue(
          node.get(InstancesAccessor.InstancesProperties.instances.name()).toString(),
          OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));

      List<String> orderOfZone = null;
      String customizedInput = null;
      if (node.get(InstancesAccessor.InstancesProperties.customized_values.name()) != null) {
        customizedInput =
            node.get(InstancesAccessor.InstancesProperties.customized_values.name()).toString();
      }

      if (node.get(InstancesAccessor.InstancesProperties.zone_order.name()) != null) {
        orderOfZone = OBJECT_MAPPER.readValue(
            node.get(InstancesAccessor.InstancesProperties.zone_order.name()).toString(),
            OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
      }

      // Prepare output result
      ObjectNode result = JsonNodeFactory.instance.objectNode();
      ArrayNode stoppableInstances =
          result.putArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
      ObjectNode failedStoppableInstances = result.putObject(
          InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());

      MaintenanceManagementService maintenanceService =
          new MaintenanceManagementService((ZKHelixDataAccessor) getDataAccssor(clusterId),
              getConfigAccessor(), skipZKRead, continueOnFailures, skipHealthCheckCategories,
              getNamespace());
      ClusterService clusterService =
          new ClusterServiceImpl(getDataAccssor(clusterId), getConfigAccessor());
      ClusterTopology clusterTopology = clusterService.getClusterTopology(clusterId);
      StoppableInstancesSelector stoppableInstancesSelector =
          new StoppableInstancesSelector.StoppableInstancesSelectorBuilder()
              .setClusterId(clusterId)
              .setOrderOfZone(orderOfZone)
              .setCustomizedInput(customizedInput)
              .setStoppableInstances(stoppableInstances)
              .setFailedStoppableInstances(failedStoppableInstances)
              .setMaintenanceService(maintenanceService)
              .setClusterTopology(clusterTopology)
              .build();
      stoppableInstancesSelector.calculateOrderOfZone(random);
      switch (selectionBase) {
        case zone_based:
          stoppableInstancesSelector.getStoppableInstancesInSingleZone(instances);
          break;
        case cross_zone_based:
          stoppableInstancesSelector.getStoppableInstancesCrossZones();
          break;
        case instance_based:
        default:
          throw new UnsupportedOperationException("instance_based selection is not supported yet!");
      }
      return JSONRepresentation(result);
    } catch (HelixException e) {
      _logger
              .error(String.format("Current cluster %s has issue with health checks!", clusterId), e);
      throw new HelixHealthException(e);
    } catch (Exception e) {
      _logger.error(String.format(
              "Failed to get parallel stoppable instances for cluster %s with a HelixException!",
              clusterId), e);
      throw e;
    }
  }
}
