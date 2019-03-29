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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Error;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.client.CustomRestClientFactory;
import org.apache.helix.rest.common.HelixDataAccessorWrapper;
import org.apache.helix.rest.server.json.instance.InstanceInfo;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.rest.server.service.ClusterService;
import org.apache.helix.rest.server.service.ClusterServiceImpl;
import org.apache.helix.rest.server.service.InstanceService;
import org.apache.helix.rest.server.service.InstanceServiceImpl;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/clusters/{clusterId}/instances")
public class InstanceAccessor extends AbstractHelixResource {
  private final static Logger _logger = LoggerFactory.getLogger(InstanceAccessor.class);

  public enum InstanceProperties {
    instances,
    online,
    disabled,
    config,
    liveInstance,
    resource,
    resources,
    partitions,
    errors,
    new_messages,
    read_messages,
    total_message_count,
    read_message_count,
    healthreports,
    instanceTags,
    selection_base,
    zone_order,
    customized_values,
    instance_stoppable_parallel,
    instance_not_stoppable_with_reasons
  }

  public enum InstanceHealthSelectionBase {
    instance_based,
    zone_based
  }

  @GET
  public Response getAllInstances(@PathParam("clusterId") String clusterId) {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), JsonNodeFactory.instance.textNode(clusterId));

    ArrayNode instancesNode = root.putArray(InstanceProperties.instances.name());
    ArrayNode onlineNode = root.putArray(InstanceProperties.online.name());
    ArrayNode disabledNode = root.putArray(InstanceProperties.disabled.name());

    List<String> instances = accessor.getChildNames(accessor.keyBuilder().instanceConfigs());

    if (instances != null) {
      instancesNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(instances));
    } else {
      return notFound();
    }

    List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());

    for (String instanceName : instances) {
      InstanceConfig instanceConfig =
          accessor.getProperty(accessor.keyBuilder().instanceConfig(instanceName));
      if (instanceConfig != null) {
        if (!instanceConfig.getInstanceEnabled() || (clusterConfig.getDisabledInstances() != null
            && clusterConfig.getDisabledInstances().containsKey(instanceName))) {
          disabledNode.add(JsonNodeFactory.instance.textNode(instanceName));
        }

        if (liveInstances.contains(instanceName)){
          onlineNode.add(JsonNodeFactory.instance.textNode(instanceName));
        }
      }
    }

    return JSONRepresentation(root);
  }

  @POST
  public Response updateInstances(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String command, String content) {
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
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
          .readValue(node.get(InstanceProperties.instances.name()).toString(),
              OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
      switch (cmd) {
      case enable:
        admin.enableInstance(clusterId, enableInstances, true);

        break;
      case disable:
        admin.enableInstance(clusterId, enableInstances, false);
        break;
      default:
        _logger.error("Unsupported command :" + command);
        return badRequest("Unsupported command :" + command);
      }
    } catch (Exception e) {
      _logger.error("Failed in updating instances : " + content, e);
      return badRequest(e.getMessage());
    }
    return OK();
  }

  @POST
  @Path("stoppable")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getParallelStoppableInstances(@PathParam("clusterId") String clusterId,
      String content) {
    try {
      JsonNode node = null;
      if (content.length() != 0) {
        node = OBJECT_MAPPER.readTree(content);
      }
      if (node == null) {
        return badRequest("Invalid input for content : " + content);
      }

      // TODO: Process input data from the content
      InstanceHealthSelectionBase selectionBase = InstanceHealthSelectionBase
          .valueOf(node.get(InstanceProperties.selection_base.name()).getValueAsText());
      List<String> instances = OBJECT_MAPPER
          .readValue(node.get(InstanceProperties.instances.name()).toString(),
              OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));

      List<String> orderOfZone = null;
      String customizedInput = null;
      if (node.get(InstanceProperties.customized_values.name()) != null) {
        customizedInput = node.get(InstanceProperties.customized_values.name()).getTextValue();
      }

      if (node.get(InstanceProperties.zone_order.name()) != null) {
        orderOfZone = OBJECT_MAPPER
            .readValue(node.get(InstanceProperties.zone_order.name()).toString(),
                OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
      }

      // Prepare output result
      ObjectNode result = JsonNodeFactory.instance.objectNode();
      ArrayNode stoppableInstances =
          result.putArray(InstanceProperties.instance_stoppable_parallel.name());
      ObjectNode failedStoppableInstances =
          result.putObject(InstanceProperties.instance_not_stoppable_with_reasons.name());

      switch (selectionBase) {
      case zone_based:
        List<String> zoneBasedInstance = getZoneBasedInstance(clusterId, instances, orderOfZone);
        for (String instance : zoneBasedInstance) {
          StoppableCheck stoppableCheck =
              checkSingleInstanceStoppable(clusterId, instance, customizedInput);
          if (!stoppableCheck.isStoppable()) {
            ArrayNode failedReasonsNode = failedStoppableInstances.putArray(instance);
            for (String failedReason : stoppableCheck.getFailedChecks()) {
              failedReasonsNode.add(JsonNodeFactory.instance.textNode(failedReason));
            }
          } else {
            stoppableInstances.add(instance);
          }
        }
        break;
      case instance_based:
      default:
        throw new NotImplementedException("instance_based selection is not support now!");
      }
      return JSONRepresentation(result);
    } catch (HelixException e) {
      _logger
          .error(String.format("Current cluster %s has issue with health checks!", clusterId), e);
      return serverError(e);
    } catch (Exception e) {
      _logger.error(
          String.format("Failed to get parallel stoppable instances for cluster %s!", clusterId),
          e);
      return serverError(e);
    }
  }

  @GET
  @Path("{instanceName}")
  public Response getInstanceById(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    // TODO reduce GC by dependency injection
    InstanceService instanceService = new InstanceServiceImpl(
        new HelixDataAccessorWrapper((ZKHelixDataAccessor) dataAccessor), getConfigAccessor());
    InstanceInfo instanceInfo = instanceService.getInstanceInfo(clusterId, instanceName,
        InstanceService.HealthCheck.STARTED_AND_HEALTH_CHECK_LIST);

    return OK(objectMapper.writeValueAsString(instanceInfo));
  }

  @POST
  @Path("{instanceName}/stoppable")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response isInstanceStoppable(String jsonContent,
      @PathParam("clusterId") String clusterId, @PathParam("instanceName") String instanceName) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    StoppableCheck stoppableCheck = null;
    try {
      stoppableCheck = checkSingleInstanceStoppable(clusterId, instanceName, jsonContent);
    } catch (HelixException e) {
      _logger
          .error(String.format("Current cluster %s has issue with health checks!", clusterId), e);
      return serverError(e);
    }
    return OK(objectMapper.writeValueAsString(stoppableCheck));
  }

  @PUT
  @Path("{instanceName}")
  public Response addInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName, String content) {
    HelixAdmin admin = getHelixAdmin();
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }

    try {
      admin.addInstance(clusterId, new InstanceConfig(record));
    } catch (Exception ex) {
      _logger.error("Error in adding an instance: " + instanceName, ex);
      return serverError(ex);
    }

    return OK();
  }

  @POST
  @Path("{instanceName}")
  public Response updateInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName, @QueryParam("command") String command,
      String content) {
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }

    HelixAdmin admin = getHelixAdmin();
    try {
      JsonNode node = null;
      if (content.length() != 0) {
        node = OBJECT_MAPPER.readTree(content);
      }

      switch (cmd) {
      case enable:
        admin.enableInstance(clusterId, instanceName, true);
        break;
      case disable:
        admin.enableInstance(clusterId, instanceName, false);
        break;

      case reset:
      case resetPartitions:
        if (!validInstance(node, instanceName)) {
          return badRequest("Instance names are not match!");
        }
        admin.resetPartition(clusterId, instanceName,
            node.get(InstanceProperties.resource.name()).getTextValue(), (List<String>) OBJECT_MAPPER
                .readValue(node.get(InstanceProperties.partitions.name()).toString(),
                    OBJECT_MAPPER.getTypeFactory()
                        .constructCollectionType(List.class, String.class)));
        break;
      case addInstanceTag:
        if (!validInstance(node, instanceName)) {
          return badRequest("Instance names are not match!");
        }
        for (String tag : (List<String>) OBJECT_MAPPER
            .readValue(node.get(InstanceProperties.instanceTags.name()).toString(),
                OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class))) {
          admin.addInstanceTag(clusterId, instanceName, tag);
        }
        break;
      case removeInstanceTag:
        if (!validInstance(node, instanceName)) {
          return badRequest("Instance names are not match!");
        }
        for (String tag : (List<String>) OBJECT_MAPPER
            .readValue(node.get(InstanceProperties.instanceTags.name()).toString(),
                OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class))) {
          admin.removeInstanceTag(clusterId, instanceName, tag);
        }
        break;
      case enablePartitions:
        admin.enablePartition(true, clusterId, instanceName,
            node.get(InstanceProperties.resource.name()).getTextValue(),
            (List<String>) OBJECT_MAPPER
                .readValue(node.get(InstanceProperties.partitions.name()).toString(),
                    OBJECT_MAPPER.getTypeFactory()
                        .constructCollectionType(List.class, String.class)));
        break;
      case disablePartitions:
        admin.enablePartition(false, clusterId, instanceName,
            node.get(InstanceProperties.resource.name()).getTextValue(),
            (List<String>) OBJECT_MAPPER
                .readValue(node.get(InstanceProperties.partitions.name()).toString(),
                    OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class)));
        break;
      default:
        _logger.error("Unsupported command :" + command);
        return badRequest("Unsupported command :" + command);
      }
    } catch (Exception e) {
      _logger.error("Failed in updating instance : " + instanceName, e);
      return badRequest(e.getMessage());
    }
    return OK();
  }

  @DELETE
  @Path("{instanceName}")
  public Response deleteInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) {
    HelixAdmin admin = getHelixAdmin();
    try {
      InstanceConfig instanceConfig = admin.getInstanceConfig(clusterId, instanceName);
      admin.dropInstance(clusterId, instanceConfig);
    } catch (HelixException e) {
      return badRequest(e.getMessage());
    }

    return OK();
  }

  @GET
  @Path("{instanceName}/configs")
  public Response getInstanceConfig(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    InstanceConfig instanceConfig =
        accessor.getProperty(accessor.keyBuilder().instanceConfig(instanceName));

    if (instanceConfig != null) {
      return JSONRepresentation(instanceConfig.getRecord());
    }

    return notFound();
  }

  @POST
  @Path("{instanceName}/configs")
  public Response updateInstanceConfig(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName, @QueryParam("command") String commandStr,
      String content) {
    Command command;
    if (commandStr == null || commandStr.isEmpty()) {
      command = Command.update; // Default behavior to keep it backward-compatible
    } else {
      try {
        command = getCommand(commandStr);
      } catch (HelixException ex) {
        return badRequest(ex.getMessage());
      }
    }

    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }
    InstanceConfig instanceConfig = new InstanceConfig(record);
    ConfigAccessor configAccessor = getConfigAccessor();
    try {
      switch (command) {
      case update:
        configAccessor.updateInstanceConfig(clusterId, instanceName, instanceConfig);
        break;
      case delete:
        HelixConfigScope instanceScope =
            new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
                .forCluster(clusterId).forParticipant(instanceName).build();
        configAccessor.remove(instanceScope, record);
        break;
      default:
        return badRequest(String.format("Unsupported command: %s", command));
      }
    } catch (HelixException ex) {
      return notFound(ex.getMessage());
    } catch (Exception ex) {
      _logger.error(String.format("Error in update instance config for instance: %s", instanceName),
          ex);
      return serverError(ex);
    }
    return OK();
  }

  @GET
  @Path("{instanceName}/resources")
  public Response getResourcesOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), instanceName);
    ArrayNode resourcesNode = root.putArray(InstanceProperties.resources.name());

    List<String> sessionIds = accessor.getChildNames(accessor.keyBuilder().sessions(instanceName));
    if (sessionIds == null || sessionIds.size() == 0) {
      return null;
    }

    // Only get resource list from current session id
    String currentSessionId = sessionIds.get(0);

    List<String> resources =
        accessor.getChildNames(accessor.keyBuilder().currentStates(instanceName, currentSessionId));
    if (resources != null && resources.size() > 0) {
      resourcesNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(resources));
    }

    return JSONRepresentation(root);
  }

  @GET
  @Path("{instanceName}/resources/{resourceName}")
  public Response getResourceOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName,
      @PathParam("resourceName") String resourceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    List<String> sessionIds = accessor.getChildNames(accessor.keyBuilder().sessions(instanceName));
    if (sessionIds == null || sessionIds.size() == 0) {
      return notFound();
    }

    // Only get resource list from current session id
    String currentSessionId = sessionIds.get(0);
    CurrentState resourceCurrentState = accessor
        .getProperty(accessor.keyBuilder().currentState(instanceName, currentSessionId, resourceName));
    if (resourceCurrentState != null) {
      return JSONRepresentation(resourceCurrentState.getRecord());
    }

    return notFound();
  }

  @GET
  @Path("{instanceName}/errors")
  public Response getErrorsOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), instanceName);
    ObjectNode errorsNode = JsonNodeFactory.instance.objectNode();

    List<String> sessionIds =
        accessor.getChildNames(accessor.keyBuilder().errors(instanceName));

    if (sessionIds == null || sessionIds.size() == 0) {
      return notFound();
    }

    for (String sessionId : sessionIds) {
      List<String> resources =
          accessor.getChildNames(accessor.keyBuilder().errors(instanceName, sessionId));
      if (resources != null) {
        ObjectNode resourcesNode = JsonNodeFactory.instance.objectNode();
        for (String resourceName : resources) {
          List<String> partitions = accessor
              .getChildNames(accessor.keyBuilder().errors(instanceName, sessionId, resourceName));
          if (partitions != null) {
            ArrayNode partitionsNode = resourcesNode.putArray(resourceName);
            partitionsNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(partitions));
          }
        }
        errorsNode.put(sessionId, resourcesNode);
      }
    }
    root.put(InstanceProperties.errors.name(), errorsNode);

    return JSONRepresentation(root);
  }

  @GET
  @Path("{instanceName}/errors/{sessionId}/{resourceName}/{partitionName}")
  public Response getErrorsOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName, @PathParam("sessionId") String sessionId,
      @PathParam("resourceName") String resourceName,
      @PathParam("partitionName") String partitionName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    Error error = accessor.getProperty(accessor.keyBuilder()
        .stateTransitionError(instanceName, sessionId, resourceName, partitionName));
    if (error != null) {
      return JSONRepresentation(error.getRecord());
    }

    return notFound();
  }

  @GET
  @Path("{instanceName}/history")
  public Response getHistoryOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    ParticipantHistory history =
        accessor.getProperty(accessor.keyBuilder().participantHistory(instanceName));
    if (history != null) {
      return JSONRepresentation(history.getRecord());
    }
    return notFound();
  }

  @GET
  @Path("{instanceName}/messages")
  public Response getMessagesOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName,
      @QueryParam("stateModelDef") String stateModelDef) {
    HelixDataAccessor accessor = getDataAccssor(clusterId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), instanceName);
    ArrayNode newMessages = root.putArray(InstanceProperties.new_messages.name());
    ArrayNode readMessages = root.putArray(InstanceProperties.read_messages.name());


    List<String> messageNames =
        accessor.getChildNames(accessor.keyBuilder().messages(instanceName));
    if (messageNames == null || messageNames.size() == 0) {
      _logger.warn("Unable to get any messages on instance: " + instanceName);
      return notFound();
    }

    for (String messageName : messageNames) {
      Message message = accessor.getProperty(accessor.keyBuilder().message(instanceName, messageName));
      if (message == null) {
        _logger.warn("Message is deleted given message name: ", messageName);
        continue;
      }
      // if stateModelDef is valid, keep messages with StateModelDef equals to the parameter
      if (StringUtil.isNotBlank(stateModelDef) && !stateModelDef.equals(message.getStateModelDef())) {
        continue;
      }

      if (Message.MessageState.NEW.equals(message.getMsgState())) {
        newMessages.add(messageName);
      } else if (Message.MessageState.READ.equals(message.getMsgState())) {
        readMessages.add(messageName);
      }
    }

    root.put(InstanceProperties.total_message_count.name(),
        newMessages.size() + readMessages.size());
    root.put(InstanceProperties.read_message_count.name(), readMessages.size());

    return JSONRepresentation(root);
  }

  @GET
  @Path("{instanceName}/messages/{messageId}")
  public Response getMessageOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName,
      @PathParam("messageId") String messageId) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    Message message = accessor.getProperty(accessor.keyBuilder().message(instanceName, messageId));
    if (message != null) {
      return JSONRepresentation(message.getRecord());
    }

    return notFound();
  }

  @GET
  @Path("{instanceName}/healthreports")
  public Response getHealthReportsOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), instanceName);
    ArrayNode healthReportsNode = root.putArray(InstanceProperties.healthreports.name());

    List<String> healthReports =
        accessor.getChildNames(accessor.keyBuilder().healthReports(instanceName));

    if (healthReports != null && healthReports.size() > 0) {
      healthReportsNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(healthReports));
    }

    return JSONRepresentation(root);
  }

  @GET
  @Path("{instanceName}/healthreports/{reportName}")
  public Response getHealthReportsOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName,
      @PathParam("reportName") String reportName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    HealthStat healthStat =
        accessor.getProperty(accessor.keyBuilder().healthReport(instanceName, reportName));
    if (healthStat != null) {
      return JSONRepresentation(healthStat);
    }

    return notFound();
  }

  private boolean validInstance(JsonNode node, String instanceName) {
    return instanceName.equals(node.get(Properties.id.name()).getValueAsText());
  }

  private List<String> getZoneBasedInstance(String clusterId, List<String> instances, List<String> orderOfZone) {
    ClusterService
        clusterService = new ClusterServiceImpl(getDataAccssor(clusterId), getConfigAccessor());
    Map<String, Set<String>> zoneMapping = clusterService.getClusterTopology(clusterId).toZoneMapping();
    if (orderOfZone == null) {
      orderOfZone = new ArrayList<>(zoneMapping.keySet());
    }
    Collections.sort(orderOfZone);
    if (orderOfZone.isEmpty()) {
      return orderOfZone;
    }

    Set<String> instanceSet = null;
    for (String zone : orderOfZone) {
      instanceSet = new TreeSet<>(instances);
      Set<String> currentZoneInstanceSet = new HashSet<>(zoneMapping.get(zone));
      instanceSet.retainAll(currentZoneInstanceSet);
      if (instanceSet.size() > 0) {
        return new ArrayList<>(instanceSet);
      }
    }

    return Collections.EMPTY_LIST;
  }

  private StoppableCheck checkSingleInstanceStoppable(String clusterId, String instanceName,
      String jsonContent) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    // TODO reduce GC by dependency injection
    InstanceService instanceService = new InstanceServiceImpl(
        new HelixDataAccessorWrapper((ZKHelixDataAccessor) dataAccessor), getConfigAccessor());

    Map<String, Boolean> helixStoppableCheck = instanceService.getInstanceHealthStatus(clusterId,
        instanceName, InstanceService.HealthCheck.STOPPABLE_CHECK_LIST);
    CustomRestClient customClient = CustomRestClientFactory.get(jsonContent);
    // TODO add the json content parse logic
    Map<String, Boolean> customStoppableCheck =
        customClient.getInstanceStoppableCheck(Collections.<String, String> emptyMap());
    StoppableCheck stoppableCheck =
        StoppableCheck.mergeStoppableChecks(helixStoppableCheck, customStoppableCheck);
    return stoppableCheck;
  }
}
