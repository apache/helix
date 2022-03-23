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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Error;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.rest.clusterMaintenanceService.HealthCheck;
import org.apache.helix.rest.clusterMaintenanceService.MaintenanceManagementService;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.helix.rest.server.json.instance.InstanceInfo;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ClusterAuth
@Path("/clusters/{clusterId}/instances/{instanceName}")
public class PerInstanceAccessor extends AbstractHelixResource {
  private final static Logger LOG = LoggerFactory.getLogger(PerInstanceAccessor.class);

  public enum PerInstanceProperties {
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
    health_check_list,
    health_check_config,
    operation_list,
    operation_config,
    continueOnFailures,
    skipZKRead,
    performOperation
    }

  private static class MaintenanceOpInputFields {
    List<String> healthChecks = null;
    Map<String, String> healthCheckConfig = null;
    List<String> operations = null;
    Map<String, String> operationConfig = null;
    Set<String> nonBlockingHelixCheck = new HashSet<>();
    boolean skipZKRead = false;
    boolean performOperation = true;
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  public Response getInstanceById(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName, @QueryParam("skipZKRead") String skipZKRead,
      @DefaultValue("getInstance") @QueryParam("command") String command) {
    // Get the command. If not provided, the default would be "getInstance"
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }

    switch (cmd) {
    case getInstance:
      HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
      // TODO reduce GC by dependency injection
      MaintenanceManagementService service =
          new MaintenanceManagementService((ZKHelixDataAccessor) dataAccessor, getConfigAccessor(),
              Boolean.parseBoolean(skipZKRead), getNamespace());
      InstanceInfo instanceInfo = service.getInstanceHealthInfo(clusterId, instanceName,
          HealthCheck.STARTED_AND_HEALTH_CHECK_LIST);
      String instanceInfoString;
      try {
        instanceInfoString = OBJECT_MAPPER.writeValueAsString(instanceInfo);
      } catch (JsonProcessingException e) {
        return serverError(e);
      }
      return OK(instanceInfoString);
    case validateWeight:
      // Validates instanceConfig for WAGED rebalance
      HelixAdmin admin = getHelixAdmin();
      Map<String, Boolean> validationResultMap;
      try {
        validationResultMap = admin.validateInstancesForWagedRebalance(clusterId,
            Collections.singletonList(instanceName));
      } catch (HelixException e) {
        return badRequest(e.getMessage());
      }
      return JSONRepresentation(validationResultMap);
    default:
      LOG.error("Unsupported command :" + command);
      return badRequest("Unsupported command :" + command);
    }
  }

  /**
   * Performs health checks for an instance to answer if it is stoppable.
   *
   * @param jsonContent json payload
   * @param clusterId cluster id
   * @param instanceName Instance name to be checked
   * @param skipZKRead skip reading from zk server
   * @param continueOnFailures whether or not continue to perform the subsequent checks if previous
   *                           check fails. If false, when helix own check fails, the subsequent
   *                           custom checks will not be performed.
   * @return json response representing if queried instance is stoppable
   * @throws IOException if there is any IO/network error
   */
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("stoppable")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response isInstanceStoppable(
      String jsonContent,
      @PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName,
      @QueryParam("skipZKRead") boolean skipZKRead,
      @QueryParam("continueOnFailures") boolean continueOnFailures) throws IOException {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    MaintenanceManagementService maintenanceService =
        new MaintenanceManagementService((ZKHelixDataAccessor) dataAccessor, getConfigAccessor(), skipZKRead,
            continueOnFailures, getNamespace());
    StoppableCheck stoppableCheck;
    try {
      JsonNode node = null;
      if (jsonContent.length() != 0) {
        node = OBJECT_MAPPER.readTree(jsonContent);
      }
      if (node == null) {
        return badRequest("Invalid input for content : " + jsonContent);
      }

      String customizedInput = null;
      if (node.get(InstancesAccessor.InstancesProperties.customized_values.name()) != null) {
        customizedInput = node.get(InstancesAccessor.InstancesProperties.customized_values.name()).toString();
      }

      stoppableCheck =
          maintenanceService.getInstanceStoppableCheck(clusterId, instanceName, customizedInput);
    } catch (HelixException e) {
      LOG.error("Current cluster: {}, instance: {} has issue with health checks!", clusterId,
          instanceName, e);
      return serverError(e);
    }
    return OK(OBJECT_MAPPER.writeValueAsString(stoppableCheck));
  }

  /**
   * Performs health checks, user designed operation check and execution for take an instance.
   *
   * @param jsonContent json payload
   * @param clusterId cluster id
   * @param instanceName Instance name to be checked
   * @return json response representing if queried instance is stoppable
   * @throws IOException if there is any IO/network error
   */
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("takeInstance")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response takeSingleInstance(
      String jsonContent,
      @PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName){

    try {
      MaintenanceOpInputFields inputFields = readMaintenanceInputFromJson(jsonContent);
      if (inputFields == null) {
        return badRequest("Invalid input for content : " + jsonContent);
      }

      MaintenanceManagementService maintenanceManagementService =
          new MaintenanceManagementService((ZKHelixDataAccessor) getDataAccssor(clusterId),
              getConfigAccessor(), inputFields.skipZKRead, inputFields.nonBlockingHelixCheck,
              getNamespace());

      return JSONRepresentation(maintenanceManagementService
          .takeInstance(clusterId, instanceName, inputFields.healthChecks,
              inputFields.healthCheckConfig,
              inputFields.operations,
              inputFields.operationConfig, inputFields.performOperation));
    } catch (Exception e) {
      LOG.error("Failed to takeInstances:", e);
      return badRequest("Failed to takeInstances: " + e.getMessage());
    }
  }

  /**
   * Performs health checks, user designed operation check and execution for free an instance.
   *
   * @param jsonContent json payload
   * @param clusterId cluster id
   * @param instanceName Instance name to be checked
   * @return json response representing if queried instance is stoppable
   * @throws IOException if there is any IO/network error
   */
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("freeInstance")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response freeSingleInstance(
      String jsonContent,
      @PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName){

    try {
      MaintenanceOpInputFields inputFields = readMaintenanceInputFromJson(jsonContent);
      if (inputFields == null) {
        return badRequest("Invalid input for content : " + jsonContent);
      }
      if (inputFields.healthChecks.size() != 0) {
        LOG.warn("freeSingleInstance won't perform user passed health check.");
      }

      MaintenanceManagementService maintenanceManagementService =
          new MaintenanceManagementService((ZKHelixDataAccessor) getDataAccssor(clusterId),
              getConfigAccessor(), inputFields.skipZKRead, inputFields.nonBlockingHelixCheck,
              getNamespace());

      return JSONRepresentation(maintenanceManagementService
          .freeInstance(clusterId, instanceName, inputFields.healthChecks,
              inputFields.healthCheckConfig,
              inputFields.operations,
              inputFields.operationConfig, inputFields.performOperation));
    } catch (Exception e) {
      LOG.error("Failed to takeInstances:", e);
      return badRequest("Failed to takeInstances: " + e.getMessage());
    }
  }

  private MaintenanceOpInputFields readMaintenanceInputFromJson(String jsonContent) throws IOException {
    JsonNode node = null;
    if (jsonContent.length() != 0) {
      node = OBJECT_MAPPER.readTree(jsonContent);
    }
    if (node == null) {
      return null;
    }
    MaintenanceOpInputFields inputFields = new MaintenanceOpInputFields();
    String continueOnFailuresName = PerInstanceProperties.continueOnFailures.name();
    String skipZKReadName = PerInstanceProperties.skipZKRead.name();
    String performOperation = PerInstanceProperties.performOperation.name();

    inputFields.healthChecks = MaintenanceManagementService
        .getListFromJsonPayload(node.get(PerInstanceProperties.health_check_list.name()));
    inputFields.healthCheckConfig = MaintenanceManagementService
        .getMapFromJsonPayload(node.get(PerInstanceProperties.health_check_config.name()));

    if (inputFields.healthCheckConfig != null) {
      if (inputFields.healthCheckConfig.containsKey(continueOnFailuresName)) {
        inputFields.nonBlockingHelixCheck = new HashSet<String>(MaintenanceManagementService
            .getListFromJsonPayload(inputFields.healthCheckConfig.get(continueOnFailuresName)));
        // healthCheckConfig will be passed to customer's health check directly, we need to
        // remove unrelated kc paris.
        inputFields.healthCheckConfig.remove(continueOnFailuresName);
      }
      if (inputFields.healthCheckConfig.containsKey(skipZKReadName)) {
        inputFields.skipZKRead =
            Boolean.parseBoolean(inputFields.healthCheckConfig.get(skipZKReadName));
        inputFields.healthCheckConfig.remove(skipZKReadName);
      }
    }

    inputFields.operations = MaintenanceManagementService
        .getListFromJsonPayload(node.get(PerInstanceProperties.operation_list.name()));
    inputFields.operationConfig = MaintenanceManagementService
        .getMapFromJsonPayload(node.get(PerInstanceProperties.operation_config.name()));
    if (inputFields.operationConfig != null && inputFields.operationConfig
        .containsKey(performOperation)) {
      inputFields.performOperation =
          Boolean.parseBoolean(inputFields.operationConfig.get(performOperation));
    }

    LOG.debug("Input fields for take/free Instance" + inputFields.toString());

    return inputFields;
  }


  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  public Response addInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName, String content) {
    HelixAdmin admin = getHelixAdmin();
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      LOG.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }

    try {
      admin.addInstance(clusterId, new InstanceConfig(record));
    } catch (Exception ex) {
      LOG.error("Error in adding an instance: " + instanceName, ex);
      return serverError(ex);
    }

    return OK();
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  public Response updateInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName, @QueryParam("command") String command,
      @QueryParam("instanceDisabledType") String disabledType,
      @QueryParam("instanceDisabledReason") String disabledReason, String content) {
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
          InstanceConstants.InstanceDisabledType disabledTypeEnum = null;
          if (disabledType != null) {
            try {
              disabledTypeEnum = InstanceConstants.InstanceDisabledType.valueOf(disabledType);
            } catch (IllegalArgumentException ex) {
              return badRequest("Invalid instanceDisabledType!");
            }
          }
          admin.enableInstance(clusterId, instanceName, false, disabledTypeEnum, disabledReason);
          break;

        case reset:
        case resetPartitions:
          if (!validInstance(node, instanceName)) {
            return badRequest("Instance names are not match!");
          }
          admin.resetPartition(clusterId, instanceName,
              node.get(PerInstanceProperties.resource.name()).textValue(),
              (List<String>) OBJECT_MAPPER
                  .readValue(node.get(PerInstanceProperties.partitions.name()).toString(),
                    OBJECT_MAPPER.getTypeFactory()
                        .constructCollectionType(List.class, String.class)));
        break;
      case addInstanceTag:
        if (!validInstance(node, instanceName)) {
          return badRequest("Instance names are not match!");
        }
        for (String tag : (List<String>) OBJECT_MAPPER
            .readValue(node.get(PerInstanceProperties.instanceTags.name()).toString(),
                OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class))) {
          admin.addInstanceTag(clusterId, instanceName, tag);
        }
        break;
      case removeInstanceTag:
        if (!validInstance(node, instanceName)) {
          return badRequest("Instance names are not match!");
        }
        for (String tag : (List<String>) OBJECT_MAPPER
            .readValue(node.get(PerInstanceProperties.instanceTags.name()).toString(),
                OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class))) {
          admin.removeInstanceTag(clusterId, instanceName, tag);
        }
        break;
      case enablePartitions:
        admin.enablePartition(true, clusterId, instanceName,
            node.get(PerInstanceProperties.resource.name()).textValue(),
            (List<String>) OBJECT_MAPPER
                .readValue(node.get(PerInstanceProperties.partitions.name()).toString(),
                    OBJECT_MAPPER.getTypeFactory()
                        .constructCollectionType(List.class, String.class)));
        break;
      case disablePartitions:
        admin.enablePartition(false, clusterId, instanceName,
            node.get(PerInstanceProperties.resource.name()).textValue(),
            (List<String>) OBJECT_MAPPER
                .readValue(node.get(PerInstanceProperties.partitions.name()).toString(),
                    OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class)));
        break;
      default:
        LOG.error("Unsupported command :" + command);
        return badRequest("Unsupported command :" + command);
      }
    } catch (Exception e) {
      LOG.error("Failed in updating instance : " + instanceName, e);
      return badRequest(e.getMessage());
    }
    return OK();
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @DELETE
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

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("configs")
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

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("configs")
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
      LOG.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }
    InstanceConfig instanceConfig = new InstanceConfig(record);
    ConfigAccessor configAccessor = getConfigAccessor();

    try {
      switch (command) {
        case update:
          /*
           * The new instanceConfig will be merged with existing one.
           * Even if the instance is disabled, non-valid instance topology config will cause rebalance
           * failure. We are doing the check whenever user updates InstanceConfig.
           */
          validateDeltaTopologySettingInInstanceConfig(clusterId, instanceName, configAccessor,
              instanceConfig, command);
          configAccessor.updateInstanceConfig(clusterId, instanceName, instanceConfig);
          break;
        case delete:
          validateDeltaTopologySettingInInstanceConfig(clusterId, instanceName, configAccessor,
              instanceConfig, command);
          HelixConfigScope instanceScope =
              new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
                  .forCluster(clusterId).forParticipant(instanceName).build();
          configAccessor.remove(instanceScope, record);
          break;
        default:
          return badRequest(String.format("Unsupported command: %s", command));
      }
    } catch (IllegalArgumentException ex) {
      LOG.error(String.format("Invalid topology setting for Instance : {}. Fail the config update",
          instanceName), ex);
      return serverError(ex);
    } catch (HelixException ex) {
      return notFound(ex.getMessage());
    } catch (Exception ex) {
      LOG.error(String.format("Error in update instance config for instance: %s", instanceName),
          ex);
      return serverError(ex);
    }
    return OK();
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("resources")
  public Response getResourcesOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), instanceName);
    ArrayNode resourcesNode = root.putArray(PerInstanceProperties.resources.name());

    List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
    if (!liveInstances.contains(instanceName)) {
      return null;
    }
    LiveInstance liveInstance =
        accessor.getProperty(accessor.keyBuilder().liveInstance(instanceName));

    // get the current session id
    String currentSessionId = liveInstance.getEphemeralOwner();

    List<String> resources =
        accessor.getChildNames(accessor.keyBuilder().currentStates(instanceName, currentSessionId));
    resources.addAll(accessor
        .getChildNames(accessor.keyBuilder().taskCurrentStates(instanceName, currentSessionId)));
    if (resources.size() > 0) {
      resourcesNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(resources));
    }

    return JSONRepresentation(root);
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET @Path("resources/{resourceName}")
  public Response getResourceOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName,
      @PathParam("resourceName") String resourceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
    if (!liveInstances.contains(instanceName)) {
      return notFound();
    }
    LiveInstance liveInstance =
        accessor.getProperty(accessor.keyBuilder().liveInstance(instanceName));

    // get the current session id
    String currentSessionId = liveInstance.getEphemeralOwner();
    CurrentState resourceCurrentState = accessor.getProperty(
        accessor.keyBuilder().currentState(instanceName, currentSessionId, resourceName));
    if (resourceCurrentState == null) {
      resourceCurrentState = accessor.getProperty(
          accessor.keyBuilder().taskCurrentState(instanceName, currentSessionId, resourceName));
    }
    if (resourceCurrentState != null) {
      return JSONRepresentation(resourceCurrentState.getRecord());
    }

    return notFound();
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("errors")
  public Response getErrorsOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), instanceName);
    ObjectNode errorsNode = JsonNodeFactory.instance.objectNode();

    List<String> sessionIds = accessor.getChildNames(accessor.keyBuilder().errors(instanceName));

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
    root.put(PerInstanceProperties.errors.name(), errorsNode);

    return JSONRepresentation(root);
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("errors/{sessionId}/{resourceName}/{partitionName}")
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

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("history")
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

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("messages")
  public Response getMessagesOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName,
      @QueryParam("stateModelDef") String stateModelDef) {
    HelixDataAccessor accessor = getDataAccssor(clusterId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), instanceName);
    ArrayNode newMessages = root.putArray(PerInstanceProperties.new_messages.name());
    ArrayNode readMessages = root.putArray(PerInstanceProperties.read_messages.name());

    List<String> messageNames =
        accessor.getChildNames(accessor.keyBuilder().messages(instanceName));
    if (messageNames == null || messageNames.size() == 0) {
      LOG.warn("Unable to get any messages on instance: " + instanceName);
      return notFound();
    }

    for (String messageName : messageNames) {
      Message message = accessor.getProperty(accessor.keyBuilder().message(instanceName, messageName));
      if (message == null) {
        LOG.warn("Message is deleted given message name: ", messageName);
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

    root.put(PerInstanceProperties.total_message_count.name(),
        newMessages.size() + readMessages.size());
    root.put(PerInstanceProperties.read_message_count.name(), readMessages.size());

    return JSONRepresentation(root);
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("messages/{messageId}")
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

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("healthreports")
  public Response getHealthReportsOnInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), instanceName);
    ArrayNode healthReportsNode = root.putArray(PerInstanceProperties.healthreports.name());

    List<String> healthReports =
        accessor.getChildNames(accessor.keyBuilder().healthReports(instanceName));

    if (healthReports != null && healthReports.size() > 0) {
      healthReportsNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(healthReports));
    }

    return JSONRepresentation(root);
  }

  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("healthreports/{reportName}")
  public Response getHealthReportsOnInstance(
      @PathParam("clusterId") String clusterId, @PathParam("instanceName") String instanceName,
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
    return instanceName.equals(node.get(Properties.id.name()).textValue());
  }

  private boolean validateDeltaTopologySettingInInstanceConfig(String clusterName,
      String instanceName, ConfigAccessor configAccessor, InstanceConfig newInstanceConfig,
      Command command) {
    InstanceConfig originalInstanceConfigCopy =
        configAccessor.getInstanceConfig(clusterName, instanceName);
    if (command == Command.delete) {
      for (Map.Entry<String, String> entry : newInstanceConfig.getRecord().getSimpleFields()
          .entrySet()) {
        originalInstanceConfigCopy.getRecord().getSimpleFields().remove(entry.getKey());
      }
    } else {
      originalInstanceConfigCopy.getRecord().update(newInstanceConfig.getRecord());
    }

    return originalInstanceConfigCopy
        .validateTopologySettingInInstanceConfig(configAccessor.getClusterConfig(clusterName),
            instanceName);
  }
}
