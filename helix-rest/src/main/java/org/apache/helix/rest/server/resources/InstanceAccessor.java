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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Error;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.ParticipantHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

@Path("/clusters/{clusterId}/instances")
public class InstanceAccessor extends AbstractResource {
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
    instanceTags
  }

  @GET
  public Response getInstances(@PathParam("clusterId") String clusterId) {
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

  @GET
  @Path("{instanceName}")
  public Response getInstance(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    Map<String, Object> instanceMap = new HashMap<>();
    instanceMap.put(Properties.id.name(), JsonNodeFactory.instance.textNode(instanceName));
    instanceMap.put(InstanceProperties.liveInstance.name(), null);

    InstanceConfig instanceConfig =
        accessor.getProperty(accessor.keyBuilder().instanceConfig(instanceName));
    LiveInstance liveInstance =
        accessor.getProperty(accessor.keyBuilder().liveInstance(instanceName));

    if (instanceConfig != null) {
      instanceMap.put(InstanceProperties.config.name(), instanceConfig.getRecord());
    } else {
      return notFound();
    }

    if (liveInstance != null) {
      instanceMap.put(InstanceProperties.liveInstance.name(), liveInstance.getRecord());
    }

    return JSONRepresentation(instanceMap);
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
        if (!validInstance(node, instanceName)) {
          return badRequest("Instance names are not match!");
        }
        admin.resetPartition(clusterId, instanceName,
            node.get(InstanceProperties.resource.name()).toString(), (List<String>) OBJECT_MAPPER
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

  @PUT
  @Path("{instanceName}/configs")
  public Response updateInstanceConfig(@PathParam("clusterId") String clusterId,
      @PathParam("instanceName") String instanceName, String content) throws IOException {
    HelixAdmin admin = getHelixAdmin();
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }

    try {
      admin.setInstanceConfig(clusterId, instanceName, new InstanceConfig(record));
    } catch (Exception ex) {
      _logger.error("Error in update instance config: " + instanceName, ex);
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
      @PathParam("instanceName") String instanceName) throws IOException {
    HelixDataAccessor accessor = getDataAccssor(clusterId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), instanceName);
    ArrayNode newMessages = root.putArray(InstanceProperties.new_messages.name());
    ArrayNode readMessages = root.putArray(InstanceProperties.read_messages.name());


    List<String> messages =
        accessor.getChildNames(accessor.keyBuilder().messages(instanceName));
    if (messages == null || messages.size() == 0) {
      return notFound();
    }

    for (String messageName : messages) {
      Message message = accessor.getProperty(accessor.keyBuilder().message(instanceName, messageName));
      if (message.getMsgState() == Message.MessageState.NEW) {
        newMessages.add(messageName);
      }

      if (message.getMsgState() == Message.MessageState.READ) {
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
}
