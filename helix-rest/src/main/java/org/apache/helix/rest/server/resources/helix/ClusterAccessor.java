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
import java.util.HashMap;
import java.util.HashSet;
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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.api.exceptions.HelixConflictException;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.api.status.ClusterManagementModeRequest;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.rest.acl.AclRegister;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.helix.rest.server.filters.NamespaceAuth;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.apache.helix.rest.server.service.ClusterService;
import org.apache.helix.rest.server.service.ClusterServiceImpl;
import org.apache.helix.rest.server.service.VirtualTopologyGroupService;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;


@Path("/clusters")
@Api (value = "", description = "Helix REST Clusters  APIs")
public class ClusterAccessor extends AbstractHelixResource {
  private static Logger LOG = LoggerFactory.getLogger(ClusterAccessor.class.getName());

  public enum ClusterProperties {
    controller,
    instances,
    liveInstances,
    resources,
    paused,
    maintenance,
    messages,
    stateModelDefinitions,
    clusters,
    maintenanceSignal,
    maintenanceHistory,
    clusterName
  }

  @NamespaceAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @ApiOperation (value = "Return list of all clusters", notes = "Helix REST Cluster Get API")
  public Response getClusters() {
    HelixAdmin helixAdmin = getHelixAdmin();
    List<String> clusters = helixAdmin.getClusters();

    Map<String, List<String>> dataMap = new HashMap<>();
    dataMap.put(ClusterProperties.clusters.name(), clusters);

    return JSONRepresentation(dataMap);
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @ApiOperation (value = "Return information for particular cluster", notes = "Helix REST Cluster  Get API")
  @Path("{clusterId}")
  public Response getClusterInfo(@PathParam("clusterId") String clusterId) {
    if (!doesClusterExist(clusterId)) {
      return notFound();
    }

    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    Map<String, Object> clusterInfo = new HashMap<>();
    clusterInfo.put(Properties.id.name(), clusterId);

    LiveInstance controller = dataAccessor.getProperty(keyBuilder.controllerLeader());
    if (controller != null) {
      clusterInfo.put(ClusterProperties.controller.name(), controller.getInstanceName());
    } else {
      clusterInfo.put(ClusterProperties.controller.name(), "No Lead Controller!");
    }

    boolean paused = dataAccessor.getBaseDataAccessor()
        .exists(keyBuilder.pause().getPath(), AccessOption.PERSISTENT);
    clusterInfo.put(ClusterProperties.paused.name(), paused);
    boolean maintenance = getHelixAdmin().isInMaintenanceMode(clusterId);
    clusterInfo.put(ClusterProperties.maintenance.name(), maintenance);

    List<String> idealStates = dataAccessor.getChildNames(keyBuilder.idealStates());
    clusterInfo.put(ClusterProperties.resources.name(), idealStates);
    List<String> instances = dataAccessor.getChildNames(keyBuilder.instanceConfigs());
    clusterInfo.put(ClusterProperties.instances.name(), instances);
    List<String> liveInstances = dataAccessor.getChildNames(keyBuilder.liveInstances());
    clusterInfo.put(ClusterProperties.liveInstances.name(), liveInstances);

    return JSONRepresentation(clusterInfo);
  }

  @NamespaceAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  @Path("{clusterId}")
  public Response createCluster(@PathParam("clusterId") String clusterId,
      @DefaultValue("false") @QueryParam("recreate") String recreate,
      @DefaultValue("false") @QueryParam("addCloudConfig") String addCloudConfig,
      String cloudConfigManifest) {

    boolean recreateIfExists = Boolean.parseBoolean(recreate);
    boolean cloudConfigIncluded = Boolean.parseBoolean(addCloudConfig);

    ClusterSetup clusterSetup = getClusterSetup();

    CloudConfig cloudConfig = null;
    if (cloudConfigIncluded) {
      ZNRecord record;
      try {
        record = toZNRecord(cloudConfigManifest);
        cloudConfig = new CloudConfig.Builder(record).build();
      } catch (IOException | HelixException e) {
        String errMsg = "Failed to generate a valid CloudConfig from " + cloudConfigManifest;
        LOG.error(errMsg, e);
        return badRequest(errMsg + " Exception: " + e.getMessage());
      }
    }

    try {
      getAclRegister().createACL(_servletRequest);
    } catch (Exception ex) {
      LOG.error("Failed to create ACL for cluster {}. Exception: {}.", clusterId, ex);
      return serverError(ex);
    }

    try {
      clusterSetup.addCluster(clusterId, recreateIfExists, cloudConfig);
    } catch (Exception ex) {
      LOG.error("Failed to create cluster {}. Exception: {}.", clusterId, ex);
      return serverError(ex);
    }
    return created();
  }

  @NamespaceAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @DELETE
  @Path("{clusterId}")
  public Response deleteCluster(@PathParam("clusterId") String clusterId) {
    ClusterSetup clusterSetup = getClusterSetup();

    try {
      clusterSetup.deleteCluster(clusterId);
    } catch (HelixException ex) {
      LOG.info("Failed to delete cluster {}, cluster is still in use. Exception: {}.", clusterId,
          ex);
      return badRequest(ex.getMessage());
    } catch (Exception ex) {
      LOG.error("Failed to delete cluster {}. Exception: {}.", clusterId, ex);
      return serverError(ex);
    }

    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{clusterId}")
  public Response updateCluster(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String commandStr, @QueryParam("superCluster") String superCluster,
      @QueryParam("duration") Long duration, String content) {
    Command command;
    try {
      command = getCommand(commandStr);
    } catch (HelixException ex) {
      return badRequest(ex.getMessage());
    }

    ClusterSetup clusterSetup = getClusterSetup();
    HelixAdmin helixAdmin = getHelixAdmin();

    switch (command) {
      case activate:
        if (superCluster == null) {
          return badRequest("Super Cluster name is missing!");
        }
        try {
          clusterSetup.activateCluster(clusterId, superCluster, true);
        } catch (Exception ex) {
          LOG.error("Failed to add cluster {} to super cluster {}.", clusterId, superCluster);
          return serverError(ex);
        }
        break;

      case deactivate:
        if (superCluster == null) {
          return badRequest("Super Cluster name is missing!");
        }
        try {
          clusterSetup.activateCluster(clusterId, superCluster, false);
        } catch (Exception ex) {
          LOG.error("Failed to deactivate cluster {} from super cluster {}.", clusterId, superCluster);
          return serverError(ex);
        }
        break;

      case addVirtualTopologyGroup:
        try {
          addVirtualTopologyGroup(clusterId, content);
        } catch (JsonProcessingException ex) {
          LOG.error("Failed to parse json string: {}", content, ex);
          return badRequest("Invalid payload json body: " + content);
        } catch (IllegalArgumentException ex) {
          LOG.error("Illegal input {} for command {}.", content, command, ex);
          return badRequest(String.format("Illegal input %s for command %s", content, command));
        } catch (Exception ex) {
          LOG.error("Failed to add virtual topology group to cluster {}", clusterId, ex);
          return serverError(ex);
        }
        break;

      case expand:
        try {
          clusterSetup.expandCluster(clusterId);
        } catch (Exception ex) {
          LOG.error("Failed to expand cluster {}.", clusterId);
          return serverError(ex);
        }
        break;

      case enable:
        try {
          helixAdmin.enableCluster(clusterId, true);
        } catch (Exception ex) {
          LOG.error("Failed to enable cluster {}.", clusterId);
          return serverError(ex);
        }
        break;

      case disable:
        try {
          helixAdmin.enableCluster(clusterId, false);
        } catch (Exception ex) {
          LOG.error("Failed to disable cluster {}.", clusterId);
          return serverError(ex);
        }
        break;

      case enableMaintenanceMode:
      case disableMaintenanceMode:
        // Try to parse the content string. If parseable, use it as a KV mapping. Otherwise, treat it
        // as a REASON String
        Map<String, String> customFieldsMap = null;
        // Default to USER triggering entity
        boolean isAutomationTriggered = false;
        try {
          // Try to parse content
          customFieldsMap =
              OBJECT_MAPPER.readValue(content, new TypeReference<HashMap<String, String>>() {
              });
          // content is given as a KV mapping. Nullify content unless (case-insensitive) reason key present in map
          content = null;
          for (Map.Entry<String, String> entry : customFieldsMap.entrySet()) {
            if ("reason".equalsIgnoreCase(entry.getKey())) {
              content = entry.getValue();
            }
            if ("isAutomation".equalsIgnoreCase(entry.getKey())) {
              isAutomationTriggered = Boolean.parseBoolean(entry.getValue());
            }
          }
        } catch (Exception e) {
          // NOP
        }

        if (customFieldsMap != null) {
          customFieldsMap.entrySet().removeIf(entry ->
              "isAutomation".equalsIgnoreCase(entry.getKey()) ||
                  "reason".equalsIgnoreCase(entry.getKey()));
        }

        if (isAutomationTriggered) {
          helixAdmin
              .automationEnableMaintenanceMode(clusterId, command == Command.enableMaintenanceMode,
                  content, customFieldsMap);
        } else {
          helixAdmin
              .manuallyEnableMaintenanceMode(clusterId, command == Command.enableMaintenanceMode,
                  content, customFieldsMap);
        }
        break;
      case enableWagedRebalanceForAllResources:
        // Enable WAGED rebalance for all resources in the cluster
        List<String> resources = helixAdmin.getResourcesInCluster(clusterId);
        try {
          helixAdmin.enableWagedRebalance(clusterId, resources);
        } catch (HelixException e) {
          return badRequest(e.getMessage());
        }
        break;
      case purgeOfflineParticipants:
        if (duration == null || duration < 0) {
          helixAdmin
              .purgeOfflineInstances(clusterId, ClusterConfig.OFFLINE_DURATION_FOR_PURGE_NOT_SET);
        } else {
          helixAdmin.purgeOfflineInstances(clusterId, duration);
        }
        break;
      case onDemandRebalance:
        try {
          helixAdmin.onDemandRebalance(clusterId);
        } catch (Exception ex) {
          LOG.error(
              "Cannot start on-demand rebalance for cluster: {}, Exception: {}", clusterId, ex);
          return serverError(ex);
        }
        break;
      default:
        return badRequest("Unsupported command {}." + command);
    }

    return OK();
  }

  private void addVirtualTopologyGroup(String clusterId, String content) throws JsonProcessingException {
    ClusterService clusterService = new ClusterServiceImpl(getDataAccssor(clusterId), getConfigAccessor());
    VirtualTopologyGroupService service = new VirtualTopologyGroupService(
        getHelixAdmin(), clusterService, getConfigAccessor(), getDataAccssor(clusterId));
    Map<String, String> customFieldsMap =
        OBJECT_MAPPER.readValue(content, new TypeReference<HashMap<String, String>>() { });
    service.addVirtualTopologyGroup(clusterId, customFieldsMap);
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/management-mode")
  public Response getClusterManagementMode(@PathParam("clusterId") String clusterId,
      @QueryParam("showDetails") boolean showDetails) {
    ClusterManagementMode mode = getHelixAdmin().getClusterManagementMode(clusterId);
    if (mode == null) {
      return notFound("Cluster " + clusterId + " is not in management mode");
    }

    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put("cluster", clusterId);
    responseMap.put("mode", mode.getMode());
    responseMap.put("status", mode.getStatus());
    if (showDetails) {
      // To show details, query participants that are in progress to management mode.
      responseMap.put("details", getManagementModeDetails(clusterId, mode));
    }

    return JSONRepresentation(responseMap);
  }

  private Map<String, Object> getManagementModeDetails(String clusterId,
      ClusterManagementMode mode) {
    Map<String, Object> details = new HashMap<>();
    Map<String, Object> participantDetails = new HashMap<>();
    ClusterManagementMode.Status status = mode.getStatus();
    details.put("cluster", ImmutableMap.of("cluster", clusterId, "status", status.name()));

    boolean hasPendingST = false;
    Set<String> liveInstancesInProgress = new HashSet<>();

    if (ClusterManagementMode.Status.IN_PROGRESS.equals(status)) {
      HelixDataAccessor accessor = getDataAccssor(clusterId);
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      List<LiveInstance> liveInstances = accessor.getChildValues(keyBuilder.liveInstances());
      BaseDataAccessor<ZNRecord> baseAccessor = accessor.getBaseDataAccessor();

      if (ClusterManagementMode.Type.CLUSTER_FREEZE.equals(mode.getMode())) {
        // Entering cluster freeze mode, check live instance freeze status and pending ST
        for (LiveInstance liveInstance : liveInstances) {
          String instanceName = liveInstance.getInstanceName();
          if (!LiveInstance.LiveInstanceStatus.FROZEN.equals(liveInstance.getStatus())) {
            liveInstancesInProgress.add(instanceName);
          }
          Stat stat = baseAccessor
              .getStat(keyBuilder.messages(instanceName).getPath(), AccessOption.PERSISTENT);
          if (stat.getNumChildren() > 0) {
            hasPendingST = true;
            liveInstancesInProgress.add(instanceName);
          }
        }
      } else if (ClusterManagementMode.Type.NORMAL.equals(mode.getMode())) {
        // Exiting freeze mode, check live instance unfreeze status
        for (LiveInstance liveInstance : liveInstances) {
          if (LiveInstance.LiveInstanceStatus.FROZEN.equals(liveInstance.getStatus())) {
            liveInstancesInProgress.add(liveInstance.getInstanceName());
          }
        }
      }
    }

    participantDetails.put("status", status.name());
    participantDetails.put("liveInstancesInProgress", liveInstancesInProgress);
    if (ClusterManagementMode.Type.CLUSTER_FREEZE.equals(mode.getMode())) {
      // Add pending ST result for cluster freeze mode
      participantDetails.put("hasPendingStateTransition", hasPendingST);
    }

    details.put(ClusterProperties.liveInstances.name(), participantDetails);
    return details;
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{clusterId}/management-mode")
  public Response updateClusterManagementMode(@PathParam("clusterId") String clusterId,
      @DefaultValue("{}") String content) {
    ClusterManagementModeRequest request;
    try {
      request = OBJECT_MAPPER.readerFor(ClusterManagementModeRequest.class).readValue(content);
    } catch (JsonProcessingException e) {
      LOG.warn("Failed to parse json string: {}", content, e);
      return badRequest("Invalid payload json body: " + content);
    }

    // Need to add cluster name
    request = ClusterManagementModeRequest.newBuilder()
        .withClusterName(clusterId)
        .withMode(request.getMode())
        .withCancelPendingST(request.isCancelPendingST())
        .withReason(request.getReason())
        .build();

    try {
      getHelixAdmin().setClusterManagementMode(request);
    } catch (HelixConflictException e) {
      return Response.status(Response.Status.CONFLICT).entity(e.getMessage()).build();
    } catch (HelixException e) {
      return serverError(e.getMessage());
    }

    return JSONRepresentation(ImmutableMap.of("acknowledged", true));
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/configs")
  public Response getClusterConfig(@PathParam("clusterId") String clusterId) {
    ConfigAccessor accessor = getConfigAccessor();
    ClusterConfig config = null;
    try {
      config = accessor.getClusterConfig(clusterId);
    } catch (HelixException ex) {
      // cluster not found.
      LOG.info("Failed to get cluster config for cluster {}, cluster not found. Exception: {}.",
          clusterId, ex);
    } catch (Exception ex) {
      LOG.error("Failed to get cluster config for cluster {}. Exception: {}", clusterId, ex);
      return serverError(ex);
    }
    if (config == null) {
      return notFound();
    }
    return JSONRepresentation(config.getRecord());
  }


  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  @Path("{clusterId}/customized-state-config")
  public Response addCustomizedStateConfig(@PathParam("clusterId") String clusterId,
      String content) {
    if (!doesClusterExist(clusterId)) {
      return notFound(String.format("Cluster %s does not exist", clusterId));
    }

    HelixAdmin admin = getHelixAdmin();
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      return badRequest("Input is not a vaild ZNRecord!");
    }

    try {
      CustomizedStateConfig customizedStateConfig =
          new CustomizedStateConfig.Builder(record).build();
      admin.addCustomizedStateConfig(clusterId, customizedStateConfig);
    } catch (Exception ex) {
      LOG.error("Cannot add CustomizedStateConfig to cluster: {} Exception: {}",
          clusterId, ex);
      return serverError(ex);
    }

    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @DELETE
  @Path("{clusterId}/customized-state-config")
  public Response removeCustomizedStateConfig(@PathParam("clusterId") String clusterId) {
    if (!doesClusterExist(clusterId)) {
      return notFound(String.format("Cluster %s does not exist", clusterId));
    }

    HelixAdmin admin = getHelixAdmin();
    try {
      admin.removeCustomizedStateConfig(clusterId);
    } catch (Exception ex) {
      LOG.error(
          "Cannot remove CustomizedStateConfig from cluster: {}, Exception: {}",
          clusterId, ex);
      return serverError(ex);
    }

    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/customized-state-config")
  public Response getCustomizedStateConfig(@PathParam("clusterId") String clusterId) {
    if (!doesClusterExist(clusterId)) {
      return notFound(String.format("Cluster %s does not exist", clusterId));
    }

    ConfigAccessor configAccessor = getConfigAccessor();
    CustomizedStateConfig customizedStateConfig =
        configAccessor.getCustomizedStateConfig(clusterId);

    if (customizedStateConfig != null) {
      return JSONRepresentation(customizedStateConfig.getRecord());
    }

    return notFound();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{clusterId}/customized-state-config")
  public Response updateCustomizedStateConfig(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String commandStr, @QueryParam("type") String type) {
    if (!doesClusterExist(clusterId)) {
      return notFound(String.format("Cluster %s does not exist", clusterId));
    }

    Command command;
    if (commandStr == null || commandStr.isEmpty()) {
      command = Command.add; // Default behavior
    } else {
      try {
        command = getCommand(commandStr);
      } catch (HelixException ex) {
        return badRequest(ex.getMessage());
      }
    }

    HelixAdmin admin = getHelixAdmin();

    try {
      switch (command) {
      case delete:
        admin.removeTypeFromCustomizedStateConfig(clusterId, type);
        break;
      case add:
        admin.addTypeToCustomizedStateConfig(clusterId, type);
        break;
      default:
        return badRequest("Unsupported command " + commandStr);
      }
    } catch (Exception ex) {
      LOG.error("Failed to {} CustomizedStateConfig for cluster {} new type: {}, Exception: {}", command, clusterId, type, ex);
      return serverError(ex);
    }
    return OK();
  }


  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/topology")
  public Response getClusterTopology(@PathParam("clusterId") String clusterId) throws IOException {
    //TODO reduce the GC by dependency injection
    ClusterService clusterService =
        new ClusterServiceImpl(getDataAccssor(clusterId), getConfigAccessor());
    ObjectMapper objectMapper = new ObjectMapper();
    ClusterTopology clusterTopology = clusterService.getClusterTopology(clusterId);

    return OK(objectMapper.writeValueAsString(clusterTopology));
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/topologymap")
  public Response getClusterTopologyMap(@PathParam("clusterId") String clusterId) {
    HelixAdmin admin = getHelixAdmin();
    Map<String, List<String>> topologyMap;
    try {
      topologyMap = admin.getClusterTopology(clusterId).getTopologyMap();
    } catch (HelixException ex) {
      return badRequest(ex.getMessage());
    }
    return JSONRepresentation(topologyMap);
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/faultzonemap")
  public Response getClusterFaultZoneMap(@PathParam("clusterId") String clusterId) {
    HelixAdmin admin = getHelixAdmin();
    Map<String, List<String>> faultZoneMap;
    try {
      faultZoneMap = admin.getClusterTopology(clusterId).getFaultZoneMap();
    } catch (HelixException ex) {
      return badRequest(ex.getMessage());
    }
    return JSONRepresentation(faultZoneMap);
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{clusterId}/configs")
  public Response updateClusterConfig(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String commandStr, String content) {
    Command command;
    try {
      command = getCommand(commandStr);
    } catch (HelixException ex) {
      return badRequest(ex.getMessage());
    }

    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      LOG.error("Failed to deserialize user's input {}. Exception: {}.", content, e);
      return badRequest("Input is not a valid ZNRecord!");
    }

    if (!clusterId.equals(record.getId())) {
      return badRequest("ID does not match the cluster name in input!");
    }

    ClusterConfig config = new ClusterConfig(record);
    ConfigAccessor configAccessor = getConfigAccessor();
    try {
      switch (command) {
        case update:
          validateClusterConfigChange(clusterId, configAccessor, config, command);
          configAccessor.updateClusterConfig(clusterId, config);
          break;
        case delete: {
          validateClusterConfigChange(clusterId, configAccessor, config, command);
          HelixConfigScope clusterScope =
              new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
                  .forCluster(clusterId).build();
          configAccessor.remove(clusterScope, config.getRecord());
        }
        break;

        default:
          return badRequest("Unsupported command " + commandStr);
      }
    } catch (HelixException ex) {
      return notFound(ex.getMessage());
    } catch (Exception ex) {
      LOG
          .error("Failed to {} cluster config, cluster {}, new config: {}. Exception: {}.", command,
              clusterId, content, ex);
      return serverError(ex);
    }
    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/controller")
  public Response getClusterController(@PathParam("clusterId") String clusterId) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    Map<String, Object> controllerInfo = new HashMap<>();
    controllerInfo.put(Properties.id.name(), clusterId);

    LiveInstance leader = dataAccessor.getProperty(dataAccessor.keyBuilder().controllerLeader());
    if (leader != null) {
      controllerInfo.put(ClusterProperties.controller.name(), leader.getInstanceName());
      controllerInfo.putAll(leader.getRecord().getSimpleFields());
    } else {
      controllerInfo.put(ClusterProperties.controller.name(), "No Lead Controller!");
    }

    return JSONRepresentation(controllerInfo);
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/controller/history")
  public Response getClusterControllerLeadershipHistory(@PathParam("clusterId") String clusterId) {
    return JSONRepresentation(
        getControllerHistory(clusterId, ControllerHistory.HistoryType.CONTROLLER_LEADERSHIP));
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/controller/maintenanceHistory")
  public Response getClusterMaintenanceHistory(@PathParam("clusterId") String clusterId) {
    return JSONRepresentation(
        getControllerHistory(clusterId, ControllerHistory.HistoryType.MAINTENANCE));
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/controller/maintenanceSignal")
  public Response getClusterMaintenanceSignal(@PathParam("clusterId") String clusterId) {
    boolean inMaintenanceMode = getHelixAdmin().isInMaintenanceMode(clusterId);

    if (inMaintenanceMode) {
      HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
      MaintenanceSignal maintenanceSignal = dataAccessor.getProperty(dataAccessor.keyBuilder().maintenance());

      Map<String, String> maintenanceInfo = (maintenanceSignal != null) ?
          maintenanceSignal.getRecord().getSimpleFields() : new HashMap<>();
      maintenanceInfo.put(ClusterProperties.clusterName.name(), clusterId);

      return JSONRepresentation(maintenanceInfo);
    }
    return notFound(String.format("Cluster %s is not in maintenance mode!", clusterId));
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/controller/messages")
  public Response getClusterControllerMessages(@PathParam("clusterId") String clusterId) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);

    Map<String, Object> controllerMessages = new HashMap<>();
    controllerMessages.put(Properties.id.name(), clusterId);

    List<String> messages =
        dataAccessor.getChildNames(dataAccessor.keyBuilder().controllerMessages());
    controllerMessages.put(ClusterProperties.messages.name(), messages);
    controllerMessages.put(Properties.count.name(), messages.size());

    return JSONRepresentation(controllerMessages);
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/controller/messages/{messageId}")
  public Response getClusterControllerMessages(@PathParam("clusterId") String clusterId,
      @PathParam("messageId") String messageId) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    Message message =
        dataAccessor.getProperty(dataAccessor.keyBuilder().controllerMessage(messageId));
    return JSONRepresentation(message.getRecord());
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/statemodeldefs")
  public Response getClusterStateModelDefinitions(@PathParam("clusterId") String clusterId) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    List<String> stateModelDefs =
        dataAccessor.getChildNames(dataAccessor.keyBuilder().stateModelDefs());

    Map<String, Object> clusterStateModelDefs = new HashMap<>();
    clusterStateModelDefs.put(Properties.id.name(), clusterId);
    clusterStateModelDefs.put(ClusterProperties.stateModelDefinitions.name(), stateModelDefs);

    return JSONRepresentation(clusterStateModelDefs);
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/statemodeldefs/{statemodel}")
  public Response getClusterStateModelDefinition(@PathParam("clusterId") String clusterId,
      @PathParam("statemodel") String statemodel) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    StateModelDefinition stateModelDef =
        dataAccessor.getProperty(dataAccessor.keyBuilder().stateModelDef(statemodel));

    if (stateModelDef == null) {
      return badRequest("Statemodel not found!");
    }
    return JSONRepresentation(stateModelDef.getRecord());
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  @Path("{clusterId}/statemodeldefs/{statemodel}")
  public Response createClusterStateModelDefinition(@PathParam("clusterId") String clusterId,
      @PathParam("statemodel") String statemodel, String content) {
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      LOG.error("Failed to deserialize user's input {}. Exception: {}.", content, e);
      return badRequest("Input is not a valid ZNRecord!");
    }
    RealmAwareZkClient zkClient = getRealmAwareZkClient();
    String path = PropertyPathBuilder.stateModelDef(clusterId);
    try {
      ZKUtil.createChildren(zkClient, path, record);
    } catch (Exception e) {
      LOG.error("Failed to create zk node with path {}. Exception: {}", path, e);
      return badRequest("Failed to create a Znode for stateModel! " + e);
    }

    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{clusterId}/statemodeldefs/{statemodel}")
  public Response setClusterStateModelDefinition(@PathParam("clusterId") String clusterId,
      @PathParam("statemodel") String statemodel, String content) {
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      LOG.error("Failed to deserialize user's input {}. Exception: {}.", content, e);
      return badRequest("Input is not a valid ZNRecord!");
    }

    StateModelDefinition stateModelDefinition = new StateModelDefinition(record);
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);

    PropertyKey key = dataAccessor.keyBuilder().stateModelDef(stateModelDefinition.getId());
    boolean retcode = true;
    try {
      retcode = dataAccessor.setProperty(key, stateModelDefinition);
    } catch (Exception e) {
      LOG.error("Failed to set StateModelDefinition key: {}. Exception: {}.", key, e);
      return badRequest("Failed to set the content " + content);
    }

    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @DELETE
  @Path("{clusterId}/statemodeldefs/{statemodel}")
  public Response removeClusterStateModelDefinition(@PathParam("clusterId") String clusterId,
      @PathParam("statemodel") String statemodel) {
    //Shall we validate the statemodel string not having special character such as ../ etc?
    if (!StringUtils.isAlphanumeric(statemodel)) {
      return badRequest("Invalid statemodel name!");
    }

    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    PropertyKey key = dataAccessor.keyBuilder().stateModelDef(statemodel);
    boolean retcode = true;
    try {
      retcode = dataAccessor.removeProperty(key);
    } catch (Exception e) {
      LOG.error("Failed to remove StateModelDefinition key: {}. Exception: {}.", key, e);
      retcode = false;
    }
    if (!retcode) {
      return badRequest("Failed to remove!");
    }
    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  @Path("{clusterId}/restconfig")
  public Response createRESTConfig(@PathParam("clusterId") String clusterId,
      String content) {
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      LOG.error("Failed to deserialize user's input {}. Exception: {}.", content, e);
      return badRequest("Input is not a valid ZNRecord!");
    }

    if (!record.getId().equals(clusterId)) {
      return badRequest("ID does not match the cluster name in input!");
    }

    RESTConfig config = new RESTConfig(record);
    ConfigAccessor configAccessor = getConfigAccessor();
    try {
      configAccessor.setRESTConfig(clusterId, config);
    } catch (HelixException ex) {
      // TODO: Could use a more generic error for HelixException
      return notFound(ex.getMessage());
    } catch (Exception ex) {
      LOG.error("Failed to create rest config, cluster {}, new config: {}. Exception: {}.", clusterId, content, ex);
      return serverError(ex);
    }
    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{clusterId}/restconfig")
  public Response updateRESTConfig(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String commandStr, String content) {
    //TODO: abstract out the logic that is duplicated from cluster config methods
    Command command;
    try {
      command = getCommand(commandStr);
    } catch (HelixException ex) {
      return badRequest(ex.getMessage());
    }

    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      LOG.error("Failed to deserialize user's input {}. Exception: {}", content, e);
      return badRequest("Input is not a valid ZNRecord!");
    }

    RESTConfig config = new RESTConfig(record);
    ConfigAccessor configAccessor = getConfigAccessor();
    try {
      switch (command) {
        case update:
          configAccessor.updateRESTConfig(clusterId, config);
          break;
        case delete: {
          HelixConfigScope scope =
              new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.REST)
                  .forCluster(clusterId).build();
          configAccessor.remove(scope, config.getRecord());
        }
        break;
        default:
          return badRequest("Unsupported command " + commandStr);
      }
    } catch (HelixException ex) {
      return notFound(ex.getMessage());
    } catch (Exception ex) {
      LOG.error(
          "Failed to {} rest config, cluster {}, new config: {}. Exception: {}", command, clusterId, content, ex);
      return serverError(ex);
    }
    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/restconfig")
  public Response getRESTConfig(@PathParam("clusterId") String clusterId) {
    ConfigAccessor accessor = getConfigAccessor();
    RESTConfig config = null;
    try {
      config = accessor.getRESTConfig(clusterId);
    } catch (HelixException ex) {
      LOG.info(
          "Failed to get rest config for cluster {}, cluster not found. Exception: {}.", clusterId, ex);
    } catch (Exception ex) {
      LOG.error("Failed to get rest config for cluster {}. Exception: {}.", clusterId, ex);
      return serverError(ex);
    }
    if (config == null) {
      return notFound();
    }
    return JSONRepresentation(config.getRecord());
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @DELETE
  @Path("{clusterId}/restconfig")
  public Response deleteRESTConfig(@PathParam("clusterId") String clusterId) {
    ConfigAccessor accessor = getConfigAccessor();
    try {
      accessor.deleteRESTConfig(clusterId);
    } catch (HelixException ex) {
      LOG.info("Failed to delete rest config for cluster {}, cluster rest config is not found. Exception: {}.", clusterId, ex);
      return notFound(ex.getMessage());
    } catch (Exception ex) {
      LOG.error("Failed to delete rest config, cluster {}, Exception: {}.", clusterId, ex);
      return serverError(ex);
    }
    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/maintenance")
  public Response getClusterMaintenanceMode(@PathParam("clusterId") String clusterId) {
    return JSONRepresentation(ImmutableMap
        .of(ClusterProperties.maintenance.name(), getHelixAdmin().isInMaintenanceMode(clusterId)));
  }

  private boolean doesClusterExist(String cluster) {
    RealmAwareZkClient zkClient = getRealmAwareZkClient();
    return ZKUtil.isClusterSetup(cluster, zkClient);
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  @Path("{clusterId}/cloudconfig")
  public Response addCloudConfig(@PathParam("clusterId") String clusterId, String content) {

    RealmAwareZkClient zkClient = getRealmAwareZkClient();
    if (!ZKUtil.isClusterSetup(clusterId, zkClient)) {
      return notFound("Cluster is not properly setup!");
    }

    HelixAdmin admin = getHelixAdmin();
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      LOG.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }

    try {
      CloudConfig cloudConfig = new CloudConfig.Builder(record).build();
      admin.addCloudConfig(clusterId, cloudConfig);
    } catch (HelixException ex) {
      LOG.error("Error in adding a CloudConfig to cluster: " + clusterId, ex);
      return badRequest(ex.getMessage());
    } catch (Exception ex) {
      LOG.error("Cannot add CloudConfig to cluster: " + clusterId, ex);
      return serverError(ex);
    }

    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{clusterId}/cloudconfig")
  public Response getCloudConfig(@PathParam("clusterId") String clusterId) {

    RealmAwareZkClient zkClient = getRealmAwareZkClient();
    if (!ZKUtil.isClusterSetup(clusterId, zkClient)) {
      return notFound();
    }

    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    CloudConfig cloudConfig = configAccessor.getCloudConfig(clusterId);

    if (cloudConfig != null) {
      return JSONRepresentation(cloudConfig.getRecord());
    }

    return notFound();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @DELETE
  @Path("{clusterId}/cloudconfig")
  public Response deleteCloudConfig(@PathParam("clusterId") String clusterId) {
    HelixAdmin admin = getHelixAdmin();
    admin.removeCloudConfig(clusterId);
    return OK();
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{clusterId}/cloudconfig")
  public Response updateCloudConfig(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String commandStr, String content) {

    RealmAwareZkClient zkClient = getRealmAwareZkClient();
    if (!ZKUtil.isClusterSetup(clusterId, zkClient)) {
      return notFound();
    }

    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    // Here to update cloud config
    Command command;
    if (commandStr == null || commandStr.isEmpty()) {
      command = Command.update; // Default behavior
    } else {
      try {
        command = getCommand(commandStr);
      } catch (HelixException ex) {
        return badRequest(ex.getMessage());
      }
    }

    ZNRecord record;
    CloudConfig cloudConfig;
    try {
      record = toZNRecord(content);
      cloudConfig = new CloudConfig(record);
    } catch (IOException e) {
      LOG.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }
    try {
      switch (command) {
      case delete: {
        configAccessor.deleteCloudConfigFields(clusterId, cloudConfig);
      }
      break;
      case update: {
        try {
          configAccessor.updateCloudConfig(clusterId, cloudConfig);
        } catch (HelixException ex) {
          LOG.error("Error in updating a CloudConfig to cluster: " + clusterId, ex);
          return badRequest(ex.getMessage());
        } catch (Exception ex) {
          LOG.error("Cannot update CloudConfig for cluster: " + clusterId, ex);
          return serverError(ex);
        }
      }
      break;
      default:
        return badRequest("Unsupported command " + commandStr);
      }
    } catch (Exception ex) {
      LOG.error("Failed to " + command + " cloud config, cluster " + clusterId + " new config: "
          + content + ", Exception: " + ex);
      return serverError(ex);
    }
    return OK();
  }

  /**
   * Reads HISTORY ZNode from the metadata store and generates a Map object that contains the
   * pertinent history entries depending on the history type.
   * @param clusterId
   * @param historyType
   * @return
   */
  private Map<String, Object> getControllerHistory(String clusterId,
      ControllerHistory.HistoryType historyType) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    Map<String, Object> history = new HashMap<>();
    history.put(Properties.id.name(), clusterId);

    ControllerHistory historyRecord =
        dataAccessor.getProperty(dataAccessor.keyBuilder().controllerLeaderHistory());

    switch (historyType) {
      case CONTROLLER_LEADERSHIP:
        history.put(Properties.history.name(),
            historyRecord != null ? historyRecord.getHistoryList() : Collections.emptyList());
        break;
      case MAINTENANCE:
        history.put(ClusterProperties.maintenanceHistory.name(),
            historyRecord != null ? historyRecord.getMaintenanceHistoryList()
                : Collections.emptyList());
        break;
    }
    return history;
  }

  private AclRegister getAclRegister() {
    return (AclRegister) _application.getProperties().get(ContextPropertyKeys.ACL_REGISTER.name());
  }

  /**
   * Validates the changes to the cluster configuration.
   *
   * Specifically checks changes related to topology settings. If topology settings are updated,
   * ensures all instance configurations align with the new cluster configuration.
   *
   * @param clusterName Name of the cluster to validate.
   * @param configAccessor Accessor to retrieve and update cluster configurations.
   * @param newClusterConfig The new cluster configuration to validate against.
   * @param command Type of command triggering this validation (e.g., update, delete).
   */
  private void validateClusterConfigChange(String clusterName, ConfigAccessor configAccessor,
      ClusterConfig newClusterConfig, Command command) {
    ClusterConfig oldConfig = configAccessor.getClusterConfig(clusterName);
    ClusterConfig updatedConfig = configAccessor.getClusterConfig(clusterName);

    if (command == Command.delete) {
      // Since the topology related setting is only in the simple field, we don't need to validate
      // other fields.
      for (Map.Entry<String, String> entry : newClusterConfig.getRecord().getSimpleFields()
          .entrySet()) {
        updatedConfig.getRecord().getSimpleFields().remove(entry.getKey());
      }
    } else {
      updatedConfig.getRecord().update(newClusterConfig.getRecord());
    }

    // Only validate the topology related settings if the topology aware is enabled.
    if (updatedConfig.isTopologyAwareEnabled()) {
      if (updatedConfig.getTopology() == null || updatedConfig.getFaultZoneType() == null) {
        throw new IllegalArgumentException(
            "Topology and fault zone type must be set when topology aware is enabled.");
      }

      boolean isTopologyAwareChanged =
          !oldConfig.isTopologyAwareEnabled() && updatedConfig.isTopologyAwareEnabled();
      boolean isTopologyPathChanged =
          oldConfig.getTopology() == null || (oldConfig.getTopology() != null
              && !oldConfig.getTopology().equals(updatedConfig.getTopology()));
      boolean isFaultZoneTypeChanged =
          oldConfig.getFaultZoneType() == null || (oldConfig.getFaultZoneType() != null
              && !oldConfig.getFaultZoneType().equals(updatedConfig.getFaultZoneType()));

      if (isTopologyAwareChanged || isTopologyPathChanged || isFaultZoneTypeChanged) {
        HelixDataAccessor dataAccessor = getDataAccssor(clusterName);
        PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
        List<InstanceConfig> instanceConfigs = dataAccessor.getChildValues(keyBuilder.instanceConfigs(), true);
        for (InstanceConfig instanceConfig : instanceConfigs) {
          instanceConfig.validateTopologySettingInInstanceConfig(updatedConfig,
              instanceConfig.getInstanceName());
        }
      }
    }
  }
}
