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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.swing.text.AbstractDocument;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.LeaderHistory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

@Path("/clusters")
@Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
@Consumes({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
public class ClusterAccessor extends AbstractResource {
  private static Logger _logger = Logger.getLogger(ClusterAccessor.class.getName());

  public enum ClusterProperties {
    controller,
    instances,
    liveInstances,
    resources,
    paused,
    messages,
    stateModelDefinitions,
    clusters
  }

  @GET
  public Response getClusters() {
    HelixAdmin helixAdmin = getHelixAdmin();
    List<String> clusters = helixAdmin.getClusters();

    Map<String, List<String>> dataMap = new HashMap<>();
    dataMap.put(ClusterProperties.clusters.name(), clusters);

    return JSONRepresentation(dataMap);
  }

  @GET
  @Path("{clusterId}")
  public Response getClusterInfo(@PathParam("clusterId") String clusterId) {
    if (!isClusterExist(clusterId)) {
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

    boolean paused = (dataAccessor.getProperty(keyBuilder.pause()) == null ? false : true);
    clusterInfo.put(ClusterProperties.paused.name(), paused);

    List<String> idealStates = dataAccessor.getChildNames(keyBuilder.idealStates());
    clusterInfo.put(ClusterProperties.resources.name(), idealStates);
    List<String> instances = dataAccessor.getChildNames(keyBuilder.instanceConfigs());
    clusterInfo.put(ClusterProperties.instances.name(), instances);
    List<String> liveInstances = dataAccessor.getChildNames(keyBuilder.liveInstances());
    clusterInfo.put(ClusterProperties.liveInstances.name(), liveInstances);

    return JSONRepresentation(clusterInfo);
  }

  @GET
  @Path("{clusterId}/configs")
  public Response getClusterConfig(@PathParam("clusterId") String clusterId) {
    ConfigAccessor accessor = getConfigAccessor();
    ClusterConfig config = null;
    try {
      config = accessor.getClusterConfig(clusterId);
    } catch (HelixException ex) {
      // cluster not found.
      _logger.info("Failed to get cluster config for cluster " + clusterId
          + ", cluster not found, Exception: " + ex);
    } catch (Exception ex) {
      _logger.error("Failed to get cluster config for cluster " + clusterId + " Exception: " + ex);
      String errorMsg = String
          .format("Failed to get cluster config for cluster %s, Exception: %s", clusterId,
              ex.getMessage());
      return serverError(errorMsg);
    }
    if (config == null) {
      return notFound();
    }
    return JSONRepresentation(config.getRecord());
  }

  @POST
  @Path("{clusterId}/configs")
  public Response updateClusterConfig(@PathParam("clusterId") String clusterId, String content) {
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }
    ClusterConfig config = new ClusterConfig(record);
    ConfigAccessor configAccessor = getConfigAccessor();
    try {
      configAccessor.updateClusterConfig(clusterId, config);
    } catch (HelixException ex) {
      return notFound(ex.getMessage());
    } catch (Exception ex) {
      _logger.error(
          "Failed to update cluster config, cluster " + clusterId + " new config: " + content
              + ", Exception: " + ex);
      return serverError(ex.getMessage());
    }
    return OK();
  }

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

  @GET
  @Path("{clusterId}/controller/history")
  public Response getClusterControllerHistory(@PathParam("clusterId") String clusterId) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    Map<String, Object> controllerHistory = new HashMap<>();
    controllerHistory.put(Properties.id.name(), clusterId);

    LeaderHistory history =
        dataAccessor.getProperty(dataAccessor.keyBuilder().controllerLeaderHistory());
    if (history != null) {
      controllerHistory.put(Properties.history.name(), history.getHistoryList());
    } else {
      controllerHistory.put(Properties.history.name(), Collections.emptyList());
    }

    return JSONRepresentation(controllerHistory);
  }

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

  @GET
  @Path("{clusterId}/controller/messages/{messageId}")
  public Response getClusterControllerMessages(@PathParam("clusterId") String clusterId, @PathParam("messageId") String messageId) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    Message message =
        dataAccessor.getProperty(dataAccessor.keyBuilder().controllerMessage(messageId));
    return JSONRepresentation(message.getRecord());
  }

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

  @GET
  @Path("{clusterId}/statemodeldefs/{statemodel}")
  public Response getClusterStateModelDefinition(@PathParam("clusterId") String clusterId,
      @PathParam("statemodel") String statemodel) {
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    StateModelDefinition stateModelDef =
        dataAccessor.getProperty(dataAccessor.keyBuilder().stateModelDef(statemodel));

    return JSONRepresentation(stateModelDef.getRecord());
  }

  private boolean isClusterExist(String cluster) {
    ZkClient zkClient = getZkClient();
    if (ZKUtil.isClusterSetup(cluster, zkClient)) {
      return true;
    }
    return false;
  }
}
