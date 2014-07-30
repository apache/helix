package org.apache.helix.webapp.resources;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.webapp.RestAdminApplication;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

public class ClusterResource extends ServerResource {

  public ClusterResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      presentation = getClusterRepresentation(clusterName);
    }

    catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      e.printStackTrace();
    }
    return presentation;
  }

  StringRepresentation getClusterRepresentation(String clusterName) throws JsonGenerationException,
      JsonMappingException, IOException {
    ZkClient zkClient = (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
    ClusterSetup setupTool = new ClusterSetup(zkClient);
    List<String> instances =
        setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);

    ZNRecord clusterSummayRecord = new ZNRecord("Cluster Summary");
    clusterSummayRecord.setListField("participants", instances);

    List<String> resources =
        setupTool.getClusterManagementTool().getResourcesInCluster(clusterName);
    clusterSummayRecord.setListField("resources", resources);

    List<String> models = setupTool.getClusterManagementTool().getStateModelDefs(clusterName);
    clusterSummayRecord.setListField("stateModelDefs", models);

    HelixDataAccessor accessor =
        ClusterRepresentationUtil.getClusterDataAccessor(zkClient, clusterName);
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader != null) {
      clusterSummayRecord.setSimpleField("LEADER", leader.getInstanceName());
    } else {
      clusterSummayRecord.setSimpleField("LEADER", "");
    }
    StringRepresentation representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(clusterSummayRecord),
            MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation post(Representation entity) {
    try {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkClient);

      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();

      if (command == null) {
        throw new HelixException("Could NOT find 'command' in parameterMap: "
            + jsonParameters._parameterMap);
      } else if (command.equalsIgnoreCase(ClusterSetup.activateCluster)
          || JsonParameters.CLUSTERSETUP_COMMAND_ALIASES.get(ClusterSetup.activateCluster)
              .contains(command)) {
        jsonParameters.verifyCommand(ClusterSetup.activateCluster);

        boolean enabled = true;
        if (jsonParameters.getParameter(JsonParameters.ENABLED) != null) {
          enabled = Boolean.parseBoolean(jsonParameters.getParameter(JsonParameters.ENABLED));
        }

        String grandCluster = jsonParameters.getParameter(JsonParameters.GRAND_CLUSTER);

        setupTool.activateCluster(clusterName, grandCluster, enabled);
      } else if (command.equalsIgnoreCase(ClusterSetup.expandCluster)) {
        setupTool.expandCluster(clusterName);
      } else {
        throw new HelixException("Unsupported command: " + command + ". Should be one of ["
            + ClusterSetup.activateCluster + ", " + ClusterSetup.expandCluster + "]");
      }
      getResponse().setEntity(getClusterRepresentation(clusterName));
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    return getResponseEntity();
  }

  @Override
  public Representation delete() {
    try {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkClient);
      setupTool.deleteCluster(clusterName);
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    return null;
  }
}
