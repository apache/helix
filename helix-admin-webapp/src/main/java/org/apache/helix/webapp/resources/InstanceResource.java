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
import java.util.Arrays;

import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.tools.ClusterSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

public class InstanceResource extends ServerResource {
  private final static Logger LOG = LoggerFactory.getLogger(InstanceResource.class);

  public InstanceResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      presentation = getInstanceRepresentation();
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("Exception in get instance", e);
    }
    return presentation;
  }

  StringRepresentation getInstanceRepresentation() throws JsonGenerationException,
      JsonMappingException, IOException {
    String clusterName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
    String instanceName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.INSTANCE_NAME);
    Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkClient zkclient =
        ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.RAW_ZKCLIENT);

    String instanceCfgStr =
        ResourceUtil.readZkAsBytes(zkclient, keyBuilder.instanceConfig(instanceName));
    StringRepresentation representation =
        new StringRepresentation(instanceCfgStr, MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation post(Representation entity) {
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      String instanceName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.INSTANCE_NAME);

      ZkClient zkclient =
          ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkclient);

      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();
      if (command.equalsIgnoreCase(ClusterSetup.enableInstance)) {
        jsonParameters.verifyCommand(ClusterSetup.enableInstance);

        boolean enabled = Boolean.parseBoolean(jsonParameters.getParameter(JsonParameters.ENABLED));

        setupTool.getClusterManagementTool().enableInstance(clusterName, instanceName, enabled);
      } else if (command.equalsIgnoreCase(ClusterSetup.enablePartition)) {
        jsonParameters.verifyCommand(ClusterSetup.enablePartition);

        boolean enabled = Boolean.parseBoolean(jsonParameters.getParameter(JsonParameters.ENABLED));

        String[] partitions = jsonParameters.getParameter(JsonParameters.PARTITION).split(";");
        String resource = jsonParameters.getParameter(JsonParameters.RESOURCE);

        setupTool.getClusterManagementTool().enablePartition(enabled, clusterName, instanceName,
            resource, Arrays.asList(partitions));
      } else if (command.equalsIgnoreCase(ClusterSetup.resetPartition)) {
        jsonParameters.verifyCommand(ClusterSetup.resetPartition);

        String resource = jsonParameters.getParameter(JsonParameters.RESOURCE);

        String[] partitionNames =
            jsonParameters.getParameter(JsonParameters.PARTITION).split("\\s+");
        setupTool.getClusterManagementTool().resetPartition(clusterName, instanceName, resource,
            Arrays.asList(partitionNames));
      } else if (command.equalsIgnoreCase(ClusterSetup.resetInstance)) {
        jsonParameters.verifyCommand(ClusterSetup.resetInstance);

        setupTool.getClusterManagementTool()
            .resetInstance(clusterName, Arrays.asList(instanceName));
      } else if (command.equalsIgnoreCase(ClusterSetup.addInstanceTag)) {
        jsonParameters.verifyCommand(ClusterSetup.addInstanceTag);
        String tag = jsonParameters.getParameter(ClusterSetup.instanceGroupTag);
        setupTool.getClusterManagementTool().addInstanceTag(clusterName, instanceName, tag);
      } else if (command.equalsIgnoreCase(ClusterSetup.removeInstanceTag)) {
        jsonParameters.verifyCommand(ClusterSetup.removeInstanceTag);
        String tag = jsonParameters.getParameter(ClusterSetup.instanceGroupTag);
        setupTool.getClusterManagementTool().removeInstanceTag(clusterName, instanceName, tag);
      } else {
        throw new HelixException("Unsupported command: " + command + ". Should be one of ["
            + ClusterSetup.enableInstance + ", " + ClusterSetup.enablePartition + ", "
            + ClusterSetup.resetInstance + "]");
      }

      getResponse().setEntity(getInstanceRepresentation());
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Exception in post instance", e);
    }
    return null;
  }

  @Override
  public Representation delete() {
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      String instanceName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.INSTANCE_NAME);
      ZkClient zkclient =
          ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkclient);
      setupTool.dropInstanceFromCluster(clusterName, instanceName);
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Error in delete instance", e);
    }
    return null;
  }
}
