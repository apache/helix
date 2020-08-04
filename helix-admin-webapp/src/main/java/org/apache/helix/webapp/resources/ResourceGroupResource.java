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
import java.util.Arrays;

import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.tools.ClusterSetup;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceGroupResource extends ServerResource {
  private final static Logger LOG = LoggerFactory.getLogger(ResourceGroupResource.class);

  public ResourceGroupResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      String resourceName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.RESOURCE_NAME);
      presentation = getIdealStateRepresentation(clusterName, resourceName);
    }

    catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("Exception in get resource group", e);
    }
    return presentation;
  }

  StringRepresentation getIdealStateRepresentation(String clusterName, String resourceName)
      throws JsonGenerationException, JsonMappingException, IOException {
    Builder keyBuilder = new PropertyKey.Builder(clusterName);

    ZkClient zkclient =
        ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.RAW_ZKCLIENT);
    String idealStateStr =
        ResourceUtil.readZkAsBytes(zkclient, keyBuilder.idealStates(resourceName));

    StringRepresentation representation =
        new StringRepresentation(idealStateStr, MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation delete() {
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      String resourceName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.RESOURCE_NAME);
      ZkClient zkclient =
          ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);

      ClusterSetup setupTool = new ClusterSetup(zkclient);
      setupTool.dropResourceFromCluster(clusterName, resourceName);
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("", e);
    }
    return null;
  }

  @Override
  public Representation post(Representation entity) {
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      String resourceName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.RESOURCE_NAME);
      ZkClient zkclient =
          ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkclient);

      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();
      if (command.equalsIgnoreCase(ClusterSetup.resetResource)) {

        setupTool.getClusterManagementTool()
            .resetResource(clusterName, Arrays.asList(resourceName));
      } else if (command.equalsIgnoreCase(ClusterSetup.enableResource)) {
        jsonParameters.verifyCommand(ClusterSetup.enableResource);
        boolean enabled = Boolean.parseBoolean(jsonParameters.getParameter(JsonParameters.ENABLED));
        setupTool.getClusterManagementTool().enableResource(clusterName, resourceName, enabled);
      } else {
        throw new HelixException("Unsupported command: " + command + ". Should be one of ["
            + ClusterSetup.resetResource + "]");
      }
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("", e);
    }
    return null;
  }

}
