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
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.webapp.RestAdminApplication;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ResourceGroupsResource extends ServerResource {
  private final static Logger LOG = Logger.getLogger(ResourceGroupsResource.class);

  public ResourceGroupsResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      presentation = getHostedEntitiesRepresentation(clusterName);
    }

    catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("", e);
    }
    return presentation;
  }

  StringRepresentation getHostedEntitiesRepresentation(String clusterName)
      throws JsonGenerationException, JsonMappingException, IOException {
    // Get all resources
    ZkClient zkClient = (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
    HelixDataAccessor accessor =
        ClusterRepresentationUtil.getClusterDataAccessor(zkClient, clusterName);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Map<String, IdealState> idealStateMap = accessor.getChildValuesMap(keyBuilder.idealStates());

    // Create the result
    ZNRecord hostedEntitiesRecord = new ZNRecord("ResourceGroups");

    // Figure out which tags are present on which resources
    Map<String, String> tagMap = Maps.newHashMap();
    for (IdealState idealState : idealStateMap.values()) {
      String tag = idealState.getInstanceGroupTag();
      if (tag != null) {
        tagMap.put(idealState.getId(), tag);
      }
    }

    // Populate the result
    List<String> allResources = Lists.newArrayList(idealStateMap.keySet());
    hostedEntitiesRecord.setListField("ResourceGroups", allResources);
    if (!tagMap.isEmpty()) {
      hostedEntitiesRecord.setMapField("ResourceTags", tagMap);
    }

    StringRepresentation representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(hostedEntitiesRecord),
            MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation post(Representation entity) {
    try {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");

      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();

      if (command.equalsIgnoreCase(ClusterSetup.addResource)
          || JsonParameters.CLUSTERSETUP_COMMAND_ALIASES.get(ClusterSetup.addResource).contains(
              command)) {
        jsonParameters.verifyCommand(ClusterSetup.addResource);

        String entityName = jsonParameters.getParameter(JsonParameters.RESOURCE_GROUP_NAME);
        String stateModelDefRef = jsonParameters.getParameter(JsonParameters.STATE_MODEL_DEF_REF);
        int partitions = Integer.parseInt(jsonParameters.getParameter(JsonParameters.PARTITIONS));
        String mode = RebalanceMode.SEMI_AUTO.toString();
        if (jsonParameters.getParameter(JsonParameters.IDEAL_STATE_MODE) != null) {
          mode = jsonParameters.getParameter(JsonParameters.IDEAL_STATE_MODE);
        }

        int bucketSize = 0;
        if (jsonParameters.getParameter(JsonParameters.BUCKET_SIZE) != null) {
          try {
            bucketSize = Integer.parseInt(jsonParameters.getParameter(JsonParameters.BUCKET_SIZE));
          } catch (Exception e) {

          }
        }

        int maxPartitionsPerNode = -1;
        if (jsonParameters.getParameter(JsonParameters.MAX_PARTITIONS_PER_NODE) != null) {
          try {
            maxPartitionsPerNode =
                Integer.parseInt(jsonParameters
                    .getParameter(JsonParameters.MAX_PARTITIONS_PER_NODE));
          } catch (Exception e) {

          }
        }

        ZkClient zkClient =
            (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
        ;
        ClusterSetup setupTool = new ClusterSetup(zkClient);
        setupTool.addResourceToCluster(clusterName, entityName, partitions, stateModelDefRef, mode,
            bucketSize, maxPartitionsPerNode);
      } else {
        throw new HelixException("Unsupported command: " + command + ". Should be one of ["
            + ClusterSetup.addResource + "]");

      }

      getResponse().setEntity(getHostedEntitiesRepresentation(clusterName));
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Error in posting " + entity, e);
    }
    return null;
  }
}
