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
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
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

/**
 * Class for server-side resource at
 * <code>"/clusters/{clusterName}/resourceGroups/{resourceName}/idealState"
 * <p>
 * <li>GET get ideal state
 * <li>POST set ideal state
 */
public class IdealStateResource extends ServerResource {
  private final static Logger LOG = LoggerFactory.getLogger(IdealStateResource.class);

  public IdealStateResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  /**
   * Get ideal state
   * <p>
   * Usage:
   * <code>curl http://{host:port}/clusters/{clusterName}/resourceGroups/{resourceName}/idealState
   */
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

      LOG.error("Exception in get idealState", e);
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

  /**
   * Set ideal state
   * <p>
   * Usage:
   * <p>
   * <li>Add ideal state:
   * <code>curl -d @'{newIdealState.json}' -H 'Content-Type: application/json'
   * http://{host:port}/clusters/{cluster}/resourceGroups/{resource}/idealState
   * <pre>
   * newIdealState:
   * jsonParameters={"command":"addIdealState"}&newIdealState={
   *  "id" : "{MyDB}",
   *  "simpleFields" : {
   *    "IDEAL_STATE_MODE" : "AUTO",
   *    "NUM_PARTITIONS" : "{8}",
   *    "REBALANCE_MODE" : "SEMI_AUTO",
   *    "REPLICAS" : "0",
   *    "STATE_MODEL_DEF_REF" : "MasterSlave",
   *    "STATE_MODEL_FACTORY_NAME" : "DEFAULT"
   *  },
   *  "listFields" : {
   *  },
   *  "mapFields" : {
   *    "{MyDB_0}" : {
   *      "{localhost_1001}" : "MASTER",
   *      "{localhost_1002}" : "SLAVE"
   *    }
   *  }
   * }
   * </pre>
   * <li>Rebalance cluster:
   * <code>curl -d 'jsonParameters={"command":"rebalance","replicas":"{3}"}'
   * -H "Content-Type: application/json" http://{host:port}/clusters/{cluster}/resourceGroups/{resource}/idealState
   * <li>Expand resource: <code>n/a
   * <li>Add resource property:
   * <code>curl -d 'jsonParameters={"command":"addResourceProperty","{REBALANCE_TIMER_PERIOD}":"{500}"}'
   * -H "Content-Type: application/json" http://{host:port}/clusters/{cluster}/resourceGroups/{resource}/idealState
   */
  @Override
  public Representation post(Representation entity) {
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      String resourceName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.RESOURCE_NAME);

      ZkClient zkClient =
          ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkClient);

      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();

      if (command.equalsIgnoreCase(ClusterSetup.addIdealState)) {
        ZNRecord newIdealState = jsonParameters.getExtraParameter(JsonParameters.NEW_IDEAL_STATE);
        HelixDataAccessor accessor =
            ClusterRepresentationUtil.getClusterDataAccessor(zkClient, clusterName);

        accessor.setProperty(accessor.keyBuilder().idealStates(resourceName), new IdealState(
            newIdealState));

      } else if (command.equalsIgnoreCase(ClusterSetup.rebalance)) {
        int replicas = Integer.parseInt(jsonParameters.getParameter(JsonParameters.REPLICAS));
        String keyPrefix = jsonParameters.getParameter(JsonParameters.RESOURCE_KEY_PREFIX);
        String groupTag = jsonParameters.getParameter(ClusterSetup.instanceGroupTag);

        setupTool.rebalanceCluster(clusterName, resourceName, replicas, keyPrefix, groupTag);

      } else if (command.equalsIgnoreCase(ClusterSetup.expandResource)) {
        setupTool.expandResource(clusterName, resourceName);
      } else if (command.equalsIgnoreCase(ClusterSetup.addResourceProperty)) {
        Map<String, String> parameterMap = jsonParameters.cloneParameterMap();
        parameterMap.remove(JsonParameters.MANAGEMENT_COMMAND);
        for (String key : parameterMap.keySet()) {
          setupTool.addResourceProperty(clusterName, resourceName, key, parameterMap.get(key));
        }
      } else {
        throw new HelixException("Unsupported command: " + command + ". Should be one of ["
            + ClusterSetup.addIdealState + ", " + ClusterSetup.rebalance + ", "
            + ClusterSetup.expandResource + ", " + ClusterSetup.addResourceProperty + "]");
      }

      getResponse().setEntity(getIdealStateRepresentation(clusterName, resourceName));
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Error in posting " + entity, e);
    }
    return null;
  }
}
