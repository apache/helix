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

import org.apache.helix.HelixException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
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

/**
 * Class for server-side resource at <code> "/clusters"
 * <p>
 * <li>GET list all Helix clusters
 * <li>POST add a new cluster
 */
public class ClustersResource extends ServerResource {
  private final static Logger LOG = LoggerFactory.getLogger(ClustersResource.class);

  public ClustersResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  /**
   * List all Helix clusters
   * <p>
   * Usage: <code> curl http://{host:port}/clusters
   */
  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      presentation = getClustersRepresentation();
    } catch (Exception e) {
      LOG.error("Exception in get all clusters", e);
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
    }
    return presentation;
  }

  StringRepresentation getClustersRepresentation() throws JsonGenerationException,
      JsonMappingException, IOException {
    ZkClient zkClient =
        ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);

    ClusterSetup setupTool = new ClusterSetup(zkClient);
    List<String> clusters = setupTool.getClusterManagementTool().getClusters();

    ZNRecord clustersRecord = new ZNRecord("Clusters Summary");
    clustersRecord.setListField("clusters", clusters);
    StringRepresentation representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(clustersRecord),
            MediaType.APPLICATION_JSON);

    return representation;
  }

  /**
   * Add a new Helix cluster
   * <p>
   * Usage: <code> curl -d 'jsonParameters={"command":"addCluster","clusterName":"{clusterName}"}' -H
   * "Content-Type: application/json" http://{host:port}/clusters
   */
  @Override
  public Representation post(Representation entity) {
    try {
      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();

      if (command == null) {
        throw new HelixException("Could NOT find 'command' in parameterMap: "
            + jsonParameters._parameterMap);
      } else if (command.equalsIgnoreCase(ClusterSetup.addCluster)) {
        jsonParameters.verifyCommand(ClusterSetup.addCluster);

        ZkClient zkClient =
            ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
        ClusterSetup setupTool = new ClusterSetup(zkClient);
        setupTool.addCluster(jsonParameters.getParameter(JsonParameters.CLUSTER_NAME), false);
      } else {
        throw new HelixException("Unsupported command: " + command + ". Should be one of ["
            + ClusterSetup.addCluster + "]");
      }

      getResponse().setEntity(getClustersRepresentation());
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Error in posting " + entity, e);
    }
    return null;
  }

  @Override
  public Representation delete() {
    return null;
  }
}
