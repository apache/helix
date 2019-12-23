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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/clusters")
public class CloudAccessor extends AbstractHelixResource {
  private static Logger _logger = LoggerFactory.getLogger(CloudAccessor.class.getName());

  @PUT
  @Path("{clusterId}/cloudconfig")
  public Response addCloudConfig(@PathParam("clusterId") String clusterId, String content) {

    HelixZkClient zkClient = getHelixZkClient();
    if (!ZKUtil.isClusterSetup(clusterId, zkClient)) {
      return notFound();
    }

    HelixAdmin admin = getHelixAdmin();
    ZNRecord record;
    try {
      record = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a vaild ZNRecord!");
    }

    try {
      CloudConfig cloudConfig = new CloudConfig.Builder(record).build();
      admin.addCloudConfig(clusterId, cloudConfig);
    } catch (Exception ex) {
      _logger.error("Error in adding a CloudConfig to cluster: " + clusterId, ex);
      return badRequest(ex.getMessage());
    }

    return OK();
  }

  @GET
  @Path("{clusterId}/cloudconfig")
  public Response getCloudConfig(@PathParam("clusterId") String clusterId) {

    HelixZkClient zkClient = getHelixZkClient();
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

  @POST
  @Path("{clusterId}/cloudconfig")
  public Response updateCloudConfig(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String commandStr, String content) {

    HelixZkClient zkClient = getHelixZkClient();
    if (!ZKUtil.isClusterSetup(clusterId, zkClient)) {
      return notFound();
    }

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

    HelixAdmin admin = getHelixAdmin();
    try {
      switch (command) {
        case delete: {
          admin.removeCloudConfig(clusterId);
        }
        break;
        case update: {
          ZNRecord record;
          try {
            record = toZNRecord(content);
          } catch (IOException e) {
            _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
            return badRequest("Input is not a vaild ZNRecord!");
          }
          try {
            CloudConfig cloudConfig = new CloudConfig.Builder(record).build();
            admin.removeCloudConfig(clusterId);
            admin.addCloudConfig(clusterId, cloudConfig);
          } catch (Exception ex) {
            _logger.error("Error in updating a CloudConfig to cluster: " + clusterId, ex);
            return badRequest(ex.getMessage());
          }
        }
        break;
        default:
          return badRequest("Unsupported command " + commandStr);
      }
    } catch (Exception ex) {
      _logger.error("Failed to " + command + " cloud config, cluster " + clusterId + " new config: "
          + content + ", Exception: " + ex);
      return serverError(ex);
    }
    return OK();
  }
}
