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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ClusterConfig;
import org.apache.log4j.Logger;
import javax.ws.rs.core.Response;

@Path("/clusters")
public class ClusterAccessor extends AbstractResource {
  private static Logger _logger = Logger.getLogger(ClusterAccessor.class.getName());

  @GET
  @Produces({"application/json", "text/plain"})
  public Response getClusters() {
    Response r;

    HelixAdmin helixAdmin = getHelixAdmin();
    List<String> clusters = helixAdmin.getClusters();

    Map<String, List<String>> dataMap = new HashMap<>();
    dataMap.put("clusters", clusters);
    try {
      String jsonStr = toJson(dataMap);
      r = Response.ok(jsonStr).build();
    } catch (IOException e) {
      _logger.error("Failed to convert map to JSON response", e);
      r = Response.serverError().build();
    }

    return r;
  }

  @GET
  @Path("{clusterId}/configs")
  @Produces({"application/json", "text/plain"})
  public Response getClusterConfig(@PathParam("clusterId") String clusterId) {
    ConfigAccessor accessor = getConfigAccessor();
    ClusterConfig config = accessor.getClusterConfig(clusterId);
    if (config == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    try {
      String jsonStr = toJson(config.getRecord());
      return Response.ok(jsonStr).build();
    } catch (IOException e) {
      _logger.error("Failed to convert ClusterConfig to JSON response", e);
      return Response.serverError().build();
    }
  }
}
