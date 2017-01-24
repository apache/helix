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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import javax.ws.rs.core.Response;

@Path("/clusters")
public class Clusters {
  private static Logger _logger = Logger.getLogger(Clusters.class.getName());

  @GET
  @Produces("text/json")
  public Response getClusters() {
    Response r;

    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, List<String>> configMap = new HashMap<String, List<String>>();
    configMap.put("clusters", Collections.<String>emptyList());
    try {
      ObjectWriter objectWriter = objectMapper.writer();
      String jsonStr = objectWriter.writeValueAsString(configMap);
      r = Response.ok(jsonStr).build();
    } catch (IOException e) {
      _logger.error("Failed to convert map to JSON response", e);
      r = Response.serverError().build();
    }

    return r;
  }
}
