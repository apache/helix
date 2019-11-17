package org.apache.helix.rest.server.resources.helix.waged;

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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.rest.server.resources.helix.AbstractHelixResource;
import org.apache.helix.rest.server.resources.helix.ResourceAccessor;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/clusters/{clusterId}/waged/resources")
public class WagedResourceAccessor extends AbstractHelixResource {
  private final static Logger LOG = LoggerFactory.getLogger(ResourceAccessor.class);

  @PUT
  @Path("{resourceName}")
  public Response addResource(@PathParam("clusterId") String clusterId,
      @PathParam("resourceName") String resourceName, String content) {
    HelixAdmin admin = getHelixAdmin();

    if (content == null || content.length() == 0) {
      LOG.error("Input is null or empty!");
      return badRequest("Input is null or empty!");
    }
    Map<String, ZNRecord> input;
    // Content must supply both IdealState and ResourceConfig
    try {
      TypeReference<Map<String, ZNRecord>> typeRef = new TypeReference<Map<String, ZNRecord>>() {
      };
      input = OBJECT_MAPPER.readValue(content, typeRef);
    } catch (IOException e) {
      LOG.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest("Input is not a valid map of String-ZNRecord pairs!");
    }

    // Check if the map contains both IdealState and ResourceConfig
    ZNRecord idealStateRecord = input.get(ResourceAccessor.ResourceProperties.idealState.name());
    ZNRecord resourceConfigRecord =
        input.get(ResourceAccessor.ResourceProperties.resourceConfig.name());

    if (idealStateRecord == null || resourceConfigRecord == null) {
      LOG.error("Input does not contain both IdealState and ResourceConfig!");
      return badRequest("Input does not contain both IdealState and ResourceConfig!");
    }

    try {
      admin.addResourceWithWeight(clusterId, new IdealState(idealStateRecord),
          new ResourceConfig(resourceConfigRecord));
    } catch (HelixException e) {
      String errMsg = String
          .format("Failed to add resource %s with weight in cluster %s!", idealStateRecord.getId(),
              clusterId);
      LOG.error(errMsg, e);
      return badRequest(errMsg);
    }

    return OK();
  }
}

