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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.rest.server.service.PropertyStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/clusters/{clusterId}/propertyStore")
public class PropertyStoreAccessor extends AbstractHelixResource {
  private static Logger LOG = LoggerFactory.getLogger(PropertyStoreAccessor.class);

  /**
   * Sample HTTP URLs:
   *  http://<HOST>/clusters/{clusterId}/propertyStore/<PATH>
   * It refers to the /PROPERTYSTORE/<PATH> in Helix metadata store
   * @param clusterId The cluster Id
   * @param path path parameter is like "abc/abc/abc" in the URL
   * @return JSON object as the response
   */
  @GET
  @Path("{path: .+}")
  public Response getPropertyByPath(@PathParam("clusterId") String clusterId,
      @PathParam("path") String path) {
    path = "/" + path;
    if (!isPathValid(path)) {
      LOG.error("The propertyStore path {} is invalid for cluster {}", path, clusterId);
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(
          "Invalid path string. Valid path strings use slash as the directory separator and names the location of ZNode")
          .build());
    }
    String propertyStoreRootPath = PropertyPathBuilder.propertyStore(clusterId);
    PropertyStoreService propertyStoreService = new PropertyStoreService(getHelixZkClient());
    try {
      ZNRecord record = propertyStoreService.readRecord(propertyStoreRootPath + path);
      return JSONRepresentation(record);
    } catch (ZkNoNodeException e) {
      String errorMessage = "The node doesn't exist for propertyStore path: " + path;
      LOG.error(errorMessage);
      throw new WebApplicationException(
          Response.status(Response.Status.NOT_FOUND).entity(errorMessage).build());
    }
  }

  /**
   * Valid matches:
   * /
   * /abc
   * /abc/abc/abc/abc
   * Invalid matches:
   * null or empty string
   * /abc/
   * /abc/abc/abc/abc/
   **/
  private static boolean isPathValid(String path) {
    return path.matches("^/|(/[\\w-]+)+$");
  }
}
