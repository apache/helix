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

import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/clusters/{clusterId}/propertyStore")
public class PropertyStoreAccessor extends AbstractHelixResource {
  private static Logger LOG = LoggerFactory.getLogger(PropertyStoreAccessor.class);
  private static final String CONTENT_KEY = "content";
  private static final ZNRecordSerializer ZN_RECORD_SERIALIZER = new ZNRecordSerializer();

  /**
   * Sample HTTP URLs:
   *  http://<HOST>/clusters/{clusterId}/propertyStore/<PATH>
   * It refers to the /PROPERTYSTORE/<PATH> in Helix metadata store
   * @param clusterId The cluster Id
   * @param path path parameter is like "abc/abc/abc" in the URL
   * @return If the payload is ZNRecord format, return ZnRecord json response;
   *         Otherwise, return json object {<PATH>: raw string}
   */
  @GET
  @Path("{path: .+}")
  public Response getPropertyByPath(@PathParam("clusterId") String clusterId,
      @PathParam("path") String path) {
    path = "/" + path;
    if (!isPathValid(path)) {
      LOG.info("The propertyStore path {} is invalid for cluster {}", path, clusterId);
      return badRequest(
          "Invalid path string. Valid path strings use slash as the directory separator and names the location of ZNode");
    }
    final String recordPath = PropertyPathBuilder.propertyStore(clusterId) + path;
    ZkBaseDataAccessor<byte[]> propertyStoreDataAccessor = getByteArrayDataAccessor();
    if (propertyStoreDataAccessor.exists(recordPath, AccessOption.PERSISTENT)) {
      byte[] bytes = propertyStoreDataAccessor.get(recordPath, null, AccessOption.PERSISTENT);
      ZNRecord znRecord = (ZNRecord) ZN_RECORD_SERIALIZER.deserialize(bytes);
      // The ZNRecordSerializer returns null when exception occurs in deserialization method
      if (znRecord == null) {
        ObjectNode jsonNode = OBJECT_MAPPER.createObjectNode();
        jsonNode.put(CONTENT_KEY, new String(bytes));
        return JSONRepresentation(jsonNode);
      }
      return JSONRepresentation(znRecord);
    } else {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity(String.format("The property store path %s doesn't exist", recordPath)).build());
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
