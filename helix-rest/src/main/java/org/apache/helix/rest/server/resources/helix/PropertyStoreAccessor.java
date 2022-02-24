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

import java.io.IOException;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.msdcommon.util.ZkValidationUtil;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ClusterAuth
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
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("{path: .+}")
  public Response getPropertyByPath(@PathParam("clusterId") String clusterId,
      @PathParam("path") String path) {
    path = "/" + path;
    if (!ZkValidationUtil.isPathValid(path)) {
      LOG.info("The propertyStore path {} is invalid for cluster {}", path, clusterId);
      return badRequest(
          "Invalid path string. Valid path strings use slash as the directory separator and names the location of ZNode");
    }
    final String recordPath = PropertyPathBuilder.propertyStore(clusterId) + path;
    BaseDataAccessor<byte[]> propertyStoreDataAccessor = getByteArrayDataAccessor();
    if (!propertyStoreDataAccessor.exists(recordPath, AccessOption.PERSISTENT)) {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity(String.format("The property store path %s doesn't exist", recordPath))
          .build());
    }
    byte[] bytes = propertyStoreDataAccessor.get(recordPath, null, AccessOption.PERSISTENT);
    if (bytes == null) {
      throw new WebApplicationException(Response.status(Response.Status.NO_CONTENT).build());
    }
    ZNRecord znRecord = (ZNRecord) ZN_RECORD_SERIALIZER.deserialize(bytes);
    // The ZNRecordSerializer returns null when exception occurs in deserialization method
    if (znRecord == null) {
      // If the zk node cannot be deserialized, return the content directly.
      ObjectNode jsonNode = OBJECT_MAPPER.createObjectNode();
      jsonNode.put(CONTENT_KEY, new String(bytes));
      return JSONRepresentation(jsonNode);
    } else {
      return JSONRepresentation(znRecord);
    }
  }

  /**
   * Sample HTTP URLs:
   *  http://<HOST>/clusters/{clusterId}/propertyStore/<PATH>
   * It refers to the /PROPERTYSTORE/<PATH> in Helix metadata store
   * @param clusterId The cluster Id
   * @param path path parameter is like "abc/abc/abc" in the URL
   * @param isZNRecord true if the content represents a ZNRecord. false means byte array.
   * @param content
   * @return Response
   */
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @PUT
  @Path("{path: .+}")
  public Response putPropertyByPath(@PathParam("clusterId") String clusterId,
      @PathParam("path") String path,
      @QueryParam("isZNRecord") @DefaultValue("true") String isZNRecord, String content) {
    path = "/" + path;
    if (!ZkValidationUtil.isPathValid(path)) {
      LOG.info("The propertyStore path {} is invalid for cluster {}", path, clusterId);
      return badRequest(
          "Invalid path string. Valid path strings use slash as the directory separator and names the location of ZNode");
    }
    final String recordPath = PropertyPathBuilder.propertyStore(clusterId) + path;
    try {
      if (Boolean.parseBoolean(isZNRecord)) {
        try {
          ZNRecord record = toZNRecord(content);
          BaseDataAccessor<ZNRecord> propertyStoreDataAccessor =
              getDataAccssor(clusterId).getBaseDataAccessor();
          if (!propertyStoreDataAccessor.set(recordPath, record, AccessOption.PERSISTENT)) {
            return serverError(
                "Failed to set content: " + content + " in PropertyStore path: " + path);
          }
        } catch (IOException e) {
          LOG.error("Failed to deserialize content " + content + " into a ZNRecord!", e);
          return badRequest(
              "Failed to write to path: " + recordPath + "! Content is not a valid ZNRecord!");
        }
      } else {
        BaseDataAccessor<byte[]> propertyStoreDataAccessor = getByteArrayDataAccessor();
        if (!propertyStoreDataAccessor
            .set(recordPath, content.getBytes(), AccessOption.PERSISTENT)) {
          return serverError(
              "Failed to set content: " + content + " in PropertyStore path: " + path);
        }
      }
      return OK();
    } catch (Exception e) {
      return serverError(e);
    }
  }

  /**
   * Recursively deletes the PropertyStore path. If the node does not exist, it returns OK().
   * @param clusterId
   * @param path
   * @return
   */
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @DELETE
  @Path("{path: .+}")
  public Response deletePropertyByPath(@PathParam("clusterId") String clusterId,
      @PathParam("path") String path) {
    path = "/" + path;
    if (!ZkValidationUtil.isPathValid(path)) {
      LOG.info("The propertyStore path {} is invalid for cluster {}", path, clusterId);
      return badRequest(
          "Invalid path string. Valid path strings use slash as the directory separator and names the location of ZNode");
    }
    final String recordPath = PropertyPathBuilder.propertyStore(clusterId) + path;
    BaseDataAccessor<byte[]> propertyStoreDataAccessor = getByteArrayDataAccessor();
    if (!propertyStoreDataAccessor.remove(recordPath, AccessOption.PERSISTENT)) {
      return serverError("Failed to delete PropertyStore record in path: " + path);
    }
    return OK();
  }
}
