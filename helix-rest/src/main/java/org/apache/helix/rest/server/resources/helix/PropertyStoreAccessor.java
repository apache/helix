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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.DataFormatException;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.msdcommon.util.ZkValidationUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.util.ZLibCompressionUtil;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/clusters/{clusterId}/propertyStore{path: /.+}")
public class PropertyStoreAccessor extends AbstractHelixResource {
  private static Logger LOG = LoggerFactory.getLogger(PropertyStoreAccessor.class);

  private static final String CONTENT_KEY = "content";
  private static final String GZIP_COMPRESSION = "gzip";
  private static final String ZLIB_COMPRESSION = "zlib";
  private static final String DEFLATER_COMPRESSION = "deflater";
  private static final String BASE64_ENCODE = "base64";
  private static final ZNRecordSerializer ZN_RECORD_SERIALIZER = new ZNRecordSerializer();

  /**
   * Sample HTTP URLs:
   *  http://<HOST>/clusters/{clusterId}/propertyStore/<PATH>
   * It refers to the /PROPERTYSTORE/<PATH> in Helix metadata store
   * <p>
   * If the znode content is compressed, compression method could be specified to
   * decompress the content before returning the response:
   * "/propertyStore/path?compression=zlib"
   * <p>
   * Encoding znode content in response could be achieved by explicitly specifying
   * encode query param:
   * "/propertyStore/path?encode=base64"
   * So the content will be encoded to base64 and client needs to decode the content.
   * If compression method is not supported, a client should specify base64 to encode
   * compressed bytes so client could decode and decompress data.
   *
   * @param clusterId The cluster Id
   * @param path path parameter is like "/abc/abc/abc" in the URL
   * @param compression method that compresses znode content, eg. gzip, zlib, deflater.
   * @param encode encoding method that encodes znode content byte array in response
   *               so the client could decode the content accordingly. base64 is supported.
   *               It is useful when compression method is not supported, we could encode the
   *               bytes to base64.
   * @return If the payload is ZNRecord format, return ZnRecord json response;
   *         Otherwise, return json object {{@link #CONTENT_KEY}: raw string}
   */
  @GET
  public Response getPropertyByPath(@PathParam("clusterId") String clusterId,
      @PathParam("path") String path, @QueryParam("compression") String compression,
      @QueryParam("encode") String encode) {
    if (!ZkValidationUtil.isPathValid(path)) {
      LOG.info("The propertyStore path {} is invalid for cluster {}", path, clusterId);
      return badRequest(
          "Invalid path string. Valid path strings use slash as the directory separator and names the location of ZNode");
    }
    final String recordPath = PropertyPathBuilder.propertyStore(clusterId) + path;
    byte[] bytes = readPropertyStoreBytes(recordPath);

    if (compression != null && !compression.isEmpty()) {
      return decompressBytes(bytes, recordPath, compression);
    }

    if (encode != null && !encode.isEmpty()) {
      return encodeBytes(bytes, encode);
    }

    ZNRecord znRecord = (ZNRecord) ZN_RECORD_SERIALIZER.deserialize(bytes);
    // The ZNRecordSerializer returns null when exception occurs in deserialization method
    if (znRecord == null) {
      ObjectNode jsonNode = OBJECT_MAPPER.createObjectNode();
      // By default, treat the bytes as plain string bytes and convert the bytes to string.
      // If bytes are compressed, converting to string would cause client not able to decompress.
      jsonNode.put(CONTENT_KEY, new String(bytes));
      return JSONRepresentation(jsonNode);
    }

    return JSONRepresentation(znRecord);
  }

  /**
   * Sample HTTP URLs:
   *  http://<HOST>/clusters/{clusterId}/propertyStore/<PATH>
   * It refers to the /PROPERTYSTORE/<PATH> in Helix metadata store
   * @param clusterId The cluster Id
   * @param path path parameter is like "/abc/abc/abc" in the URL
   * @param isZNRecord true if the content represents a ZNRecord. false means byte array.
   * @param content
   * @return Response
   */
  @PUT
  public Response putPropertyByPath(@PathParam("clusterId") String clusterId,
      @PathParam("path") String path,
      @QueryParam("isZNRecord") @DefaultValue("true") String isZNRecord, String content) {
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
  @DELETE
  public Response deletePropertyByPath(@PathParam("clusterId") String clusterId,
      @PathParam("path") String path) {
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

  private byte[] readPropertyStoreBytes(String recordPath) {
    BaseDataAccessor<byte[]> propertyStoreDataAccessor = getByteArrayDataAccessor();

    // If znode does not exist, bytes is null. But content of an existing znode could be null.
    // We use stat ctime to check if znode exists or not. ctime == 0L means znode does not exist.
    Stat stat = new Stat();
    byte[] bytes = propertyStoreDataAccessor.get(recordPath, stat, AccessOption.PERSISTENT);
    if (stat.getCtime() == 0L) {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity(String.format("The property store path %s doesn't exist", recordPath)).build());
    }

    return bytes;
  }

  private Response decompressBytes(byte[] bytes, String recordPath, String compression) {
    byte[] decompressedBytes;
    try {
      switch (compression) {
        case ZLIB_COMPRESSION:
        case DEFLATER_COMPRESSION:
          decompressedBytes = ZLibCompressionUtil.decompress(bytes);
          break;
        case GZIP_COMPRESSION:
          decompressedBytes = GZipCompressionUtil.uncompress(new ByteArrayInputStream(bytes));
          break;
        default:
          LOG.info("{} compression method is not supported.", compression);
          return badRequest(compression + " compression method is not supported.");
      }
    } catch (IOException | DataFormatException e) {
      LOG.info("Failed to decompress znode bytes using {} for path: {}", compression, recordPath,
          e);
      return badRequest("Failed to decompress znode bytes for path: " + recordPath
          + ", caused by: " + e.getMessage());
    }

    ObjectNode jsonNode = OBJECT_MAPPER.createObjectNode();
    jsonNode.put(CONTENT_KEY, new String(decompressedBytes));

    return JSONRepresentation(jsonNode);
  }

  private Response encodeBytes(byte[] bytes, String encode) {
    // For now only supports base64.
    if (!BASE64_ENCODE.equalsIgnoreCase(encode)) {
      return badRequest("Encoding method: " + encode + " is not supported");
    }

    ObjectNode jsonNode = OBJECT_MAPPER.createObjectNode();
    jsonNode.put(CONTENT_KEY, Base64.getEncoder().encodeToString(bytes));

    return JSONRepresentation(jsonNode);
  }
}
