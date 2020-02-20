package org.apache.helix.rest.server.resources.zookeeper;

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

import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.base.Enums;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.ServerContext;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.msdcommon.util.ZkValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ZooKeeperAccessor provides methods for accessing ZooKeeper resources (ZNodes).
 * It provides basic ZooKeeper features supported by ZkClient.
 */
@Path("/zookeeper")
public class ZooKeeperAccessor extends AbstractResource {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperAccessor.class.getName());
  private ZkBaseDataAccessor<byte[]> _zkBaseDataAccessor;

  public enum ZooKeeperCommand {
    exists,
    getBinaryData,
    getStringData,
    getChildren
  }

  @GET
  @Path("{path: .+}")
  public Response get(@PathParam("path") String path, @QueryParam("command") String commandStr) {
    ZooKeeperCommand cmd = getZooKeeperCommandIfPresent(commandStr);
    if (cmd == null) {
      return badRequest("Invalid ZooKeeper command: " + commandStr);
    }

    // Lazily initialize ZkBaseDataAccessor
    ServerContext _serverContext =
        (ServerContext) _application.getProperties().get(ContextPropertyKeys.SERVER_CONTEXT.name());
    _zkBaseDataAccessor = _serverContext.getByteArrayZkBaseDataAccessor();

    // Need to prepend a "/" since JAX-RS regex removes it
    path = "/" + path;

    // Check that the path supplied is valid
    if (!ZkValidationUtil.isPathValid(path)) {
      String errMsg = "The given path is not a valid ZooKeeper path: " + path;
      LOG.info(errMsg);
      return badRequest(errMsg);
    }

    switch (cmd) {
      case exists:
        return exists(_zkBaseDataAccessor, path);
      case getBinaryData:
        return getBinaryData(_zkBaseDataAccessor, path);
      case getStringData:
        return getStringData(_zkBaseDataAccessor, path);
      case getChildren:
        return getChildren(_zkBaseDataAccessor, path);
      default:
        String errMsg = "Unsupported command: " + commandStr;
        LOG.error(errMsg);
        return badRequest(errMsg);
    }
  }

  /**
   * Checks if a ZNode exists in the given path.
   * @param zkBaseDataAccessor
   * @param path
   * @return true if a ZNode exists, false otherwise
   */
  private Response exists(ZkBaseDataAccessor<byte[]> zkBaseDataAccessor, String path) {
    Map<String, Boolean> result = ImmutableMap.of(ZooKeeperCommand.exists.name(),
        zkBaseDataAccessor.exists(path, AccessOption.PERSISTENT));
    return JSONRepresentation(result);
  }

  /**
   * Returns a response containing the binary data.
   * @param zkBaseDataAccessor
   * @param path
   * @return
   */
  private Response getBinaryData(ZkBaseDataAccessor<byte[]> zkBaseDataAccessor, String path) {
    byte[] bytes = readBinaryDataFromZK(zkBaseDataAccessor, path);
    Map<String, byte[]> binaryResult =
        ImmutableMap.of(ZooKeeperCommand.getBinaryData.name(), bytes);
    // Note: this serialization (using ObjectMapper) will convert this byte[] into
    // a Base64 String! The REST client (user) must convert the resulting String back into
    // a byte[] using Base64.
    return JSONRepresentation(binaryResult);
  }

  /**
   * Returns a response containing the string data.
   * @param zkBaseDataAccessor
   * @param path
   * @return
   */
  private Response getStringData(ZkBaseDataAccessor<byte[]> zkBaseDataAccessor, String path) {
    byte[] bytes = readBinaryDataFromZK(zkBaseDataAccessor, path);
    Map<String, String> stringResult =
        ImmutableMap.of(ZooKeeperCommand.getStringData.name(), new String(bytes));
    return JSONRepresentation(stringResult);
  }

  /**
   * Returns byte[] from ZooKeeper.
   * @param zkBaseDataAccessor
   * @param path
   * @return
   */
  private byte[] readBinaryDataFromZK(ZkBaseDataAccessor<byte[]> zkBaseDataAccessor, String path) {
    if (zkBaseDataAccessor.exists(path, AccessOption.PERSISTENT)) {
      return zkBaseDataAccessor.get(path, null, AccessOption.PERSISTENT);
    } else {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity(String.format("The ZNode at path %s does not exist!", path)).build());
    }
  }

  /**
   * Returns a list of children ZNode names given the path for the parent ZNode.
   * @param zkBaseDataAccessor
   * @param path
   * @return list of child ZNodes
   */
  private Response getChildren(ZkBaseDataAccessor<byte[]> zkBaseDataAccessor, String path) {
    if (zkBaseDataAccessor.exists(path, AccessOption.PERSISTENT)) {
      Map<String, List<String>> result = ImmutableMap.of(ZooKeeperCommand.getChildren.name(),
          zkBaseDataAccessor.getChildNames(path, AccessOption.PERSISTENT));
      return JSONRepresentation(result);
    } else {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity(String.format("The ZNode at path %s does not exist", path)).build());
    }
  }

  private ZooKeeperCommand getZooKeeperCommandIfPresent(String command) {
    return Enums.getIfPresent(ZooKeeperCommand.class, command).orNull();
  }
}
