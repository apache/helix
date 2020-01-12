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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.ServerContext;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.rest.server.resources.helix.ClusterAccessor;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ZooKeeperAccessor provides methods for accessing ZooKeeper resources (ZNodes).
 * It provides basic ZooKeeper features supported by ZkClient.
 */
@Path("/zookeeper")
public class ZooKeeperAccessor extends AbstractResource {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperAccessor.class.getName());

  private ServerContext _serverContext =
      (ServerContext) _application.getProperties().get(ContextPropertyKeys.SERVER_CONTEXT.name());
  private ZkBaseDataAccessor<byte[]> _zkBaseDataAccessor =
      _serverContext.getByteArrayZkBaseDataAccessor();

  @GET
  @Path("{path: .+}")
  public Response get(@PathParam("path") String path)

  /**
   * Checks if a ZNode exists in the given path.
   * @param path
   * @return true if a ZNode exists, false otherwise
   */
  @GET
  @Path("exists/{path: .+}")
  public Response exists(@PathParam("path") String path) {
    if (!isPathValid(path)) {
      String errMsg = "exists(): The given path {} is not a valid ZooKeeper path!" + path;
      LOG.error(errMsg);
      return badRequest(errMsg);
    }

    boolean exists = _zkBaseDataAccessor.exists(path, AccessOption.PERSISTENT);
    return JSONRepresentation(exists);
  }

  /**
   * Reads the given path from ZooKeeper and returns the binary data for the ZNode.
   * @param path
   * @return binary data in the ZNode
   */
  @GET
  @Path("getData/{path: .+}")
  public Response getData(@PathParam("path") String path) {
    if (!isPathValid(path)) {
      String errMsg = "getData(): The given path {} is not a valid ZooKeeper path!" + path;
      LOG.error(errMsg);
      return badRequest(errMsg);
    }

    if (_zkBaseDataAccessor.exists(path, AccessOption.PERSISTENT)) {
      byte[] bytes = _zkBaseDataAccessor.get(path, null, AccessOption.PERSISTENT);
      return JSONRepresentation(bytes);
    } else {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity(String.format("The ZNode at path %s does not exist", path)).build());
    }
  }

  /**
   * Returns a list of children ZNode names given the path for the parent ZNode.
   * @param path
   * @return list of child ZNodes
   */
  @GET
  @Path("getChildren/{path: .+}")
  public Response getChildren(@PathParam("path") String path) {
    if (!isPathValid(path)) {
      String errMsg = "getChildren(): The given path {} is not a valid ZooKeeper path!" + path;
      LOG.error(errMsg);
      return badRequest(errMsg);
    }

    if (_zkBaseDataAccessor.exists(path, AccessOption.PERSISTENT)) {
      List<String> children = _zkBaseDataAccessor.getChildNames(path, AccessOption.PERSISTENT);
      return JSONRepresentation(children);
    } else {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
          .entity(String.format("The ZNode at path %s does not exist", path)).build());
    }
  }

  /**
   * Validates whether a given path string is a valid ZK path.
   *
   * Valid matches:
   * /
   * /abc
   * /abc/abc/abc/abc
   * Invalid matches:
   * null or empty string
   * /abc/
   * /abc/abc/abc/abc/
   **/
  public static boolean isPathValid(String path) {
    return path.matches("^/|(/[\\w-]+)+$");
  }
}
