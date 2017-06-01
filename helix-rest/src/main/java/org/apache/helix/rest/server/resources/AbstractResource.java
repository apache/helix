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
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.ServerContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.tools.ClusterSetup;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

@Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
@Consumes({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
public class AbstractResource {
  private static Logger _logger = Logger.getLogger(AbstractResource.class.getName());

  public enum Properties {
    id,
    disabled,
    history,
    count,
    error
  }

  public enum Command {
    activate,
    addInstanceTag,
    expand,
    enable,
    disable,
    update,
    delete,
    rebalance,
    reset,
    removeInstanceTag
  }

  @Context
  private Application _application;

  private static ZkClient _zkClient;
  private static HelixAdmin _helixAdmin;
  private static Map<String, TaskDriver> _taskDriverMap = new HashMap<>();
  private static ConfigAccessor _configAccessor;
  private static ClusterSetup _clusterSetup;

  protected ZkClient getZkClient() {
    if (_zkClient == null) {
      ServerContext serverContext = (ServerContext) _application.getProperties()
          .get(ContextPropertyKeys.SERVER_CONTEXT.name());
      _zkClient = serverContext.getZkClient();
    }
    return _zkClient;
  }

  protected HelixAdmin getHelixAdmin () {
    ZkClient zkClient = getZkClient();
    if (_helixAdmin == null) {
      _helixAdmin = new ZKHelixAdmin(zkClient);
    }
    return _helixAdmin;
  }

  protected ClusterSetup getClusterSetup() {
    ZkClient zkClient = getZkClient();
    if (_clusterSetup == null) {
      _clusterSetup = new ClusterSetup(zkClient);
    }

    return _clusterSetup;
  }

  protected TaskDriver getTaskDriver(String clusterName) {
    ZkClient zkClient = getZkClient();
    if (!_taskDriverMap.containsKey(clusterName)) {
      _taskDriverMap.put(clusterName, new TaskDriver(zkClient, clusterName));
    }
    return _taskDriverMap.get(clusterName);
  }

  protected ConfigAccessor getConfigAccessor () {
    ZkClient zkClient = getZkClient();
    if (_configAccessor == null) {
      _configAccessor = new ConfigAccessor(zkClient);
    }
    return _configAccessor;
  }

  protected HelixDataAccessor getDataAccssor(String clusterName) {
    ZkClient zkClient = getZkClient();
    ZkBaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    return new ZKHelixDataAccessor(clusterName, InstanceType.ADMINISTRATOR, baseDataAccessor);
  }

  protected Response serverError() {
    return Response.serverError().build();
  }

  protected Response serverError(String errorMsg) {
    return Response.serverError().entity(errorMsgToJson(errorMsg)).build();
  }

  protected Response notFound() {
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  protected Response notFound(String errorMsg) {
    return Response.status(Response.Status.NOT_FOUND).entity(errorMsgToJson(errorMsg)).build();
  }

  protected Response OK(Object entity) {
    return Response.ok(entity, MediaType.APPLICATION_JSON_TYPE).build();
  }

  protected Response OK() {
    return Response.ok().build();
  }

  protected Response created() {
    return Response.status(Response.Status.CREATED).build();
  }

  protected Response badRequest(String errorMsg) {
    return Response.status(Response.Status.BAD_REQUEST).entity(errorMsgToJson(errorMsg))
        .type(MediaType.TEXT_PLAIN).build();
  }

  private String errorMsgToJson(String error) {
    try {
      Map<String, String> errorMap = new HashMap<>();
      errorMap.put(Properties.error.name(), error);
      return toJson(errorMap);
    } catch (IOException e) {
      _logger.error("Failed to convert " + error + " to JSON string", e);
      return error;
    }
  }

  protected Response JSONRepresentation(Object entity) {
    try {
      String jsonStr = toJson(entity);
      return OK(jsonStr);
    } catch (IOException e) {
      _logger.error("Failed to convert " + entity + " to JSON response", e);
      return serverError();
    }
  }

  protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected static String toJson(Object object)
      throws IOException {
    SerializationConfig serializationConfig = OBJECT_MAPPER.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    OBJECT_MAPPER.writeValue(sw, object);
    sw.append('\n');

    return sw.toString();
  }

  protected static ZNRecord toZNRecord(String data) throws IOException {
    return OBJECT_MAPPER.reader(ZNRecord.class).readValue(data);
  }

  protected Command getCommand(String commandStr) throws HelixException {
    if (commandStr == null) {
      throw new HelixException("Unknown command " + commandStr);
    }
    try {
      Command command = Command.valueOf(commandStr);
      return command;
    } catch (IllegalArgumentException ex) {
      throw new HelixException("Unknown command " + commandStr);
    }
  }
}
