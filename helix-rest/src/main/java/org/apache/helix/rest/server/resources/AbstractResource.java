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
import javax.servlet.http.HttpServletRequest;
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
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.ServerContext;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.tools.ClusterSetup;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
@Consumes({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
public class AbstractResource {
  private static Logger _logger = LoggerFactory.getLogger(AbstractResource.class.getName());

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
    enableMaintenanceMode,
    disableMaintenanceMode,
    enablePartitions,
    disablePartitions,
    update,
    delete,
    rebalance,
    reset,
    removeInstanceTag
  }

  @Context
  protected Application _application;

  @Context
  protected HttpServletRequest _servletRequest;
  protected AuditLog.Builder _auditLogBuilder;

  protected void addExceptionToAuditLog(Exception ex) {
    if (_auditLogBuilder == null) {
      _auditLogBuilder =
          (AuditLog.Builder) _servletRequest.getAttribute(AuditLog.ATTRIBUTE_NAME);
    }
    _auditLogBuilder.addException(ex);
  }

  protected Response serverError() {
    return Response.serverError().build();
  }

  protected Response serverError(Exception ex) {
    addExceptionToAuditLog(ex);
    return Response.serverError().entity(errorMsgToJson(ex.getMessage())).build();
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
