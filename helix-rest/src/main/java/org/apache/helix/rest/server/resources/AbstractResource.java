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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.helix.HelixException;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.introspect.CodehausJacksonIntrospector;
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
    deactivate,
    addInstanceTag,
    addVirtualTopologyGroup,
    expand,
    enable,
    disable,
    enableMaintenanceMode,
    disableMaintenanceMode,
    enablePartitions,
    disablePartitions,
    update,
    add,
    delete,
    stoppable,
    rebalance,
    reset,
    resetPartitions,
    removeInstanceTag,
    addResource,
    addWagedResource,
    getResource,
    validateWeight,
    enableWagedRebalance,
    enableWagedRebalanceForAllResources,
    purgeOfflineParticipants,
    getInstance,
    getAllInstances
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

  protected Response serverError(String errorMsg) {
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errorMsgToJson(errorMsg))
        .build();
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

  protected Response OKWithHeader(Object entity, String headerName, Object headerValue) {
    if (headerName == null || headerName.length() == 0) {
      return OK(entity);
    } else {
      return Response.ok(entity).header(headerName, headerValue).build();
    }
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
    return JSONRepresentation(entity, null, null);
  }

  /**
   * Any metadata about the response could be conveyed through the entity headers.
   * More details can be found at 'REST-API-Design-Rulebook' -- Ch4 Metadata Design
   */
  protected Response JSONRepresentation(Object entity, String headerName, Object headerValue) {
    try {
      String jsonStr = toJson(entity);
      return OKWithHeader(jsonStr, headerName, headerValue);
    } catch (IOException e) {
      _logger.error("Failed to convert " + entity + " to JSON response", e);
      return serverError();
    }
  }

  protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Needs a separate object reader for ZNRecord annotated with Jackson 1
  // TODO: remove AnnotationIntrospector config once ZNRecord upgrades Jackson
  protected static ObjectReader ZNRECORD_READER = new ObjectMapper()
      .setAnnotationIntrospector(new CodehausJacksonIntrospector())
      .readerFor(ZNRecord.class);

  protected static String toJson(Object object)
      throws IOException {
    OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);

    StringWriter sw = new StringWriter();
    OBJECT_MAPPER.writeValue(sw, object);
    sw.append('\n');

    return sw.toString();
  }

  protected Command getCommand(String commandStr) throws HelixException {
    if (commandStr == null) {
      throw new HelixException("Command string is null!");
    }
    try {
      return Command.valueOf(commandStr);
    } catch (IllegalArgumentException ex) {
      throw new HelixException("Unknown command: " + commandStr);
    }
  }

  protected String getNamespace() {
    HelixRestNamespace namespace =
        (HelixRestNamespace) _application.getProperties().get(ContextPropertyKeys.METADATA.name());
    return namespace.getName();
  }
}
