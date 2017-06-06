package org.apache.helix.rest.server.filters;

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

import com.google.common.io.CharStreams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.rest.server.auditlog.AuditLogger;
import org.apache.log4j.Logger;

@Provider
public class AuditLogFilter implements ContainerRequestFilter, ContainerResponseFilter {
  private static Logger _logger = Logger.getLogger(AuditLogFilter.class.getName());

  @Context
  private HttpServletRequest _servletRequest;

  private List<AuditLogger> _auditLoggers;

  public AuditLogFilter(List<AuditLogger> auditLoggers) {
    _auditLoggers = auditLoggers;
  }

  @Override
  public void filter(ContainerRequestContext request) throws IOException {
    AuditLog.Builder auditLogBuilder = new AuditLog.Builder();
    auditLogBuilder.requestPath(request.getUriInfo().getPath()).httpMethod(request.getMethod())
        .startTime(new Date()).requestHeaders(getHeaders(request.getHeaders()))
        .principal(_servletRequest.getUserPrincipal()).clientIP(_servletRequest.getRemoteAddr())
        .clientHostPort(_servletRequest.getRemoteHost() + ":" + _servletRequest.getRemotePort());

    String entity = getEntity(request.getEntityStream());
    auditLogBuilder.requestEntity(entity);

    InputStream stream = new ByteArrayInputStream(entity.getBytes(StandardCharsets.UTF_8));
    request.setEntityStream(stream);

    request.setProperty(AuditLog.ATTRIBUTE_NAME, auditLogBuilder);
  }

  @Override
  public void filter(ContainerRequestContext request, ContainerResponseContext response)
      throws IOException {
    AuditLog.Builder auditLogBuilder =
        (AuditLog.Builder) request.getProperty(AuditLog.ATTRIBUTE_NAME);
    auditLogBuilder.completeTime(new Date()).responseCode(response.getStatus())
        .responseEntity((String) response.getEntity());

    AuditLog auditLog = auditLogBuilder.build();
    if (_auditLoggers != null) {
      for (AuditLogger logger : _auditLoggers) {
        logger.write(auditLog);
      }
    }
  }

  private List<String> getHeaders(MultivaluedMap<String, String> headersMap) {
    List<String> headers = new ArrayList<>();
    for (String key : headersMap.keySet()) {
      headers.add(key + ":" + headersMap.get(key));
    }

    return headers;
  }

  private String getEntity(InputStream entityStream) {
    if (entityStream != null) {
      try {
        return CharStreams.toString(new InputStreamReader(entityStream, StandardCharsets.UTF_8));
      } catch (IOException e) {
        _logger.warn("Failed to parse input entity stream " + e);
      }
    }
    return null;
  }
}
