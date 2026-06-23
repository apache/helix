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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

@Provider
public class CORSFilter implements ContainerRequestFilter, ContainerResponseFilter {

  /**
   * Comma-separated list of origins allowed to make cross-origin requests. A single {@code "*"}
   * enables anonymous wildcard access (without credentials). When unset, no cross-origin requests
   * are permitted.
   */
  public static final String ALLOWED_ORIGINS_PROPERTY = "cors.allowed.origins";

  private static final String WILDCARD = "*";
  // Helix REST only exposes these HTTP methods; do not reflect arbitrary requested methods.
  private static final String ALLOWED_METHODS = "GET, POST, PUT, DELETE, OPTIONS, HEAD";

  private final Set<String> _allowedOrigins;
  private final boolean _allowAnyOrigin;

  public CORSFilter() {
    this(System.getProperty(ALLOWED_ORIGINS_PROPERTY));
  }

  CORSFilter(String allowedOriginsConfig) {
    Set<String> origins = new HashSet<>();
    if (allowedOriginsConfig != null) {
      origins = Arrays.stream(allowedOriginsConfig.split(","))
          .map(String::trim)
          .filter(origin -> !origin.isEmpty())
          .collect(Collectors.toSet());
    }
    _allowAnyOrigin = origins.contains(WILDCARD);
    _allowedOrigins = Collections.unmodifiableSet(origins);
  }

  private boolean isAllowedOrigin(String origin) {
    return origin != null && (_allowAnyOrigin || _allowedOrigins.contains(origin));
  }

  @Override
  public void filter(ContainerRequestContext request) throws IOException {
    // handle preflight
    if (request.getMethod().equalsIgnoreCase("OPTIONS")) {
      Response.ResponseBuilder builder = Response.ok();
      String origin = request.getHeaderString("Origin");

      if (isAllowedOrigin(origin)) {
        if (_allowAnyOrigin && !_allowedOrigins.contains(origin)) {
          // Wildcard, anonymous access only: never advertise credentials with "*".
          builder.header("Access-Control-Allow-Origin", WILDCARD);
        } else {
          // Echo the specific allowed origin and permit credentials.
          builder.header("Access-Control-Allow-Origin", origin);
          builder.header("Access-Control-Allow-Credentials", "true");
          builder.header("Vary", "Origin");
        }

        builder.header("Access-Control-Allow-Methods", ALLOWED_METHODS);

        // Reflect requested headers only for an already-validated allowed origin.
        String allowHeaders = request.getHeaderString("Access-Control-Request-Headers");
        if (allowHeaders != null) {
          builder.header("Access-Control-Allow-Headers", allowHeaders);
        }
      }

      request.abortWith(builder.build());
    }
  }

  @Override
  public void filter(ContainerRequestContext request,
        ContainerResponseContext response) throws IOException {
    // handle origin
    String origin = request.getHeaderString("Origin");
    if (!isAllowedOrigin(origin)) {
      return;
    }

    if (_allowAnyOrigin && !_allowedOrigins.contains(origin)) {
      // Wildcard, anonymous access only: never advertise credentials with "*".
      response.getHeaders().putSingle("Access-Control-Allow-Origin", WILDCARD);
    } else {
      // Echo the specific allowed origin and permit credentials.
      response.getHeaders().putSingle("Access-Control-Allow-Origin", origin);
      response.getHeaders().putSingle("Access-Control-Allow-Credentials", "true");
      response.getHeaders().add("Vary", "Origin");
    }
  }
}
