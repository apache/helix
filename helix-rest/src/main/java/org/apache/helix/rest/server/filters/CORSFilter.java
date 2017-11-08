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

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

@Provider
public class CORSFilter implements ContainerRequestFilter, ContainerResponseFilter {

  @Override
  public void filter(ContainerRequestContext request) throws IOException {
    // handle preflight
    if (request.getMethod().equalsIgnoreCase("OPTIONS")) {
      Response.ResponseBuilder builder = Response.ok();

      String requestMethods = request.getHeaderString("Access-Control-Request-Method");
      if (requestMethods != null) {
        builder.header("Access-Control-Allow-Methods", requestMethods);
      }

      String allowHeaders = request.getHeaderString("Access-Control-Request-Headers");
      if (allowHeaders != null) {
        builder.header("Access-Control-Allow-Headers", allowHeaders);
      }

      request.abortWith(builder.build());
    }
  }

  @Override
  public void filter(ContainerRequestContext request,
        ContainerResponseContext response) throws IOException {
    // handle origin
    response.getHeaders().putSingle("Access-Control-Allow-Origin", "*");
    response.getHeaders().putSingle("Access-Control-Allow-Credentials", "true");
  }
}
