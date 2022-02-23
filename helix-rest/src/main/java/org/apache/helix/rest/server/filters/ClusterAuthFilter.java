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

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.apache.helix.rest.server.authValidator.AuthValidator;


@ClusterAuth
@Provider
public class ClusterAuthFilter implements ContainerRequestFilter {

  AuthValidator _authValidator;

  public ClusterAuthFilter(AuthValidator authValidator) {
    _authValidator = authValidator;
  }

  @Override
  public void filter(ContainerRequestContext request) {
    if (!_authValidator.validate(request)) {
      request.abortWith(Response.status(Response.Status.FORBIDDEN).build());
    }
  }
}
