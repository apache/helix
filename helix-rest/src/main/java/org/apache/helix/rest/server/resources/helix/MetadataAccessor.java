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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.HelixRestUtils;
import org.apache.helix.rest.server.resources.AbstractResource;

@Path("")
public class MetadataAccessor extends AbstractResource {
  @GET
  public Response getMetadata() {
    if (HelixRestUtils.isDefaultServlet(_servletRequest.getServletPath())) {
      // To keep API endpoints to behave the same, if user call /admin/v2/ ,
      // we will return NotFound
      return notFound();
    }
    // This will be the root of all namespaced servlets, and returns
    // servlet namespace information
    HelixRestNamespace namespace = (HelixRestNamespace) _application.getProperties().get(
        ContextPropertyKeys.METADATA.name());
    return JSONRepresentation(namespace.getRestInfo());
  }
}
