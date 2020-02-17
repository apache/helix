package org.apache.helix.metadatastore.server.resources.metadata;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.apache.helix.metadatastore.server.common.ContextPropertyKeys;
import org.apache.helix.metadatastore.server.common.RestNamespace;
import org.apache.helix.metadatastore.server.resources.AbstractResource;


@Path("/namespaces")
public class NamespacesAccessor extends AbstractResource {
  @GET
  public Response getHelixRestNamespaces() {
    @SuppressWarnings("unchecked")
    List<RestNamespace> allNamespaces =
        (List<RestNamespace>) _application.getProperties()
            .get(ContextPropertyKeys.ALL_NAMESPACES.name());
    List<Map<String, String>> ret = new ArrayList<>();
    for (RestNamespace namespace : allNamespaces) {
      ret.add(namespace.getRestInfo());
    }
    return JSONRepresentation(ret);
  }
}
