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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.List;

@Path("/ui")
public class UIResourceAccessor extends AbstractResource {
  private static final String INDEX_PAGE = "index.html";
  private static final String UI_RESOURCE_FOLDER = "ui";

  @GET
  public Response getIndex() {
    return getStaticFile(INDEX_PAGE);
  }

  @GET
  @Path("{fileName}")
  public Response getStaticFile(@PathParam("fileName") String fileName) {
    InputStream is = getClass().getClassLoader().getResourceAsStream(UI_RESOURCE_FOLDER + "/" + fileName);

    if (is == null) {
      // forward any other requests to index except index is not found
      return fileName.equalsIgnoreCase(INDEX_PAGE) ? notFound() : getIndex();
    }

    return Response.ok(is, MediaType.TEXT_HTML).build();
  }

  @GET
  @Path("{any: .*}")
  public Response getStaticFile(@PathParam("any") List<PathSegment> segments) {
    // get the last segment
    String fileName = segments.get(segments.size() - 1).getPath();

    return getStaticFile(fileName);
  }
}
