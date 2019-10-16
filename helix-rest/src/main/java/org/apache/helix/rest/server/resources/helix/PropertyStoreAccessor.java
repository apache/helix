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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/clusters/{clusterId}/propertyStore")
public class PropertyStoreAccessor extends AbstractHelixResource {
  private static Logger LOG = LoggerFactory.getLogger(PropertyStoreAccessor.class);

  @GET
  public Response getPropertyByPath(@PathParam("clusterId") String clusterId,
      @QueryParam("path") String urlEncodedPath) {
    String propertyStoreRootPath = PropertyPathBuilder.propertyStore(clusterId);
    HelixPropertyStore<ZNRecord> propertyStore = new ZkHelixPropertyStore<>(
        (ZkBaseDataAccessor<ZNRecord>) getDataAccssor(clusterId).getBaseDataAccessor(),
        propertyStoreRootPath, Collections.emptyList());
    try {
      urlEncodedPath = decode(urlEncodedPath);
    } catch (UnsupportedEncodingException e) {
      LOG.error("The path {} is mis-inputted for cluster {}", urlEncodedPath, clusterId);
      throw new HelixException("The path needs to be url encoded from of '/%/%/.../'");
    }
    ZNRecord record = propertyStore.get(urlEncodedPath, null, AccessOption.PERSISTENT);

    return OK(record.toString());
  }

  private static String decode(String path) throws UnsupportedEncodingException {
    return URLDecoder.decode(path, StandardCharsets.UTF_8.toString());
  }
}
