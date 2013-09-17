package org.apache.helix.controller.restlet;

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

import java.io.StringReader;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.restlet.data.Form;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;

/**
 * REST resource for ZkPropertyTransfer server to receive PUT requests
 * that submits ZNRecordUpdates
 */
public class ZNRecordUpdateResource extends Resource {
  public static final String UPDATEKEY = "ZNRecordUpdate";
  private static Logger LOG = Logger.getLogger(ZNRecordUpdateResource.class);

  @Override
  public boolean allowGet() {
    return false;
  }

  @Override
  public boolean allowPost() {
    return false;
  }

  @Override
  public boolean allowPut() {
    return true;
  }

  @Override
  public boolean allowDelete() {
    return false;
  }

  @Override
  public void storeRepresentation(Representation entity) {
    try {
      ZKPropertyTransferServer server = ZKPropertyTransferServer.getInstance();

      Form form = new Form(entity);
      String jsonPayload = form.getFirstValue(UPDATEKEY, true);

      // Parse the map from zkPath --> ZNRecordUpdate from the payload
      StringReader sr = new StringReader(jsonPayload);
      ObjectMapper mapper = new ObjectMapper();
      TypeReference<TreeMap<String, ZNRecordUpdate>> typeRef =
          new TypeReference<TreeMap<String, ZNRecordUpdate>>() {
          };
      Map<String, ZNRecordUpdate> holderMap = mapper.readValue(sr, typeRef);
      // Enqueue the ZNRecordUpdate for sending
      for (ZNRecordUpdate holder : holderMap.values()) {
        server.enqueueData(holder);
        LOG.info("Received " + holder.getPath() + " from "
            + getRequest().getClientInfo().getAddress());
      }
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      LOG.error("", e);
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
    }
  }
}
