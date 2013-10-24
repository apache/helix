package org.apache.helix.webapp.resources;

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

import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.webapp.RestAdminApplication;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

public class ControllerStatusUpdateResource extends ServerResource {
  private final static Logger LOG = Logger.getLogger(ControllerStatusUpdateResource.class);

  public ControllerStatusUpdateResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      String zkServer =
          (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String messageType = (String) getRequest().getAttributes().get("MessageType");
      String messageId = (String) getRequest().getAttributes().get("MessageId");
      // TODO: need pass sessionId to this represent()
      String sessionId = (String) getRequest().getAttributes().get("SessionId");

      presentation =
          getControllerStatusUpdateRepresentation(zkServer, clusterName, sessionId, messageType,
              messageId);
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      LOG.error("", e);
    }
    return presentation;
  }

  StringRepresentation getControllerStatusUpdateRepresentation(String zkServerAddress,
      String clusterName, String sessionId, String messageType, String messageId)
      throws JsonGenerationException, JsonMappingException, IOException {
    Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkClient zkClient = (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
    String message =
        ClusterRepresentationUtil.getPropertyAsString(zkClient, clusterName,
            keyBuilder.controllerTaskStatus(messageType, messageId), MediaType.APPLICATION_JSON);
    StringRepresentation representation =
        new StringRepresentation(message, MediaType.APPLICATION_JSON);
    return representation;
  }
}
