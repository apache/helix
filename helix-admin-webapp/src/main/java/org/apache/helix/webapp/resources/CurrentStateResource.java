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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.webapp.RestAdminApplication;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CurrentStateResource extends ServerResource {
  private final static Logger LOG = LoggerFactory.getLogger(CurrentStateResource.class);

  public CurrentStateResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String instanceName = (String) getRequest().getAttributes().get("instanceName");
      String resourceGroup = (String) getRequest().getAttributes().get("resourceName");

      presentation =
          getInstanceCurrentStateRepresentation(clusterName, instanceName, resourceGroup);
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("", e);
    }
    return presentation;
  }

  StringRepresentation getInstanceCurrentStateRepresentation(String clusterName,
      String instanceName, String resourceGroup)
      throws JsonGenerationException, JsonMappingException, IOException {
    ZkClient zkClient = (ZkClient) getRequest().getAttributes().get(RestAdminApplication.ZKCLIENT);
    String instanceSessionId =
        ClusterRepresentationUtil.getInstanceSessionId(zkClient, clusterName, instanceName);
    Builder keyBuilder = new PropertyKey.Builder(clusterName);

    String message =
        ClusterRepresentationUtil.getInstancePropertyAsString(zkClient, clusterName,
            keyBuilder.currentState(instanceName, instanceSessionId, resourceGroup),
            MediaType.APPLICATION_JSON);
    StringRepresentation representation =
        new StringRepresentation(message, MediaType.APPLICATION_JSON);
    return representation;
  }
}
