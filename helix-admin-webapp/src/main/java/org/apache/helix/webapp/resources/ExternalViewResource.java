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

import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalViewResource extends ServerResource {
  private final static Logger LOG = LoggerFactory.getLogger(ExternalViewResource.class);

  public ExternalViewResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      String resourceName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.RESOURCE_NAME);

      presentation = getExternalViewRepresentation(clusterName, resourceName);
    }

    catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("Exception in get externalView", e);
    }
    return presentation;
  }

  StringRepresentation getExternalViewRepresentation(String clusterName, String resourceName)
      throws JsonGenerationException, JsonMappingException, IOException {
    Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkClient zkclient =
        ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.RAW_ZKCLIENT);

    String extViewStr =
        ResourceUtil.readZkAsBytes(zkclient, keyBuilder.externalView(resourceName));
    StringRepresentation representation =
        new StringRepresentation(extViewStr, MediaType.APPLICATION_JSON);

    return representation;
  }
}
