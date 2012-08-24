/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.webapp.resources;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.webapp.RestAdminApplication;

public class ControllerStatusUpdateResource extends Resource
{
  public ControllerStatusUpdateResource(Context context, Request request,
      Response response)
  {
    super(context, request, response);
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
  }

  @Override
  public boolean allowGet()
  {
    return true;
  }

  @Override
  public boolean allowPost()
  {
    return false;
  }

  @Override
  public boolean allowPut()
  {
    return false;
  }

  @Override
  public boolean allowDelete()
  {
    return false;
  }

  @Override
  public Representation represent(Variant variant)
  {
    StringRepresentation presentation = null;
    try
    {
      String zkServer = (String) getContext().getAttributes().get(
          RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String) getRequest().getAttributes().get(
          "clusterName");
      String messageType = (String) getRequest().getAttributes().get(
          "MessageType");
      String messageId = (String) getRequest().getAttributes().get("MessageId");
      // TODO: need pass sessionId to this represent()
      String sessionId = (String) getRequest().getAttributes().get("SessionId");

      presentation = getControllerStatusUpdateRepresentation(zkServer,
          clusterName, sessionId, messageType, messageId);
    } catch (Exception e)
    {
      String error = ClusterRepresentationUtil
          .getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      e.printStackTrace();
    }
    return presentation;
  }

  StringRepresentation getControllerStatusUpdateRepresentation(
      String zkServerAddress, String clusterName, String sessionId,
      String messageType, String messageId) throws JsonGenerationException,
      JsonMappingException, IOException
  {
    Builder keyBuilder = new PropertyKey.Builder(clusterName);
    String message = ClusterRepresentationUtil.getPropertyAsString(
        clusterName,
        keyBuilder.controllerTaskStatus(messageType, messageId),
        MediaType.APPLICATION_JSON);
    StringRepresentation representation = new StringRepresentation(message,
        MediaType.APPLICATION_JSON);
    return representation;
  }
}
