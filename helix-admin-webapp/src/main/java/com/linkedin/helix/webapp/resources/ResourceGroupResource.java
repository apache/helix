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
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class ResourceGroupResource extends Resource
{
  private final static Logger LOG = Logger.getLogger(ResourceGroupResource.class);

  public ResourceGroupResource(Context context, Request request, Response response)
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
    return true;
  }

  @Override
  public boolean allowPut()
  {
    return false;
  }

  @Override
  public boolean allowDelete()
  {
    return true;
  }

  @Override
  public Representation represent(Variant variant)
  {
    StringRepresentation presentation = null;
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String resourceName = (String) getRequest().getAttributes().get("resourceName");
      presentation = getIdealStateRepresentation(clusterName, resourceName);
    }

    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("", e);
    }
    return presentation;
  }

  StringRepresentation getIdealStateRepresentation(String clusterName, String resourceName) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkClient zkClient =
        (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);

    String message =
        ClusterRepresentationUtil.getClusterPropertyAsString(zkClient,
                                                             clusterName,
                                                             keyBuilder.idealStates(resourceName),
                                                             MediaType.APPLICATION_JSON);

    StringRepresentation representation =
        new StringRepresentation(message, MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public void removeRepresentations()
  {
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String resourceGroupName =
          (String) getRequest().getAttributes().get("resourceName");
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      
      ClusterSetup setupTool = new ClusterSetup(zkClient);
      setupTool.dropResourceFromCluster(clusterName, resourceGroupName);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("", e);
    }
  }

  @Override
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String resourceName = (String) getRequest().getAttributes().get("resourceName");

      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();
      if (command.equalsIgnoreCase(ClusterSetup.resetResource))
      {
        ZkClient zkClient =
            (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
        ClusterSetup setupTool = new ClusterSetup(zkClient);
        setupTool.getClusterManagementTool().resetResource(clusterName, Arrays.asList(resourceName));
      }
      else
      {
        throw new HelixException("Unsupported command: " + command
                                 + ". Should be one of [" + ClusterSetup.resetResource + "]");
      }
    }
    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("", e);
    }
  }

}
