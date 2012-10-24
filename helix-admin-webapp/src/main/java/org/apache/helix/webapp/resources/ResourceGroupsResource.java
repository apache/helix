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
package org.apache.helix.webapp.resources;

import java.io.IOException;
import java.util.List;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.webapp.RestAdminApplication;
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


public class ResourceGroupsResource extends Resource
{
  private final static Logger LOG = Logger.getLogger(ResourceGroupsResource.class);

  public ResourceGroupsResource(Context context, Request request, Response response)
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
    return false;
  }

  @Override
  public Representation represent(Variant variant)
  {
    StringRepresentation presentation = null;
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      presentation = getHostedEntitiesRepresentation(clusterName);
    }

    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("", e);
    }
    return presentation;
  }

  StringRepresentation getHostedEntitiesRepresentation(String clusterName) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    ZkClient zkClient =
        (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
    ;
    ClusterSetup setupTool = new ClusterSetup(zkClient);
    List<String> hostedEntities =
        setupTool.getClusterManagementTool().getResourcesInCluster(clusterName);

    ZNRecord hostedEntitiesRecord = new ZNRecord("ResourceGroups");
    hostedEntitiesRecord.setListField("ResourceGroups", hostedEntities);

    StringRepresentation representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(hostedEntitiesRecord),
                                 MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");

      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();

      if (command.equalsIgnoreCase(ClusterSetup.addResource)
          || JsonParameters.CLUSTERSETUP_COMMAND_ALIASES.get(ClusterSetup.addResource)
                                                        .contains(command))
      {
        jsonParameters.verifyCommand(ClusterSetup.addResource);

        String entityName =
            jsonParameters.getParameter(JsonParameters.RESOURCE_GROUP_NAME);
        String stateModelDefRef =
            jsonParameters.getParameter(JsonParameters.STATE_MODEL_DEF_REF);
        int partitions =
            Integer.parseInt(jsonParameters.getParameter(JsonParameters.PARTITIONS));
        String mode = IdealStateModeProperty.AUTO.toString();
        if (jsonParameters.getParameter(JsonParameters.IDEAL_STATE_MODE) != null)
        {
          mode = jsonParameters.getParameter(JsonParameters.IDEAL_STATE_MODE);
        }

        ZkClient zkClient =
            (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
        ;
        ClusterSetup setupTool = new ClusterSetup(zkClient);
        setupTool.addResourceToCluster(clusterName,
                                       entityName,
                                       partitions,
                                       stateModelDefRef,
                                       mode);
      }
      else
      {
        throw new HelixException("Unsupported command: " + command
            + ". Should be one of [" + ClusterSetup.addResource + "]");

      }

      getResponse().setEntity(getHostedEntitiesRepresentation(clusterName));
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Error in posting " + entity, e);
    }
  }
}
