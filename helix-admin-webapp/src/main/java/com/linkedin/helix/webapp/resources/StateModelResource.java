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
import java.io.StringReader;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class StateModelResource extends Resource
{
  private final static Logger LOG = Logger.getLogger(StateModelResource.class);

  public StateModelResource(Context context, Request request, Response response)
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
      String modelName = (String) getRequest().getAttributes().get("modelName");
      presentation = getStateModelRepresentation( clusterName, modelName);
    }

    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("", e);
    }
    return presentation;
  }

  StringRepresentation getStateModelRepresentation(
                                                   String clusterName,
                                                   String modelName) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);;
    
    String message =
        ClusterRepresentationUtil.getClusterPropertyAsString(zkClient,
                                                             clusterName,
                                                             keyBuilder.stateModelDef(modelName),
                                                             MediaType.APPLICATION_JSON);

    StringRepresentation representation =
        new StringRepresentation(message, MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String modelName = (String) getRequest().getAttributes().get("modelName");
      ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);;
      
      Form form = new Form(entity);

      Map<String, String> paraMap = ClusterRepresentationUtil.getFormJsonParameters(form);

      if (paraMap.get(ClusterRepresentationUtil._managementCommand)
                 .equalsIgnoreCase(ClusterSetup.addStateModelDef))
      {
        String newStateModelString =
            form.getFirstValue(ClusterRepresentationUtil._newModelDef, true);

        ObjectMapper mapper = new ObjectMapper();
        ZNRecord newStateModel =
            mapper.readValue(new StringReader(newStateModelString), ZNRecord.class);

        HelixDataAccessor accessor =
            ClusterRepresentationUtil.getClusterDataAccessor(zkClient, clusterName);

        accessor.setProperty(accessor.keyBuilder().stateModelDef(newStateModel.getId()), new StateModelDefinition(newStateModel) );
        

      }
      else
      {
        new HelixException("Missing '"
            + ClusterSetup.addStateModelDef);
      }
      getResponse().setEntity(getStateModelRepresentation(
                                                          clusterName,
                                                          modelName));
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
}
