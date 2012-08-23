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
import java.util.List;
import java.util.Map;

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
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class StateModelsResource extends Resource
{
  public StateModelsResource(Context context,
      Request request,
      Response response) 
  {
    super(context, request, response);
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
  }

  public boolean allowGet()
  {
    return true;
  }
  
  public boolean allowPost()
  {
    return true;
  }
  
  public boolean allowPut()
  {
    return false;
  }
  
  public boolean allowDelete()
  {
    return false;
  }
  
  public Representation represent(Variant variant)
  {
    StringRepresentation presentation = null;
    try
    {
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      presentation = getStateModelsRepresentation( clusterName);
    }
    
    catch(Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      
      e.printStackTrace();
    }
    return presentation;
  }
  
  StringRepresentation getStateModelsRepresentation( String clusterName) throws JsonGenerationException, JsonMappingException, IOException
  {
    ClusterSetup setupTool = new ClusterSetup(RestAdminApplication.getZkClient());
    List<String> models = setupTool.getClusterManagementTool().getStateModelDefs(clusterName);
    
    ZNRecord modelDefinitions = new ZNRecord("modelDefinitions");
    modelDefinitions.setListField("models", models);
    
    StringRepresentation representation = new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(modelDefinitions), MediaType.APPLICATION_JSON);
    
    return representation;
  }
  
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      
      Form form = new Form(entity);
      
      Map<String, String> paraMap 
      	= ClusterRepresentationUtil.getFormJsonParameters(form);
        
      if(paraMap.get(ClusterRepresentationUtil._managementCommand).equalsIgnoreCase(ClusterSetup.addStateModelDef))
      {
        String newStateModelString = form.getFirstValue(ClusterRepresentationUtil._newModelDef, true);
        
        ObjectMapper mapper = new ObjectMapper();
        ZNRecord newStateModel = mapper.readValue(new StringReader(newStateModelString),
            ZNRecord.class);
        
        HelixDataAccessor accessor = ClusterRepresentationUtil.getClusterDataAccessor( clusterName);
         
        accessor.setProperty(accessor.keyBuilder().stateModelDef(newStateModel.getId()), new StateModelDefinition(newStateModel) );
        getResponse().setEntity(getStateModelsRepresentation(clusterName));
      }
      else
      {
    	  throw new HelixException("Management command should be "+ ClusterSetup.addStateModelDef);
      }
      
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    catch(Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }  
  }
}
