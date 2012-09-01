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
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
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

import com.linkedin.helix.HelixException;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class ResourceGroupsResource extends Resource
{
  private final static Logger LOG = Logger.getLogger(ResourceGroupsResource.class);

  public static final String _partitions = "partitions";
  public static final String _resourceGroupName = "resourceGroupName";
  public static final String _stateModelDefRef = "stateModelDefRef";
  public static final String _idealStateMode = "mode";  
  
  public ResourceGroupsResource(Context context,
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
      presentation = getHostedEntitiesRepresentation( clusterName);
    }
    
    catch(Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("", e);
    }  
    return presentation;
  }
  
  StringRepresentation getHostedEntitiesRepresentation(String clusterName) throws JsonGenerationException, JsonMappingException, IOException
  {
    ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);;
    ClusterSetup setupTool = new ClusterSetup(zkClient);
    List<String> hostedEntities = setupTool.getClusterManagementTool().getResourcesInCluster(clusterName);
    
    ZNRecord hostedEntitiesRecord = new ZNRecord("ResourceGroups");
    hostedEntitiesRecord.setListField("ResourceGroups", hostedEntities);
    
    StringRepresentation representation = new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(hostedEntitiesRecord), MediaType.APPLICATION_JSON);
    
    return representation;
  }
  
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      
      Form form = new Form(entity);
      Map<String, String> paraMap 
        = ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form, ClusterSetup.addResource);
    
      if(!paraMap.containsKey(_resourceGroupName))
      {
        throw new HelixException("Json paramaters does not contain '"+_resourceGroupName+"'");
      }
      else if(!paraMap.containsKey(_partitions))
      {
        throw new HelixException("Json paramaters does not contain '"+_partitions+"'");
      }
      else if(!paraMap.containsKey(_stateModelDefRef))
      {
        throw new HelixException("Json paramaters does not contain '"+_stateModelDefRef+"'");
      }
      
      String entityName = paraMap.get(_resourceGroupName);
      String stateModelDefRef = paraMap.get(_stateModelDefRef);
      int partitions = Integer.parseInt(paraMap.get(_partitions));
      String mode = IdealStateModeProperty.AUTO.toString();
      if(paraMap.containsKey(_idealStateMode))
      {
        mode = paraMap.get(_idealStateMode);
      }
      
      ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);;
      ClusterSetup setupTool = new ClusterSetup(zkClient);
      setupTool.addResourceToCluster(clusterName, entityName, partitions,stateModelDefRef, mode);
      // add cluster
      getResponse().setEntity(getHostedEntitiesRepresentation(clusterName));
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    catch(Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("", e);
    }  
  }
}
