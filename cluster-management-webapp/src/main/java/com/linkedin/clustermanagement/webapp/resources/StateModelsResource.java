package com.linkedin.clustermanagement.webapp.resources;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.agent.zk.ZkClient;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
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

import com.linkedin.clustermanagement.webapp.RestAdminApplication;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.tools.ClusterSetup;

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
      String zkServer = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      presentation = getStateModelsRepresentation(zkServer, clusterName);
    }
    
    catch(Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      
      e.printStackTrace();
    }
    return presentation;
  }
  
  StringRepresentation getStateModelsRepresentation(String zkServerAddress, String clusterName) throws JsonGenerationException, JsonMappingException, IOException
  {
    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
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
      String zkServer = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      
      Form form = new Form(entity);
      
      Map<String, String> paraMap 
      	= ClusterRepresentationUtil.getFormJsonParameters(form);
        
      if(paraMap.get(ClusterRepresentationUtil._managementCommand).equalsIgnoreCase(ClusterRepresentationUtil._addStateModelCommand))
      {
        String newStateModelString = form.getFirstValue(ClusterRepresentationUtil._newModelDef, true);
        
        ObjectMapper mapper = new ObjectMapper();
        ZNRecord newStateModel = mapper.readValue(new StringReader(newStateModelString),
            ZNRecord.class);
        
        ClusterDataAccessor accessor = ClusterRepresentationUtil.getClusterDataAccessor(zkServer,  clusterName);
        accessor.removeClusterProperty(ClusterPropertyType.STATEMODELDEFS, newStateModel.getId());
        
        accessor.setClusterProperty(ClusterPropertyType.STATEMODELDEFS, newStateModel.getId(), newStateModel);
        getResponse().setEntity(getStateModelsRepresentation(zkServer, clusterName));
      }
      else
      {
    	  throw new ClusterManagerException("Management command should be "+ ClusterRepresentationUtil._addStateModelCommand);
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
