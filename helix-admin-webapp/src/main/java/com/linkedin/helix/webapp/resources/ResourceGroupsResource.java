package com.linkedin.helix.webapp.resources;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class ResourceGroupsResource extends Resource
{
  public static final String _partitions = "partitions";
  public static final String _resourceGroupName = "resourceGroupName";
  public static final String _stateModelDefRef = "stateModelDefRef";
  
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
      String zkServer = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      presentation = getHostedEntitiesRepresentation(zkServer, clusterName);
    }
    
    catch(Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      
      e.printStackTrace();
    }  
    return presentation;
  }
  
  StringRepresentation getHostedEntitiesRepresentation(String zkServerAddress, String clusterName) throws JsonGenerationException, JsonMappingException, IOException
  {
    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
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
      String zkServer = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      
      Form form = new Form(entity);
      Map<String, String> paraMap 
      = ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form, ClusterRepresentationUtil._addResourceGroupCommand);
    
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
      
      ClusterSetup setupTool = new ClusterSetup(zkServer);
      setupTool.addResourceToCluster(clusterName, entityName, partitions,stateModelDefRef);
      // add cluster
      getResponse().setEntity(getHostedEntitiesRepresentation(zkServer, clusterName));
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
