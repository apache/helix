package com.linkedin.clustermanagement.webapp.resources;

import java.io.IOException;
import java.util.List;

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

import com.linkedin.clustermanagement.webapp.RestAdminApplication;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.tools.ClusterSetup;

public class ClusterResource extends Resource 
{
  public ClusterResource(Context context,
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
    return false;
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
      presentation = getClusterRepresentation(zkServer, clusterName);
    }
    
    catch(Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      
      e.printStackTrace();
    }  
    return presentation;
  }
  
  StringRepresentation getClusterRepresentation(String zkServerAddress, String clusterName) throws JsonGenerationException, JsonMappingException, IOException
  {
    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
    List<String> instances = setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);
    
    ZNRecord clusterSummayRecord = new ZNRecord();
    clusterSummayRecord.setId("cluster summary");
    clusterSummayRecord.setListField("instances", instances);
    
    List<String> hostedEntities = setupTool.getClusterManagementTool().getResourceGroupsInCluster(clusterName);
    clusterSummayRecord.setListField("resourceGroups", hostedEntities);
    
    List<String> models = setupTool.getClusterManagementTool().getStateModelDefs(clusterName);
    clusterSummayRecord.setListField("stateModels", models);
    
    StringRepresentation representation = new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(clusterSummayRecord), MediaType.APPLICATION_JSON);
    
    return representation;
  }
}
