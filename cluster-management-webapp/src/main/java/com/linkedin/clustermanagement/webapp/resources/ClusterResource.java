package com.linkedin.clustermanagement.webapp.resources;

import java.util.List;

import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.clustermanager.core.listeners.ClusterManagerException;
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
    return true;
  }
  
  public boolean allowPut()
  {
    return true;
  }
  
  public boolean allowDelete()
  {
    return true;
  }
  
  public Representation represent(Variant variant)
  {
    StringRepresentation presentation = null;
    try
    {
      String zkServer = (String)getContext().getAttributes().get("zkServer");
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      presentation = getClusterRepresentation(zkServer, clusterName);
    }
    
    catch(Exception e)
    {
      getResponse().setEntity("ERROR " + e.getMessage(),
          MediaType.TEXT_PLAIN);
      getResponse().setStatus(Status.SUCCESS_OK);
    }  
    return presentation;
  }
  
  StringRepresentation getClusterRepresentation(String zkServerAddress, String clusterName)
  {
    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
    List<String> instances = setupTool.getClusterManagementTool().getNodeNamesInCluster(clusterName);
    String message = "Contents in cluster "+ clusterName + "\nTotal "+ instances.size() + " Instances:\n";
    for (String instanceName : instances)
    {
      message = message + "{ Instance : "+ instanceName + "}\n";
    }
    
    List<String> hostedEntities = setupTool.getClusterManagementTool().getDatabasesInCluster(clusterName);
    
    message += "\nTotal "+ hostedEntities.size() + " hosted entities:\n";
    for (String hostedEntityName : hostedEntities)
    {
      message = message + "{ HostedEntity : "+ hostedEntityName + "}\n";
    }
    StringRepresentation representation = new StringRepresentation(message, MediaType.APPLICATION_JSON);
    
    return representation;
  }
}
