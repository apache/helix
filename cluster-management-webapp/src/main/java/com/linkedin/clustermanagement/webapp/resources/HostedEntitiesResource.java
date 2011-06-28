package com.linkedin.clustermanagement.webapp.resources;

import java.util.List;

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

import com.linkedin.clustermanager.tools.ClusterSetup;

public class HostedEntitiesResource extends Resource
{
  public HostedEntitiesResource(Context context,
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
      String zkServer = (String)getContext().getAttributes().get("zkServer");
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      presentation = getHostedEntitiesRepresentation(zkServer, clusterName);
    }
    
    catch(Exception e)
    {
      getResponse().setEntity("ERROR " + e.getMessage(),
          MediaType.TEXT_PLAIN);
      getResponse().setStatus(Status.SUCCESS_OK);
    }  
    return presentation;
  }
  
  StringRepresentation getHostedEntitiesRepresentation(String zkServerAddress, String clusterName)
  {
    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
    List<String> hostedEntities = setupTool.getClusterManagementTool().getDatabasesInCluster(clusterName);
    String message = "Hosted entities in cluster "+ clusterName + "\nTotal "+ hostedEntities.size() + " entities:\n";
   
    for (String hostedEntityName : hostedEntities)
    {
      message = message + "{ HostedEntity : "+ hostedEntityName + "}\n";
    }
    StringRepresentation representation = new StringRepresentation(message, MediaType.APPLICATION_JSON);
    
    return representation;
  }
  
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String zkServer = (String)getContext().getAttributes().get("zkServer");
      Form form = new Form(entity);
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      String entityName = form.getFirstValue("entityName");
      int partitions = Integer.parseInt(form.getFirstValue("partitions"));
      ClusterSetup setupTool = new ClusterSetup(zkServer);
      setupTool.addDatabaseToCluster(clusterName, entityName, partitions);
      // add cluster
      getResponse().setEntity(getHostedEntitiesRepresentation(zkServer, clusterName));
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    catch(Exception e)
    {
      getResponse().setEntity("ERROR " + e.getMessage(),
          MediaType.TEXT_PLAIN);
      getResponse().setStatus(Status.SUCCESS_OK);
    }  
  }
}
