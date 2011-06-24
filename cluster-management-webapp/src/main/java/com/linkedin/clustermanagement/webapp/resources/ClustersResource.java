package com.linkedin.clustermanagement.webapp.resources;
import java.util.List;

import org.restlet.Context;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;
import org.restlet.resource.Resource;

import com.linkedin.clustermanager.core.listeners.ClusterManagerException;
import com.linkedin.clustermanager.model.ZNRecord;
import com.linkedin.clustermanager.tools.ClusterSetup;


public class ClustersResource extends Resource 
{
  public ClustersResource(Context context,
            Request request,
            Response response) 
  {
    super(context, request, response);
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
//handle(request,response);
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
      presentation = getClustersRepresentation();
    }
    catch(Exception e)
    {
      getResponse().setEntity("ERROR " + e.getMessage(),
          MediaType.TEXT_PLAIN);
      getResponse().setStatus(Status.SUCCESS_OK);
    }  
    return presentation;
  }
  
  StringRepresentation getClustersRepresentation()
  {
    String zkServer = (String)getContext().getAttributes().get("zkServer");
    ClusterSetup setupTool = new ClusterSetup(zkServer);
    List<String> clusters = setupTool.getClusterManagementTool().getClusters();
    String message = "";
    for (String cluster : clusters)
    {
      message = message + "{ ClusterName : "+ cluster + "}\n";
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
      String clusterName = form.getFirstValue("clusterName");
      ClusterSetup setupTool = new ClusterSetup(zkServer);
      setupTool.addCluster(clusterName, false);
      // add cluster
      getResponse().setEntity(getClustersRepresentation());
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
