package com.linkedin.clustermanagement.webapp.resources;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.clustermanagement.webapp.RestAdminApplication;
import com.linkedin.clustermanager.PropertyType;

public class ErrorsResource extends Resource
{
  public ErrorsResource(Context context, Request request, Response response)
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
      String zkServer = (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String instanceName = (String) getRequest().getAttributes().get("instanceName");
      presentation = getInstanceErrorsRepresentation(zkServer, clusterName, instanceName);
    }
    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      
      e.printStackTrace();
    }
    return presentation;
  }

  StringRepresentation getInstanceErrorsRepresentation(String zkServerAddress, String clusterName, String instanceName) throws JsonGenerationException, JsonMappingException, IOException
  {
      String instanceSessionId = ClusterRepresentationUtil.getInstanceSessionId(zkServerAddress, clusterName, instanceName);
      
      String message = ClusterRepresentationUtil.getInstancePropertyNameListAsString(zkServerAddress, clusterName, instanceName, PropertyType.CURRENTSTATES, instanceSessionId, MediaType.APPLICATION_JSON);

      StringRepresentation representation = new StringRepresentation(message, MediaType.APPLICATION_JSON);

      return representation;
  }
  
}
