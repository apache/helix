package com.linkedin.helix.webapp.resources;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

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

import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class StatusUpdateResource extends Resource
{
  public StatusUpdateResource(Context context, Request request, Response response)
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
      String resourceGroup = (String) getRequest().getAttributes().get("resourceName");
      
      presentation = getInstanceStatusUpdateRepresentation(zkServer, clusterName, instanceName, resourceGroup);
    }
    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      
      e.printStackTrace();
    }
    return presentation;
  }

  StringRepresentation getInstanceStatusUpdateRepresentation(String zkServerAddress, String clusterName, String instanceName, String resourceGroup) throws JsonGenerationException, JsonMappingException, IOException
  {
    String instanceSessionId = ClusterRepresentationUtil.getInstanceSessionId(zkServerAddress, clusterName, instanceName);
    String message = 
        ClusterRepresentationUtil.getInstancePropertyListAsString(zkServerAddress, clusterName, instanceName, PropertyType.STATUSUPDATES, instanceSessionId+"__"+resourceGroup, MediaType.APPLICATION_JSON);
    StringRepresentation representation = new StringRepresentation(message, MediaType.APPLICATION_JSON);
    return representation;
  }
}
