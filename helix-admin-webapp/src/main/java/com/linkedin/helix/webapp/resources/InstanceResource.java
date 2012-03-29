package com.linkedin.helix.webapp.resources;

import java.io.IOException;
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

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class InstanceResource extends Resource
{
  public InstanceResource(Context context, Request request, Response response)
  {
    super(context, request, response);
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
  }

  @Override
  public boolean allowGet()
  {
    return true;
  }

  @Override
  public boolean allowPost()
  {
    return true;
  }

  @Override
  public boolean allowPut()
  {
    return false;
  }

  @Override
  public boolean allowDelete()
  {
    return true;
  }

  @Override
  public Representation represent(Variant variant)
  {
    StringRepresentation presentation = null;
    try
    {
      String zkServer = (String) getContext().getAttributes().get(
          RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String instanceName = (String) getRequest().getAttributes().get("instanceName");
      presentation = getInstanceRepresentation(zkServer, clusterName, instanceName);
    } catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      e.printStackTrace();
    }
    return presentation;
  }

  StringRepresentation getInstanceRepresentation(String zkServerAddress, String clusterName,
      String instanceName) throws JsonGenerationException, JsonMappingException, IOException
  {
    String message = ClusterRepresentationUtil.getClusterPropertyAsString(zkServerAddress,
        clusterName, MediaType.APPLICATION_JSON, PropertyType.CONFIGS,
        ConfigScopeProperty.PARTICIPANT.toString(), instanceName);

    StringRepresentation representation = new StringRepresentation(message,
        MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String zkServer = (String) getContext().getAttributes().get(
          RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String instanceName = (String) getRequest().getAttributes().get("instanceName");

      Form form = new Form(entity);
      Map<String, String> paraMap = ClusterRepresentationUtil
          .getFormJsonParametersWithCommandVerified(form,
              ClusterRepresentationUtil._enableInstanceCommand);

      boolean enabled = Boolean.parseBoolean(paraMap.get(ClusterRepresentationUtil._enabled));

      ClusterSetup setupTool = new ClusterSetup(zkServer);
      setupTool.getClusterManagementTool().enableInstance(clusterName, instanceName, enabled);

      getResponse().setEntity(getInstanceRepresentation(zkServer, clusterName, instanceName));
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
  }
  

  @Override
  public void removeRepresentations()
  {
    try
    {
      String zkServer = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      String instanceName = (String)getRequest().getAttributes().get("instanceName");
      ClusterSetup setupTool = new ClusterSetup(zkServer);
      setupTool.dropInstanceFromCluster(clusterName, instanceName);
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
