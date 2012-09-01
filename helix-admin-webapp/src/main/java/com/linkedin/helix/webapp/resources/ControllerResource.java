package com.linkedin.helix.webapp.resources;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class ControllerResource extends Resource
{
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
    return false;
  }

  StringRepresentation getControllerRepresentation(String clusterName) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkClient zkClient =
        (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

    // return an empty znrecord if leader not exist
    String value = ClusterRepresentationUtil.ZNRecordToJson(new ZNRecord(""));
    HelixProperty property = accessor.getProperty(keyBuilder.controllerLeader());
    if (property != null)
    {
      value = ClusterRepresentationUtil.ZNRecordToJson(property.getRecord());
    }

    StringRepresentation representation =
        new StringRepresentation(value, MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation represent(Variant variant)
  {
    StringRepresentation presentation = null;
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      presentation = getControllerRepresentation(clusterName);
    }
    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      e.printStackTrace();
    }
    return presentation;
  }

  @Override
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkClient);

      Form form = new Form(entity);

      Map<String, String> paramMap =
          ClusterRepresentationUtil.getFormJsonParameters(form);
      String command = paramMap.get(ClusterRepresentationUtil._managementCommand);

      if (command.equalsIgnoreCase(ClusterSetup.enableCluster))
      {
        boolean enabled =
            Boolean.parseBoolean(paramMap.get(ClusterRepresentationUtil._enabled));

        setupTool.getClusterManagementTool().enableCluster(clusterName, enabled);

        getResponse().setEntity(getControllerRepresentation(clusterName));
        getResponse().setStatus(Status.SUCCESS_OK);
      }
      else
      {
        throw new HelixException("Command not supported: " + command);
      }
    }
    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
  }
}
