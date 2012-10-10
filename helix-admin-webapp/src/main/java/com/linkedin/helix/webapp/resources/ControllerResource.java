package com.linkedin.helix.webapp.resources;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
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
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.PauseSignal;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.util.StatusUpdateUtil.Level;
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


    ZNRecord record = null;
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader != null)
    {
      record = leader.getRecord();
    }
    else
    {
      record = new ZNRecord("");
      DateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss.SSSSSS");
      String time = formatter.format(new Date());
      Map<String, String> contentMap = new TreeMap<String, String>();
      contentMap.put("AdditionalInfo", "No leader exists");
      record.setMapField(Level.HELIX_INFO + "-" + time, contentMap);
    }
    
    boolean paused = (accessor.getProperty(keyBuilder.pause()) == null? false : true);
    record.setSimpleField(PropertyType.PAUSE.toString(), "" + paused);

    String retVal = ClusterRepresentationUtil.ZNRecordToJson(record);
    StringRepresentation representation =
        new StringRepresentation(retVal, MediaType.APPLICATION_JSON);

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

      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();

      if (command == null)
      {
        throw new HelixException("Could NOT find 'command' in parameterMap: " + jsonParameters._parameterMap);
      }
      else if (command.equalsIgnoreCase(ClusterSetup.enableCluster))
      {
        boolean enabled =
            Boolean.parseBoolean(jsonParameters.getParameter(JsonParameters.ENABLED));

        setupTool.getClusterManagementTool().enableCluster(clusterName, enabled);
      }
      else
      {
        throw new HelixException("Unsupported command: " + command
                                 + ". Should be one of [" + ClusterSetup.enableCluster + "]");
      }
      
      getResponse().setEntity(getControllerRepresentation(clusterName));
      getResponse().setStatus(Status.SUCCESS_OK);

    }
    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
  }
}
