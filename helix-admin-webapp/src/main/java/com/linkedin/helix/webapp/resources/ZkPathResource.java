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
import org.restlet.data.Reference;
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
import com.linkedin.helix.util.ZKClientPool;
import com.linkedin.helix.webapp.RestAdminApplication;

public class ZkPathResource extends Resource
{
  public ZkPathResource(Context context, Request request, Response response)
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
    return true;
  }
  
  public Representation represent(Variant variant)
  {
    StringRepresentation presentation = null;
    try
    {
      String zkServer = (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String zkPath = getZKPath();
      
      ZkClient zkClient = ZKClientPool.getZkClient(zkServer);
      ZNRecord result = new ZNRecord("nodeContent");
      List<String> children = zkClient.getChildren(zkPath);
      if(children.size() > 0)
      {
        result.setListField("children", children);
      }
      else
      {
        result = zkClient.readData(zkPath);
      }
      presentation = new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(result), MediaType.APPLICATION_JSON);
    }
    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      
      e.printStackTrace();
    }
    return presentation;
  }
  
  String getZKPath()
  {
    String zkPath = "/" + getRequest().getResourceRef().getRelativeRef().toString();
    if(zkPath.equals("/.") || zkPath.endsWith("/"))
    {
      zkPath = zkPath.substring(0, zkPath.length() - 1);
    }
    if(zkPath.length() == 0)
    {
      zkPath = "/";
    }
    return zkPath;
  }

  @Override
  public void removeRepresentations()
  {
    try
    {
      String zkServer = (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String zkPath = getZKPath();
      
      ZkClient zkClient = ZKClientPool.getZkClient(zkServer);
      zkClient.deleteRecursive(zkPath);
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
