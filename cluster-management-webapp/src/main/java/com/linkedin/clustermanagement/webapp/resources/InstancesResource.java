package com.linkedin.clustermanagement.webapp.resources;

import java.io.IOException;
import java.util.List;
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

import com.linkedin.clustermanagement.webapp.RestAdminApplication;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.util.ZNRecordUtil;

public class InstancesResource extends Resource
{
  public static final String _instanceName = "instanceName";
  public static final String _instanceNames = "instanceNames";
  public InstancesResource(Context context,
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
      String zkServer = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      presentation = getInstancesRepresentation(zkServer, clusterName);
    }
    
    catch(Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }  
    return presentation;
  }
  
  StringRepresentation getInstancesRepresentation(String zkServerAddress, String clusterName) throws JsonGenerationException, JsonMappingException, IOException
  {
    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
    List<String> instances = setupTool.getClusterManagementTool().getNodeNamesInCluster(clusterName);
    
    ClusterDataAccessor accessor = ClusterRepresentationUtil.getClusterDataAccessor(zkServerAddress,  clusterName);
    List<ZNRecord> liveInstances = accessor.getClusterPropertyList(ClusterPropertyType.LIVEINSTANCES);
    List<ZNRecord> instanceConfigs = accessor.getClusterPropertyList(ClusterPropertyType.CONFIGS);
    
    Map<String, ZNRecord> liveInstanceMap = ZNRecordUtil.convertListToMap(liveInstances);
    Map<String, ZNRecord> configsMap = ZNRecordUtil.convertListToMap(instanceConfigs);
    
    for (String instanceName : instances)
    {
      boolean isAlive = liveInstanceMap.containsKey(instanceName);
      configsMap.get(instanceName).setSimpleField("Alive", isAlive+"");
    }
    
    StringRepresentation representation = new StringRepresentation(ClusterRepresentationUtil.ObjectToJson(instanceConfigs), MediaType.APPLICATION_JSON);
    
    return representation;
  }
  
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String zkServer = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      
      Form form = new Form(entity);
      
      Map<String, String> paraMap 
      = ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form, ClusterRepresentationUtil._addInstanceCommand);
      
      ClusterSetup setupTool = new ClusterSetup(zkServer);
      if(paraMap.containsKey(_instanceName))
      {
        setupTool.addNodeToCluster(clusterName, paraMap.get(_instanceName));
      }
      else if(paraMap.containsKey(_instanceNames))
      {
        setupTool.addNodesToCluster(clusterName, paraMap.get(_instanceNames).split(";"));
      }
      else
      {
        throw new ClusterManagerException("Json paramaters does not contain '"+_instanceName+"' or '"+_instanceNames+"' ");
      }
      
      // add cluster
      getResponse().setEntity(getInstancesRepresentation(zkServer, clusterName));
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
