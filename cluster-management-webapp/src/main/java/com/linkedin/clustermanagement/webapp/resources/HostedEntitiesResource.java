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

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.tools.ClusterSetup;

public class HostedEntitiesResource extends Resource
{
  public static final String _partitions = "partitions";
  public static final String _entityName = "entityName";
  
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
  
  StringRepresentation getHostedEntitiesRepresentation(String zkServerAddress, String clusterName) throws JsonGenerationException, JsonMappingException, IOException
  {
    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
    List<String> hostedEntities = setupTool.getClusterManagementTool().getDatabasesInCluster(clusterName);
    
    ZNRecord hostedEntitiesRecord = new ZNRecord();
    hostedEntitiesRecord.setId("hostedEntities");
    hostedEntitiesRecord.setListField("entities", hostedEntities);
    
    StringRepresentation representation = new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(hostedEntitiesRecord), MediaType.APPLICATION_JSON);
    
    return representation;
  }
  
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String zkServer = (String)getContext().getAttributes().get("zkServer");
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      
      Form form = new Form(entity);
      Map<String, String> paraMap 
      = ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form, ClusterRepresentationUtil._addHostedEntityCommand);
    
      if(!paraMap.containsKey(_entityName))
      {
        throw new ClusterManagerException("Json paramaters does not contain '"+_entityName+"'");
      }
      else if(!paraMap.containsKey(_partitions))
      {
        throw new ClusterManagerException("Json paramaters does not contain '"+_partitions+"'");
      }
      
      String entityName = paraMap.get(_entityName);
      int partitions = Integer.parseInt(paraMap.get(_partitions));
      
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
