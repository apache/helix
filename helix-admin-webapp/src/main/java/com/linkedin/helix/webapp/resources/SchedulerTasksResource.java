package com.linkedin.helix.webapp.resources;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.ClusterManagerException;
import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.util.ZNRecordUtil;
import com.linkedin.helix.webapp.RestAdminApplication;

/**
 * This resource can be used to send scheduler tasks to the controller.
 * 
 * */
public class SchedulerTasksResource extends Resource
{
  public static String CRITERIA = "Criteria";
  public static String MESSAGETEMPLATE = "MessageTemplate";
  public SchedulerTasksResource(Context context,
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
      String instanceName = (String)getRequest().getAttributes().get("instanceName");
      presentation = getSchedulerTasksRepresentation(zkServer, clusterName, instanceName);
    }
    
    catch(Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      
      e.printStackTrace();
    }  
    return presentation;
  }
  
  StringRepresentation getSchedulerTasksRepresentation(String zkServerAddress, String clusterName, String instanceName) throws JsonGenerationException, JsonMappingException, IOException
  {
    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
    List<String> instances = setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);
    
    ClusterDataAccessor accessor = ClusterRepresentationUtil.getClusterDataAccessor(zkServerAddress,  clusterName);
    LiveInstance liveInstance = accessor.getProperty(LiveInstance.class, PropertyType.LIVEINSTANCES, instanceName);
    String sessionId = liveInstance.getSessionId();
    
    StringRepresentation representation = new StringRepresentation("");//(ClusterRepresentationUtil.ObjectToJson(instanceConfigs), MediaType.APPLICATION_JSON);
    
    return representation;
  }
  
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String zkServerAddress = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String)getRequest().getAttributes().get("clusterName");
      Form form = new Form(entity);
      
      String msgTemplateString = ClusterRepresentationUtil.getFormJsonParameterString(form, MESSAGETEMPLATE);
      if(msgTemplateString == null)
      {
        throw new ClusterManagerException("SchedulerTasksResource need to have MessageTemplate specified.");
      }
      Map<String, String> messageTemplate = ClusterRepresentationUtil.getFormJsonParameters(form, MESSAGETEMPLATE);
      Criteria criteria = ClusterRepresentationUtil.getFormJsonParameters(Criteria.class, form, CRITERIA);
      String criteriaString = ClusterRepresentationUtil.getFormJsonParameterString(form, CRITERIA);
      if(criteriaString == null)
      {
        throw new ClusterManagerException("SchedulerTasksResource need to have Criteria specified.");
      }
      Message schedulerMessage = new Message(MessageType.SCHEDULER_MSG, UUID.randomUUID().toString());
      schedulerMessage.getRecord().getSimpleFields().put(CRITERIA, criteriaString);
      
      schedulerMessage.getRecord().getMapFields().put(MESSAGETEMPLATE, messageTemplate);
      
      schedulerMessage.setTgtSessionId("*");
      schedulerMessage.setTgtName("CONTROLLER");
      schedulerMessage.setSrcInstanceType(InstanceType.CONTROLLER);
      
      ClusterDataAccessor accessor = ClusterRepresentationUtil.getClusterDataAccessor(zkServerAddress,  clusterName);
      accessor.setProperty(PropertyType.MESSAGES_CONTROLLER, schedulerMessage, schedulerMessage.getMsgId());
      
      getResponse().setEntity("");
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
