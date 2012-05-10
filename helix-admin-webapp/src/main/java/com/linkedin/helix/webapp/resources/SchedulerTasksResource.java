/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.webapp.resources;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
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

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.Criteria;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.PropertyPathConfig;
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
    
    DataAccessor accessor = ClusterRepresentationUtil.getClusterDataAccessor(zkServerAddress,  clusterName);
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
        throw new HelixException("SchedulerTasksResource need to have MessageTemplate specified.");
      }
      Map<String, String> messageTemplate = ClusterRepresentationUtil.getFormJsonParameters(form, MESSAGETEMPLATE);
      Criteria criteria = ClusterRepresentationUtil.getFormJsonParameters(Criteria.class, form, CRITERIA);
      String criteriaString = ClusterRepresentationUtil.getFormJsonParameterString(form, CRITERIA);
      if(criteriaString == null)
      {
        throw new HelixException("SchedulerTasksResource need to have Criteria specified.");
      }
      Message schedulerMessage = new Message(MessageType.SCHEDULER_MSG, UUID.randomUUID().toString());
      schedulerMessage.getRecord().getSimpleFields().put(CRITERIA, criteriaString);
      
      schedulerMessage.getRecord().getMapFields().put(MESSAGETEMPLATE, messageTemplate);
      
      schedulerMessage.setTgtSessionId("*");
      schedulerMessage.setTgtName("CONTROLLER");
      schedulerMessage.setSrcInstanceType(InstanceType.CONTROLLER);
      
      DataAccessor accessor = ClusterRepresentationUtil.getClusterDataAccessor(zkServerAddress,  clusterName);
      accessor.setProperty(PropertyType.MESSAGES_CONTROLLER, schedulerMessage, schedulerMessage.getMsgId());
      Map<String, String> resultMap = new HashMap<String, String>();
      resultMap.put("StatusUpdatePath", PropertyPathConfig.getPath(PropertyType.STATUSUPDATES_CONTROLLER, clusterName, MessageType.SCHEDULER_MSG.toString(),schedulerMessage.getMsgId()));
      resultMap.put("MessageType", Message.MessageType.SCHEDULER_MSG.toString());
      resultMap.put("MsgId", schedulerMessage.getMsgId());
      
      // Assemble the rest URL for task status update
      String ipAddress = InetAddress.getLocalHost().getCanonicalHostName();
      String url = "http://" + ipAddress+":" + getContext().getAttributes().get(RestAdminApplication.PORT)
          + "/clusters/" + clusterName+"/Controller/statusUpdates/SCHEDULER_MSG/" + schedulerMessage.getMsgId();
      resultMap.put("statusUpdateUrl", url);
      
      getResponse().setEntity(ClusterRepresentationUtil.ObjectToJson(resultMap), MediaType.APPLICATION_JSON);
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
