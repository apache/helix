package org.apache.helix.webapp.resources;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.webapp.RestAdminApplication;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

/**
 * This resource can be used to send scheduler tasks to the controller.
 */
public class SchedulerTasksResource extends ServerResource {
  private final static Logger LOG = Logger.getLogger(SchedulerTasksResource.class);

  public static String CRITERIA = "Criteria";
  public static String MESSAGETEMPLATE = "MessageTemplate";
  public static String TASKQUEUENAME = "TaskQueueName";

  public SchedulerTasksResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      presentation = getSchedulerTasksRepresentation();
    }

    catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("", e);
    }
    return presentation;
  }

  StringRepresentation getSchedulerTasksRepresentation() throws JsonGenerationException,
      JsonMappingException, IOException {
    String clusterName = (String) getRequest().getAttributes().get("clusterName");
    String instanceName = (String) getRequest().getAttributes().get("instanceName");
    ZkClient zkClient = (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
    ClusterSetup setupTool = new ClusterSetup(zkClient);
    List<String> instances =
        setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);

    HelixDataAccessor accessor =
        ClusterRepresentationUtil.getClusterDataAccessor(zkClient, clusterName);
    LiveInstance liveInstance =
        accessor.getProperty(accessor.keyBuilder().liveInstance(instanceName));

    StringRepresentation representation = new StringRepresentation("");// (ClusterRepresentationUtil.ObjectToJson(instanceConfigs),
                                                                       // MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation post(Representation entity) {
    try {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      Form form = new Form(entity);
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);

      String msgTemplateString =
          ClusterRepresentationUtil.getFormJsonParameterString(form, MESSAGETEMPLATE);
      if (msgTemplateString == null) {
        throw new HelixException("SchedulerTasksResource need to have MessageTemplate specified.");
      }
      Map<String, String> messageTemplate =
          ClusterRepresentationUtil.getFormJsonParameters(form, MESSAGETEMPLATE);

      String criteriaString = ClusterRepresentationUtil.getFormJsonParameterString(form, CRITERIA);
      if (criteriaString == null) {
        throw new HelixException("SchedulerTasksResource need to have Criteria specified.");
      }
      HelixDataAccessor accessor =
          ClusterRepresentationUtil.getClusterDataAccessor(zkClient, clusterName);
      LiveInstance leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
      if (leader == null) {
        throw new HelixException("There is no leader for the cluster " + clusterName);
      }

      Message schedulerMessage =
          new Message(MessageType.SCHEDULER_MSG, MessageId.from(UUID.randomUUID().toString()));
      schedulerMessage.getRecord().getSimpleFields().put(CRITERIA, criteriaString);

      schedulerMessage.getRecord().getMapFields().put(MESSAGETEMPLATE, messageTemplate);

      schedulerMessage.setTgtSessionId(SessionId.from(leader.getTypedSessionId().stringify()));
      schedulerMessage.setTgtName("CONTROLLER");
      schedulerMessage.setSrcInstanceType(InstanceType.CONTROLLER);
      String taskQueueName =
          ClusterRepresentationUtil.getFormJsonParameterString(form, TASKQUEUENAME);
      if (taskQueueName != null && taskQueueName.length() > 0) {
        schedulerMessage.getRecord().setSimpleField(
            DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE, taskQueueName);
      }
      accessor.setProperty(
          accessor.keyBuilder().controllerMessage(schedulerMessage.getMessageId().stringify()),
          schedulerMessage);

      Map<String, String> resultMap = new HashMap<String, String>();
      resultMap.put("StatusUpdatePath", PropertyPathConfig.getPath(
          PropertyType.STATUSUPDATES_CONTROLLER, clusterName, MessageType.SCHEDULER_MSG.toString(),
          schedulerMessage.getMessageId().stringify()));
      resultMap.put("MessageType", Message.MessageType.SCHEDULER_MSG.toString());
      resultMap.put("MsgId", schedulerMessage.getMessageId().stringify());

      // Assemble the rest URL for task status update
      String ipAddress = InetAddress.getLocalHost().getCanonicalHostName();
      String url =
          "http://" + ipAddress + ":" + getContext().getAttributes().get(RestAdminApplication.PORT)
              + "/clusters/" + clusterName + "/Controller/statusUpdates/SCHEDULER_MSG/"
              + schedulerMessage.getMessageId();
      resultMap.put("statusUpdateUrl", url);

      getResponse().setEntity(ClusterRepresentationUtil.ObjectToJson(resultMap),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("", e);
    }
    return null;
  }
}
