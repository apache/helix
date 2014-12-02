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

import com.google.common.collect.Lists;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Parameter;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JobQueuesResource extends ServerResource {
  private final static Logger LOG = Logger.getLogger(JobQueuesResource.class);

  public JobQueuesResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      presentation = getHostedEntitiesRepresentation(clusterName);
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("Fail to get all job queues", e);
    }
    return presentation;
  }

  StringRepresentation getHostedEntitiesRepresentation(String clusterName)
      throws JsonGenerationException, JsonMappingException, IOException {
    // Get all resources
    ZkClient zkClient =
        ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
    HelixDataAccessor accessor =
        ClusterRepresentationUtil.getClusterDataAccessor(zkClient, clusterName);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Map<String, HelixProperty> resourceConfigMap =
        accessor.getChildValuesMap(keyBuilder.resourceConfigs());

    // Create the result
    ZNRecord hostedEntitiesRecord = new ZNRecord("JobQueues");

    // Filter out non-workflow resources
    Iterator<Map.Entry<String, HelixProperty>> it = resourceConfigMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, HelixProperty> e = it.next();
      HelixProperty resource = e.getValue();
      Map<String, String> simpleFields = resource.getRecord().getSimpleFields();
      boolean isTerminable = resource.getRecord().getBooleanField(WorkflowConfig.TERMINABLE, true);
      if (!simpleFields.containsKey(WorkflowConfig.TARGET_STATE)
          || !simpleFields.containsKey(WorkflowConfig.DAG) || isTerminable) {
        it.remove();
      }
    }

    // Populate the result
    List<String> allResources = Lists.newArrayList(resourceConfigMap.keySet());
    hostedEntitiesRecord.setListField("JobQueues", allResources);

    StringRepresentation representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(hostedEntitiesRecord),
            MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation post(Representation entity) {
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      ZkClient zkClient =
          ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);

      Form form = new Form(entity);
      // Get the job queue and submit it
      if (form.size() < 1) {
        throw new HelixException("Yaml job queue config is required!");
      }
      Parameter payload = form.get(0);
      String yamlPayload = payload.getName();
      if (yamlPayload == null) {
        throw new HelixException("Yaml job queue config is required!");
      }

      Workflow workflow = Workflow.parse(yamlPayload);
      JobQueue.Builder jobQueueCfgBuilder = new JobQueue.Builder(workflow.getName());
      jobQueueCfgBuilder.fromMap(workflow.getWorkflowConfig().getResourceConfigMap());
      TaskDriver driver = new TaskDriver(zkClient, clusterName);
      driver.createQueue(jobQueueCfgBuilder.build());

      getResponse().setEntity(getHostedEntitiesRepresentation(clusterName));
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Exception in posting job queue: " + entity, e);
    }
    return null;
  }
}
