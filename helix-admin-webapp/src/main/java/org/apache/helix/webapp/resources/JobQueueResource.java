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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.apache.log4j.Logger;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

import java.util.Map;

/**
 * Class for server-side resource at <code>"/clusters/{clusterName}/jobQueues/{jobQueue}"
 * <p>
 * <li>GET list job queue info
 * <li>POST start a new job in a job queue, or stop/resume/flush/delete a job queue
 */
public class JobQueueResource extends ServerResource {
  private final static Logger LOG = Logger.getLogger(JobQueueResource.class);

  public JobQueueResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  /**
   * List job queue info
   * <p>
   * Usage: <code>curl http://{host:port}/clusters/{clusterName}/jobQueues/{jobQueue}
   */
  @Override
  public Representation get() {
    StringRepresentation presentation;
    try {
      String clusterName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
      String jobQueueName =
          ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.JOB_QUEUE);
      presentation = getHostedEntitiesRepresentation(clusterName, jobQueueName);
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      LOG.error("Fail to get job queue", e);
    }
    return presentation;
  }

  StringRepresentation getHostedEntitiesRepresentation(String clusterName, String jobQueueName)
      throws Exception {
    ZkClient zkClient =
        ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
    HelixDataAccessor accessor =
        ClusterRepresentationUtil.getClusterDataAccessor(zkClient, clusterName);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // Get job queue config
    HelixProperty jobQueueConfig = accessor.getProperty(keyBuilder.resourceConfig(jobQueueName));

    // Get job queue context
    String path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);
    HelixPropertyStore<ZNRecord> propertyStore =
        new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(zkClient), path, null);
    WorkflowContext ctx = TaskUtil.getWorkflowContext(propertyStore, jobQueueName);

    // Create the result
    ZNRecord hostedEntitiesRecord = new ZNRecord(jobQueueName);
    if (jobQueueConfig != null) {
      hostedEntitiesRecord.merge(jobQueueConfig.getRecord());
    }
    if (ctx != null) {
      hostedEntitiesRecord.merge(ctx.getRecord());
    }

    StringRepresentation representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(hostedEntitiesRecord),
            MediaType.APPLICATION_JSON);

    return representation;
  }

  /**
   * Start a new job in a job queue, or stop/resume/flush/delete a job queue
   * <p>
   * Usage:
   * <p>
   * <li>Start a new job in a job queue:
   * <code>curl -d @'./{input.txt}' -H 'Content-Type: application/json'
   * http://{host:port}/clusters/{clusterName}/jobQueues/{jobQueue}
   * <p>
   * input.txt: <code>jsonParameters={"command":"start"}&newJob={newJobConfig.yaml}
   * <p>
   * For newJobConfig.yaml, see {@link Workflow#parse(String)}
   * <li>Stop/resume/flush/delete a job queue:
   * <code>curl -d 'jsonParameters={"command":"{stop/resume/flush/delete}"}'
   * -H "Content-Type: application/json" http://{host:port}/clusters/{clusterName}/jobQueues/{jobQueue}
   */
  @Override
  public Representation post(Representation entity) {
    String clusterName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
    String jobQueueName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.JOB_QUEUE);
    ZkClient zkClient =
        ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
    try {
      TaskDriver driver = new TaskDriver(zkClient, clusterName);

      Form form = new Form(entity);
      JsonParameters jsonParameters = new JsonParameters(form);

      TaskDriver.DriverCommand cmd = TaskDriver.DriverCommand.valueOf(jsonParameters.getCommand());
      switch (cmd) {
      case start: {
        // Get the job queue and submit it
        String yamlPayload =
            ResourceUtil.getYamlParameters(form, ResourceUtil.YamlParamKey.NEW_JOB);
        if (yamlPayload == null) {
          throw new HelixException("Yaml job config is required!");
        }
        Workflow workflow = Workflow.parse(yamlPayload);

        for (String jobName : workflow.getJobConfigs().keySet()) {
          Map<String, String> jobCfgMap = workflow.getJobConfigs().get(jobName);
          JobConfig.Builder jobCfgBuilder = JobConfig.Builder.fromMap(jobCfgMap);
          if (workflow.getTaskConfigs() != null && workflow.getTaskConfigs().containsKey(jobName)) {
            jobCfgBuilder.addTaskConfigs(workflow.getTaskConfigs().get(jobName));
          }
          driver.enqueueJob(jobQueueName, TaskUtil.getDenamespacedJobName(jobQueueName, jobName),
              jobCfgBuilder);
        }
        break;
      }
      case stop: {
        driver.stop(jobQueueName);
        break;
      }
      case resume: {
        driver.resume(jobQueueName);
        break;
      }
      case flush: {
        driver.flushQueue(jobQueueName);
        break;
      }
      case delete: {
        driver.delete(jobQueueName);
        break;
      }
      default:
        throw new HelixException("Unsupported job queue command: " + cmd);
      }
      getResponse().setEntity(getHostedEntitiesRepresentation(clusterName, jobQueueName));
      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Error in posting job queue: " + entity, e);
    }
    return null;
  }
}
