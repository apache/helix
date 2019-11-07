package org.apache.helix.rest.server.resources.helix;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.WorkflowConfig;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/clusters/{clusterId}/workflows/{workflowName}/jobs")
public class JobAccessor extends AbstractHelixResource {
  private static Logger _logger = LoggerFactory.getLogger(JobAccessor.class.getName());

  public enum JobProperties {
    Jobs,
    JobConfig,
    JobContext,
    TASK_COMMAND
  }

  @GET
  public Response getJobs(@PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName) {
    TaskDriver driver = getTaskDriver(clusterId);
    WorkflowConfig workflowConfig = driver.getWorkflowConfig(workflowName);
    ObjectNode root = JsonNodeFactory.instance.objectNode();

    if (workflowConfig == null) {
      return badRequest(String.format("Workflow %s is not found!", workflowName));
    }

    Set<String> jobs = workflowConfig.getJobDag().getAllNodes();
    root.put(Properties.id.name(), JobProperties.Jobs.name());
    ArrayNode jobsNode = root.putArray(JobProperties.Jobs.name());

    if (jobs != null) {
      jobsNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(jobs));
    }
    return JSONRepresentation(root);
  }

  @GET
  @Path("{jobName}")
  public Response getJob(@PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName, @PathParam("jobName") String jobName) {
    TaskDriver driver = getTaskDriver(clusterId);
    Map<String, ZNRecord> jobMap = new HashMap<>();


    JobConfig jobConfig = driver.getJobConfig(jobName);
    if (jobConfig != null) {
      jobMap.put(JobProperties.JobConfig.name(), jobConfig.getRecord());
    } else {
      return badRequest(String.format("Job config for %s does not exists", jobName));
    }

    JobContext jobContext =
        driver.getJobContext(jobName);
    jobMap.put(JobProperties.JobContext.name(), null);

    if (jobContext != null) {
      jobMap.put(JobProperties.JobContext.name(), jobContext.getRecord());
    }

    return JSONRepresentation(jobMap);
  }

  @PUT
  @Path("{jobName}")
  public Response addJob(@PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName, @PathParam("jobName") String jobName,
      String content) {
    ZNRecord record;
    TaskDriver driver = getTaskDriver(clusterId);

    try {
      record = toZNRecord(content);
      JobConfig.Builder jobConfig = JobAccessor.getJobConfig(record);
      driver.enqueueJob(workflowName, jobName, jobConfig);
    } catch (HelixException e) {
      return badRequest(
          String.format("Failed to enqueue job %s for reason : %s", jobName, e.getMessage()));
    } catch (IOException e) {
      return badRequest(String.format("Invalid input for Job Config of Job : %s", jobName));
    }

    return OK();
  }

  @DELETE
  @Path("{jobName}")
  public Response deleteJob(@PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName, @PathParam("jobName") String jobName,
      @QueryParam("force") @DefaultValue("false") String forceDelete) {
    boolean force = Boolean.parseBoolean(forceDelete);
    TaskDriver driver = getTaskDriver(clusterId);

    try {
      driver.deleteJob(workflowName, jobName, force);
    } catch (Exception e) {
      return badRequest(e.getMessage());
    }

    return OK();
  }

  @GET
  @Path("{jobName}/configs")
  public Response getJobConfig(@PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName, @PathParam("jobName") String jobName) {
    TaskDriver driver = getTaskDriver(clusterId);

    JobConfig jobConfig = driver.getJobConfig(jobName);
    if (jobConfig != null) {
      return JSONRepresentation(jobConfig.getRecord());
    }
    return badRequest("Job config for " + jobName + " does not exists");
  }

  @GET
  @Path("{jobName}/context")
  public Response getJobContext(@PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName, @PathParam("jobName") String jobName) {
    TaskDriver driver = getTaskDriver(clusterId);

    JobContext jobContext =
        driver.getJobContext(jobName);
    if (jobContext != null) {
      return JSONRepresentation(jobContext.getRecord());
    }
    return badRequest("Job context for " + jobName + " does not exists");
  }

  @GET
  @Path("{jobName}/userContent")
  public Response getJobUserContent(@PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName, @PathParam("jobName") String jobName) {
    TaskDriver taskDriver = getTaskDriver(clusterId);
    try {
      Map<String, String> contentStore =
          taskDriver.getJobUserContentMap(workflowName, jobName);
      if (contentStore == null) {
        return notFound(String.format(
            "Unable to find content store. Workflow (%s) or Job (%s) does not exist.",
            workflowName, jobName));
      }
      return JSONRepresentation(contentStore);
    } catch (ZkNoNodeException e) {
      return notFound("Unable to find content store");
    } catch (Exception e) {
      return serverError(e);
    }
  }

  @POST
  @Path("{jobName}/userContent")
  public Response updateJobUserContent(
      @PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName,
      @PathParam("jobName") String jobName,
      @QueryParam("command") String commandStr,
      String content
  ) {
    Command cmd;
    Map<String, String> contentMap = Collections.emptyMap();
    try {
      contentMap = OBJECT_MAPPER.readValue(content, new TypeReference<Map<String, String>>() {
      });
      cmd = (commandStr == null || commandStr.isEmpty())
          ? Command.update
          : Command.valueOf(commandStr);
    } catch (IOException e) {
      return badRequest(String
          .format("Content %s cannot be deserialized to Map<String, String>. Err: %s", content,
              e.getMessage()));
    } catch (IllegalArgumentException ie) {
      return badRequest(String.format("Invalid command: %s. Err: %s", commandStr, ie.getMessage()));
    }

    TaskDriver driver = getTaskDriver(clusterId);
    try {
      switch (cmd) {
      case update:
        driver.addOrUpdateJobUserContentMap(workflowName, jobName, contentMap);
        return OK();
      default:
        return badRequest(String.format("Command \"%s\" is not supported!", cmd));
      }
    } catch (NullPointerException npe) {
      // ZkCacheBasedDataAccessor would throw npe if workflow or job does not exist
      return notFound(String.format(
          "Unable to find content store. Workflow (%s) or Job (%s) does not exist.",
          workflowName, jobName));
    } catch (Exception e) {
      _logger.error("Failed to update user content store", e);
      return serverError(e);
    }
  }

  protected static JobConfig.Builder getJobConfig(Map<String, String> cfgMap) {
    return new JobConfig.Builder().fromMap(cfgMap);
  }

  protected static JobConfig.Builder getJobConfig(ZNRecord record) {
    JobConfig.Builder jobConfig = new JobConfig.Builder().fromMap(record.getSimpleFields());
    jobConfig.addTaskConfigMap(getTaskConfigMap(record.getMapFields()));

    return jobConfig;
  }

  private static Map<String, TaskConfig> getTaskConfigMap(
      Map<String, Map<String, String>> taskConfigs) {
    Map<String, TaskConfig> taskConfigsMap = new HashMap<>();
    if (taskConfigs == null || taskConfigs.isEmpty()) {
      return Collections.emptyMap();
    }

    for (Map<String, String> taskConfigMap : taskConfigs.values()) {
      if (!taskConfigMap.containsKey(JobProperties.TASK_COMMAND.name())) {
        continue;
      }

      TaskConfig taskConfig =
          new TaskConfig(taskConfigMap.get(JobProperties.TASK_COMMAND.name()), taskConfigMap);
      taskConfigsMap.put(taskConfig.getId(), taskConfig);
    }

    return taskConfigsMap;
  }
}
