package org.apache.helix.rest.server.resources;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import org.apache.helix.ZNRecord;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

@Path("/clusters/{clusterId}/workflows/{workflowName}/jobs")
public class JobAccessor extends AbstractResource {
  private static Logger _logger = Logger.getLogger(JobAccessor.class.getName());

  public enum JobProperties {
    Jobs,
    JobConfig,
    JobContext
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
      return notFound();
    }

    JobContext jobContext =
        driver.getJobContext(jobName);
    jobMap.put(JobProperties.JobContext.name(), null);

    if (jobContext != null) {
      jobMap.put(JobProperties.JobContext.name(), jobContext.getRecord());
    }

    return JSONRepresentation(jobMap);
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
    return notFound();
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
    return notFound();
  }
}
