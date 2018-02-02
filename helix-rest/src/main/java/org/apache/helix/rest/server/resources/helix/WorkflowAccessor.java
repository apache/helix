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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

@Path("/clusters/{clusterId}/workflows")
public class WorkflowAccessor extends AbstractHelixResource {
  private static Logger _logger = LoggerFactory.getLogger(WorkflowAccessor.class.getName());

  public enum WorkflowProperties {
    Workflows,
    WorkflowConfig,
    WorkflowContext,
    Jobs,
    ParentJobs
  }

  public enum TaskCommand {
    stop,
    resume,
    clean
  }

  @GET
  public Response getWorkflows(@PathParam("clusterId") String clusterId) {
    TaskDriver taskDriver = getTaskDriver(clusterId);
    Map<String, WorkflowConfig> workflowConfigMap = taskDriver.getWorkflows();
    Map<String, List<String>> dataMap = new HashMap<>();
    dataMap.put(WorkflowProperties.Workflows.name(), new ArrayList<>(workflowConfigMap.keySet()));

    return JSONRepresentation(dataMap);
  }

  @GET
  @Path("{workflowId}")
  public Response getWorkflow(@PathParam("clusterId") String clusterId,
      @PathParam("workflowId") String workflowId) {
    TaskDriver taskDriver = getTaskDriver(clusterId);
    WorkflowConfig workflowConfig = taskDriver.getWorkflowConfig(workflowId);
    WorkflowContext workflowContext = taskDriver.getWorkflowContext(workflowId);

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    TextNode id = JsonNodeFactory.instance.textNode(workflowId);
    root.put(Properties.id.name(), id);

    ObjectNode workflowConfigNode = JsonNodeFactory.instance.objectNode();
    ObjectNode workflowContextNode = JsonNodeFactory.instance.objectNode();

    if (workflowConfig != null) {
      getWorkflowConfigNode(workflowConfigNode, workflowConfig.getRecord());
    }

    if (workflowContext != null) {
      getWorkflowContextNode(workflowContextNode, workflowContext.getRecord());
    }

    root.put(WorkflowProperties.WorkflowConfig.name(), workflowConfigNode);
    root.put(WorkflowProperties.WorkflowContext.name(), workflowContextNode);

    JobDag jobDag = workflowConfig.getJobDag();
    ArrayNode jobs = OBJECT_MAPPER.valueToTree(jobDag.getAllNodes());
    ObjectNode parentJobs = OBJECT_MAPPER.valueToTree(jobDag.getChildrenToParents());
    root.put(WorkflowProperties.Jobs.name(), jobs);
    root.put(WorkflowProperties.ParentJobs.name(), parentJobs);

    return JSONRepresentation(root);
  }

  @PUT
  @Path("{workflowId}")
  public Response createWorkflow(@PathParam("clusterId") String clusterId,
      @PathParam("workflowId") String workflowId, String content) {
    TaskDriver driver = getTaskDriver(clusterId);
    Map<String, String> cfgMap;
    try {
      JsonNode root = OBJECT_MAPPER.readTree(content);
      cfgMap = OBJECT_MAPPER
          .readValue(root.get(WorkflowProperties.WorkflowConfig.name()).toString(),
              TypeFactory.defaultInstance()
                  .constructMapType(HashMap.class, String.class, String.class));

      WorkflowConfig workflowConfig = WorkflowConfig.Builder.fromMap(cfgMap).build();

      // Since JobQueue can keep adding jobs, Helix create JobQueue will ignore the jobs
      if (workflowConfig.isJobQueue()) {
        driver.start(new JobQueue.Builder(workflowId).setWorkflowConfig(workflowConfig).build());
        return OK();
      }

      Workflow.Builder workflow = new Workflow.Builder(workflowId);

      if (root.get(WorkflowProperties.Jobs.name()) != null) {
        Map<String, JobConfig.Builder> jobConfigs =
            getJobConfigs((ArrayNode) root.get(WorkflowProperties.Jobs.name()));
        for (Map.Entry<String, JobConfig.Builder> job : jobConfigs.entrySet()) {
          workflow.addJob(job.getKey(), job.getValue());
        }
      }

      if (root.get(WorkflowProperties.ParentJobs.name()) != null) {
        Map<String, List<String>> parentJobs = OBJECT_MAPPER
            .readValue(root.get(WorkflowProperties.ParentJobs.name()).toString(),
                TypeFactory.defaultInstance()
                    .constructMapType(HashMap.class, String.class, List.class));
        for (Map.Entry<String, List<String>> entry : parentJobs.entrySet()) {
          String parentJob = entry.getKey();
          for (String childJob : entry.getValue()) {
            workflow.addParentChildDependency(parentJob, childJob);
          }
        }
      }

      driver.start(workflow.build());
    } catch (IOException e) {
      return badRequest(String
          .format("Invalid input of Workflow %s for reason : %s", workflowId, e.getMessage()));
    } catch (HelixException e) {
      return badRequest(String
          .format("Failed to create workflow %s for reason : %s", workflowId, e.getMessage()));
    }
    return OK();
  }

  @DELETE
  @Path("{workflowId}")
  public Response deleteWorkflow(@PathParam("clusterId") String clusterId,
      @PathParam("workflowId") String workflowId) {
    TaskDriver driver = getTaskDriver(clusterId);
    try {
      driver.delete(workflowId);
    } catch (HelixException e) {
      return badRequest(String
          .format("Failed to delete workflow %s for reason : %s", workflowId, e.getMessage()));
    }
    return OK();
  }

  @POST
  @Path("{workflowId}")
  public Response updateWorkflow(@PathParam("clusterId") String clusterId,
      @PathParam("workflowId") String workflowId, @QueryParam("command") String command) {
    TaskDriver driver = getTaskDriver(clusterId);

    try {
      TaskCommand cmd = TaskCommand.valueOf(command);
      switch (cmd) {
      case stop:
        driver.stop(workflowId);
        break;
      case resume:
        driver.resume(workflowId);
        break;
      case clean:
        driver.cleanupQueue(workflowId);
        break;
      default:
        return badRequest(String.format("Invalid command : %s", command));
      }
    } catch (HelixException e) {
      return badRequest(
          String.format("Failed to execute operation %s for reason : %s", command, e.getMessage()));
    } catch (Exception e) {
      return serverError(e);
    }

    return OK();
  }

  @GET
  @Path("{workflowId}/configs")
  public Response getWorkflowConfig(@PathParam("clusterId") String clusterId,
      @PathParam("workflowId") String workflowId) {
    TaskDriver taskDriver = getTaskDriver(clusterId);
    WorkflowConfig workflowConfig = taskDriver.getWorkflowConfig(workflowId);
    ObjectNode workflowConfigNode = JsonNodeFactory.instance.objectNode();
    if (workflowConfig != null) {
      getWorkflowConfigNode(workflowConfigNode, workflowConfig.getRecord());
    }

    return JSONRepresentation(workflowConfigNode);
  }

  @POST
  @Path("{workflowId}/configs")
  public Response updateWorkflowConfig(@PathParam("clusterId") String clusterId,
      @PathParam("workflowId") String workflowId, String content) {
    ZNRecord record;
    TaskDriver driver = getTaskDriver(clusterId);

    try {
      record = toZNRecord(content);

      WorkflowConfig workflowConfig = driver.getWorkflowConfig(workflowId);
      if (workflowConfig == null) {
        return badRequest(
            String.format("WorkflowConfig for workflow %s does not exists!", workflowId));
      }

      workflowConfig.getRecord().update(record);
      driver.updateWorkflow(workflowId, workflowConfig);
    } catch (HelixException e) {
      return badRequest(
          String.format("Failed to update WorkflowConfig for workflow %s", workflowId));
    } catch (Exception e) {
      return badRequest(String.format("Invalid WorkflowConfig for workflow %s", workflowId));
    }

    return OK();
  }

  @GET
  @Path("{workflowId}/context")
  public Response getWorkflowContext(@PathParam("clusterId") String clusterId,
      @PathParam("workflowId") String workflowId) {
    TaskDriver taskDriver = getTaskDriver(clusterId);
    WorkflowContext workflowContext = taskDriver.getWorkflowContext(workflowId);
    ObjectNode workflowContextNode = JsonNodeFactory.instance.objectNode();
    if (workflowContext != null) {
      getWorkflowContextNode(workflowContextNode, workflowContext.getRecord());
    }

    return JSONRepresentation(workflowContextNode);
  }

  private void getWorkflowConfigNode(ObjectNode workflowConfigNode, ZNRecord record) {
    for (Map.Entry<String, String> entry : record.getSimpleFields().entrySet()) {
      if (!entry.getKey().equals(WorkflowConfig.WorkflowConfigProperty.Dag)) {
        workflowConfigNode.put(entry.getKey(), JsonNodeFactory.instance.textNode(entry.getValue()));
      }
    }
  }

  private void getWorkflowContextNode(ObjectNode workflowContextNode, ZNRecord record) {
    if (record.getMapFields() != null) {
      for (String fieldName : record.getMapFields().keySet()) {
        JsonNode node = OBJECT_MAPPER.valueToTree(record.getMapField(fieldName));
        workflowContextNode.put(fieldName, node);
      }
    }

    if (record.getSimpleFields() != null) {
      for (Map.Entry<String, String> entry : record.getSimpleFields().entrySet()) {
        workflowContextNode
            .put(entry.getKey(), JsonNodeFactory.instance.textNode(entry.getValue()));
      }
    }
  }

  private Map<String, JobConfig.Builder> getJobConfigs(ArrayNode root)
      throws HelixException, IOException {
    Map<String, JobConfig.Builder> jobConfigsMap = new HashMap<>();
    for (Iterator<JsonNode> it = root.getElements(); it.hasNext(); ) {
      JsonNode job = it.next();
      ZNRecord record = null;

      try {
        record = toZNRecord(job.toString());
      } catch (IOException e) {
        // Ignore the parse since it could be just simple fields
      }

      if (record == null || record.getSimpleFields().isEmpty()) {
        Map<String, String> cfgMap = OBJECT_MAPPER.readValue(job.toString(),
            TypeFactory.defaultInstance()
                .constructMapType(HashMap.class, String.class, String.class));
        jobConfigsMap
            .put(job.get(Properties.id.name()).getTextValue(), JobAccessor.getJobConfig(cfgMap));
      } else {
        jobConfigsMap
            .put(job.get(Properties.id.name()).getTextValue(), JobAccessor.getJobConfig(record));
      }
    }

    return jobConfigsMap;
  }
}
