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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.apache.helix.ZNRecord;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

@Path("/clusters/{clusterId}/workflows")
public class WorkflowAccessor extends AbstractResource {
  private static Logger _logger = Logger.getLogger(WorkflowAccessor.class.getName());

  private enum WorkflowProperties {
    WorkflowConfig,
    WorkflowContext,
    Jobs,
    ParentJobs
  }

  @GET
  @Produces({"application/json", "text/plain"})
  public Response getWorkflows(@PathParam("clusterId") String clusterId) {
    TaskDriver taskDriver = getTaskDriver(clusterId);
    Map<String, WorkflowConfig> workflowConfigMap = taskDriver.getWorkflows();
    Map<String, List<String>> dataMap = new HashMap<>();
    dataMap.put("Workflows", new ArrayList<>(workflowConfigMap.keySet()));

    return JSONRepresentation(dataMap);
  }

  @GET
  @Path("{workflowId}")
  @Produces({"application/json", "text/plain"})
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

    ObjectMapper mapper = new ObjectMapper();
    JobDag jobDag = workflowConfig.getJobDag();
    ArrayNode jobs = mapper.valueToTree(jobDag.getAllNodes());
    ObjectNode parentJobs = mapper.valueToTree(jobDag.getParentsToChildren());
    root.put(WorkflowProperties.Jobs.name(), jobs);
    root.put(WorkflowProperties.ParentJobs.name(), parentJobs);

    return JSONRepresentation(root);
  }

  @GET
  @Path("{workflowId}/configs")
  @Produces({"application/json", "text/plain"})
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

  @GET
  @Path("{workflowId}/context")
  @Produces({"application/json", "text/plain"})
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
        JsonNode node = new ObjectMapper().valueToTree(record.getMapField(fieldName));
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
}
