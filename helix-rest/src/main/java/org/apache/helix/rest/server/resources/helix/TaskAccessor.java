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
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.task.TaskDriver;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/clusters/{clusterId}/workflows/{workflowName}/jobs/{jobName}/tasks")
public class TaskAccessor extends AbstractHelixResource {
  private static Logger _logger = LoggerFactory.getLogger(TaskAccessor.class.getName());

  @GET
  @Path("{taskPartitionId}/userContent")
  public Response getTaskUserContent(
      @PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName,
      @PathParam("jobName") String jobName,
      @PathParam("taskPartitionId") String taskPartitionid
  ) {
    TaskDriver taskDriver = getTaskDriver(clusterId);
    try {
      Map<String, String> contentStore =
          taskDriver.getTaskUserContentMap(workflowName, jobName, taskPartitionid);
      if (contentStore == null) {
        return notFound(String.format(
            "Unable to find content store. Workflow (%s) or Job (%s) or Task content store (%s) not created yet.",
            workflowName, jobName, taskPartitionid));
      }
      return JSONRepresentation(contentStore);
    } catch (ZkNoNodeException e) {
      return notFound(String.format(
          "Unable to find content store. Workflow (%s) or Job (%s) not created yet.",
          workflowName, jobName));
    } catch (Exception e) {
      return serverError(e);
    }
  }

  @POST
  @Path("{taskPartitionId}/userContent")
  public Response updateTaskUserContent(
      @PathParam("clusterId") String clusterId,
      @PathParam("workflowName") String workflowName,
      @PathParam("jobName") String jobName,
      @PathParam("taskPartitionId") String taskPartitionid,
      @QueryParam("command") String commandStr,
      String content
  ) {
    Command cmd;
    Map<String, String> contentMap = Collections.emptyMap();
    try {
      contentMap = OBJECT_MAPPER.readValue(content, new TypeReference<Map<String, String>>() {
      });
    } catch (IOException e) {
      return badRequest(String
          .format("Content %s cannot be deserialized to Map<String, String>. Err: %s", content,
              e.getMessage()));
    }

    try {
      cmd = (commandStr == null || commandStr.isEmpty())
          ? Command.update
          : Command.valueOf(commandStr);
    } catch (IllegalArgumentException ie) {
      return badRequest(String.format("Invalid command: %s. Err: %s", commandStr, ie.getMessage()));
    }

    TaskDriver driver = getTaskDriver(clusterId);
    try {
      switch (cmd) {
      case update:
        driver.addOrUpdateTaskUserContentMap(workflowName, jobName, taskPartitionid, contentMap);
        return OK();
      default:
        return badRequest(String.format("Command \"%s\" is not supported!", cmd));
      }
    } catch (NullPointerException npe) {
      // ZkCacheBasedDataAccessor would throw npe if workflow or job does not exist
      return notFound(
          String.format("Workflow (%s) or job (%s) does not exist", workflowName, jobName));
    } catch (Exception e) {
      _logger.error("Failed to update user content store", e);
      return serverError(e);
    }
  }
}
