package org.apache.helix.rest.server;

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
import java.util.Set;
import javax.ws.rs.core.Response;

import org.apache.helix.TestHelper;
import org.apache.helix.rest.server.resources.JobAccessor;
import org.apache.helix.task.TaskPartitionState;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestJobAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";
  private final static String WORKFLOW_NAME = WORKFLOW_PREFIX + 0;
  private final static String JOB_NAME = WORKFLOW_NAME + "_" + JOB_PREFIX + 0;

  @Test
  public void testGetJobs() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/jobs",
        Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String jobsStr = node.get(JobAccessor.JobProperties.Jobs.name()).toString();
    Set<String> jobs = OBJECT_MAPPER.readValue(jobsStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(jobs,
        _workflowMap.get(CLUSTER_NAME).get(WORKFLOW_NAME).getWorkflowConfig().getJobDag()
            .getAllNodes());
  }

  @Test
  public void testGetJob() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body =
        get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/jobs/" + JOB_NAME,
            Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertNotNull(node.get(JobAccessor.JobProperties.JobConfig.name()));
    Assert.assertNotNull(node.get(JobAccessor.JobProperties.JobContext.name()));
    String workflowId =
        node.get(JobAccessor.JobProperties.JobConfig.name()).get("simpleFields").get("WorkflowID")
            .getTextValue();
    Assert.assertEquals(workflowId, WORKFLOW_NAME);
  }

  @Test
  public void testGetJobConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body =
        get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/jobs/" + JOB_NAME
            + "/configs", Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String workflowId = node.get("simpleFields").get("WorkflowID").getTextValue();
    Assert.assertEquals(workflowId, WORKFLOW_NAME);
  }

  @Test
  public void testGetJobContext() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body =
        get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/jobs/" + JOB_NAME
            + "/context", Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertEquals(node.get("mapFields").get("0").get("STATE").getTextValue(),
        TaskPartitionState.COMPLETED.name());
  }
}
