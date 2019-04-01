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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.TestHelper;
import org.apache.helix.rest.server.resources.helix.JobAccessor;
import org.apache.helix.rest.server.resources.helix.WorkflowAccessor;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestJobAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = TASK_TEST_CLUSTER;
  private final static String WORKFLOW_NAME = WORKFLOW_PREFIX + 0;
  private final static String TEST_QUEUE_NAME = "TestQueue";
  private final static String JOB_NAME = WORKFLOW_NAME + "_" + JOB_PREFIX + 0;
  private final static String TEST_JOB_NAME = "TestJob";
  private final static String JOB_INPUT =
      "{\"id\":\"TestJob\",\"simpleFields\":{\"JobID\":\"Job2\"," + "\"WorkflowID\":\"Workflow1\"},\"mapFields\":{\"Task1\":{\"TASK_ID\":\"Task1\","
          + "\"TASK_COMMAND\":\"Backup\",\"TASK_TARGET_PARTITION\":\"p1\"},\"Task2\":{\"TASK_ID\":"
          + "\"Task2\",\"TASK_COMMAND\":\"ReIndex\"}},\"listFields\":{}}";

  @Test
  public void testGetJobs() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/jobs", null,
        Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String jobsStr = node.get(JobAccessor.JobProperties.Jobs.name()).toString();
    Set<String> jobs = OBJECT_MAPPER.readValue(jobsStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(jobs,
        _workflowMap.get(CLUSTER_NAME).get(WORKFLOW_NAME).getWorkflowConfig().getJobDag()
            .getAllNodes());
  }

  @Test(dependsOnMethods = "testGetJobs")
  public void testGetJob() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body =
        get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/jobs/" + JOB_NAME, null,
            Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertNotNull(node.get(JobAccessor.JobProperties.JobConfig.name()));
    Assert.assertNotNull(node.get(JobAccessor.JobProperties.JobContext.name()));
    String workflowId =
        node.get(JobAccessor.JobProperties.JobConfig.name()).get("simpleFields").get("WorkflowID")
            .getTextValue();
    Assert.assertEquals(workflowId, WORKFLOW_NAME);
  }

  @Test(dependsOnMethods = "testGetJob")
  public void testGetJobConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body =
        get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/jobs/" + JOB_NAME
            + "/configs", null, Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String workflowId = node.get("simpleFields").get("WorkflowID").getTextValue();
    Assert.assertEquals(workflowId, WORKFLOW_NAME);
  }

  @Test(dependsOnMethods = "testGetJobConfig")
  public void testGetJobContext() throws IOException {

    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body =
        get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/jobs/" + JOB_NAME
            + "/context", null, Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertEquals(node.get("mapFields").get("0").get("STATE").getTextValue(),
        TaskPartitionState.COMPLETED.name());
  }

  @Test(dependsOnMethods = "testGetJobContext")
  public void testCreateJob() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    TaskDriver driver = getTaskDriver(CLUSTER_NAME);
    // Create JobQueue
    JobQueue.Builder jobQueue = new JobQueue.Builder(TEST_QUEUE_NAME)
        .setWorkflowConfig(driver.getWorkflowConfig(WORKFLOW_NAME));
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(Collections
            .singletonMap(WorkflowAccessor.WorkflowProperties.WorkflowConfig.name(),
                jobQueue.build().getWorkflowConfig().getRecord().getSimpleFields())),
        MediaType.APPLICATION_JSON_TYPE);
    put("clusters/" + CLUSTER_NAME + "/workflows/" + TEST_QUEUE_NAME, null, entity,
        Response.Status.OK.getStatusCode());

    // Test enqueue job
    entity = Entity.entity(JOB_INPUT, MediaType.APPLICATION_JSON_TYPE);
    put("clusters/" + CLUSTER_NAME + "/workflows/" + TEST_QUEUE_NAME + "/jobs/" + TEST_JOB_NAME,
        null, entity, Response.Status.OK.getStatusCode());

    String jobName = TaskUtil.getNamespacedJobName(TEST_QUEUE_NAME, TEST_JOB_NAME);
    JobConfig jobConfig = driver.getJobConfig(jobName);
    Assert.assertNotNull(jobConfig);

    WorkflowConfig workflowConfig = driver.getWorkflowConfig(TEST_QUEUE_NAME);
    Assert.assertTrue(workflowConfig.getJobDag().getAllNodes().contains(jobName));
  }

  @Test(dependsOnMethods = "testCreateJob")
  public void testGetAddJobContent() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String uri = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/Job_0/userContent";

    // Empty user content
    String body =
        get(uri, null, Response.Status.OK.getStatusCode(), true);
    Map<String, String> contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {});
    Assert.assertTrue(contentStore.isEmpty());

    // Post user content
    Map<String, String> map1 = new HashMap<>();
    map1.put("k1", "v1");
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(map1), MediaType.APPLICATION_JSON_TYPE);
    post(uri, ImmutableMap.of("command", "update"), entity, Response.Status.OK.getStatusCode());

    // update (add items) workflow content store
    body = get(uri, null, Response.Status.OK.getStatusCode(), true);
    contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {});
    Assert.assertEquals(contentStore, map1);

    // modify map1 and verify
    map1.put("k1", "v2");
    map1.put("k2", "v2");
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(map1), MediaType.APPLICATION_JSON_TYPE);
    post(uri, ImmutableMap.of("command", "update"), entity, Response.Status.OK.getStatusCode());
    body = get(uri, null, Response.Status.OK.getStatusCode(), true);
    contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {});
    Assert.assertEquals(contentStore, map1);
  }

  @Test(dependsOnMethods = "testGetAddJobContent")
  public void testInvalidGetAndUpdateJobContentStore() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String validURI = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/Job_0/userContent";
    String invalidURI1 = "clusters/" + CLUSTER_NAME + "/workflows/xxx/jobs/Job_0/userContent"; // workflow not exist
    String invalidURI2 = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/xxx/userContent"; // job not exist
    Entity validEntity = Entity.entity("{\"k1\":\"v1\"}", MediaType.APPLICATION_JSON_TYPE);
    Entity invalidEntity = Entity.entity("{\"k1\":{}}", MediaType.APPLICATION_JSON_TYPE); // not Map<String, String>
    Map<String, String> validCmd = ImmutableMap.of("command", "update");
    Map<String, String> invalidCmd = ImmutableMap.of("command", "delete"); // cmd not supported

    get(invalidURI1, null, Response.Status.NOT_FOUND.getStatusCode(), false);
    get(invalidURI2, null, Response.Status.NOT_FOUND.getStatusCode(), false);

    post(invalidURI1, validCmd, validEntity, Response.Status.NOT_FOUND.getStatusCode());
    post(invalidURI2, validCmd, validEntity, Response.Status.NOT_FOUND.getStatusCode());

    post(validURI, invalidCmd, validEntity, Response.Status.BAD_REQUEST.getStatusCode());
    post(validURI, validCmd, invalidEntity, Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test(dependsOnMethods = "testInvalidGetAndUpdateJobContentStore")
  public void testDeleteJob() throws InterruptedException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    TaskDriver driver = getTaskDriver(CLUSTER_NAME);
    driver.waitToStop(TEST_QUEUE_NAME, 5000);
    delete("clusters/" + CLUSTER_NAME + "/workflows/" + TEST_QUEUE_NAME + "/jobs/" + TEST_JOB_NAME,
        Response.Status.OK.getStatusCode());

    String jobName = TaskUtil.getNamespacedJobName(TEST_QUEUE_NAME, TEST_JOB_NAME);
    JobConfig jobConfig = driver.getJobConfig(jobName);

    Assert.assertNull(jobConfig);

    WorkflowConfig workflowConfig = driver.getWorkflowConfig(TEST_QUEUE_NAME);
    Assert.assertTrue(!workflowConfig.getJobDag().getAllNodes().contains(jobName));
  }
}
