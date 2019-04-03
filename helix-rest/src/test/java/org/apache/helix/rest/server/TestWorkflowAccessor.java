package org.apache.helix.rest.server;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.TestHelper;
import org.apache.helix.rest.server.resources.helix.WorkflowAccessor;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskExecutionInfo;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestWorkflowAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";
  private final static String WORKFLOW_NAME = WORKFLOW_PREFIX + 0;
  private final static String TEST_WORKFLOW_NAME = "TestWorkflow";
  private final static String TEST_QUEUE_NAME = TEST_WORKFLOW_NAME + "_JOBQUEUE";
  private final static String WORKFLOW_INPUT =
      "{\"id\":\"Workflow1\",\"WorkflowConfig\":{\"id\":\"Workflow1\",\"Expiry\":\"43200000\","
          + "\"FailureThreshold\":\"0\",\"IsJobQueue\":\"false\",\"TargetState\":\"START\","
          + "\"Terminable\":\"true\",\"capacity\":\"500\"},\"Jobs\":[{\"id\":\"Job1\","
          + "\"simpleFields\":{\"JobID\":\"Job1\",\"WorkflowID\":\"Workflow1\"},\"mapFields\":"
          + "{\"Task1\":{\"TASK_ID\":\"Task1\",\"TASK_COMMAND\":\"Backup\",\"TASK_TARGET_PARTITION\""
          + ":\"p1\"},\"Task2\":{\"TASK_ID\":\"Task2\",\"TASK_COMMAND\":\"ReIndex\"}},"
          + "\"listFields\":{}},{\"id\":\"Job2\",\"Command\":\"Cleanup\",\"TargetResource\":\"DB2\""
          + "}],\"ParentJobs\":{\"Job1\":[\"Job2\"]}}";

  @Test
  public void testGetWorkflows() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body =
        get("clusters/" + CLUSTER_NAME + "/workflows", null, Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String workflowsStr = node.get(WorkflowAccessor.WorkflowProperties.Workflows.name()).toString();
    Set<String> workflows = OBJECT_MAPPER.readValue(workflowsStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(workflows, _workflowMap.get(CLUSTER_NAME).keySet());
  }

  @Test(dependsOnMethods = "testGetWorkflows")
  public void testGetWorkflow() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME, null,
        Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertNotNull(node.get(WorkflowAccessor.WorkflowProperties.WorkflowConfig.name()));
    Assert.assertNotNull(node.get(WorkflowAccessor.WorkflowProperties.WorkflowContext.name()));

    TaskExecutionInfo lastScheduledTask = OBJECT_MAPPER
        .treeToValue(node.get(WorkflowAccessor.WorkflowProperties.LastScheduledTask.name()),
            TaskExecutionInfo.class);
    Assert.assertTrue(lastScheduledTask
        .equals(new TaskExecutionInfo(null, null, null, TaskExecutionInfo.TIMESTAMP_NOT_SET)));
    String workflowId =
        node.get(WorkflowAccessor.WorkflowProperties.WorkflowConfig.name()).get("WorkflowID")
            .getTextValue();
    Assert.assertEquals(workflowId, WORKFLOW_NAME);
  }

  @Test(dependsOnMethods = "testGetWorkflow")
  public void testGetWorkflowConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/configs", null,
        Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String workflowId = node.get("WorkflowID").getTextValue();
    Assert.assertEquals(workflowId, WORKFLOW_NAME);
  }

  @Test(dependsOnMethods = "testGetWorkflowConfig")
  public void testGetWorkflowContext() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/context", null,
        Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertEquals(node.get("STATE").getTextValue(),
        TaskState.IN_PROGRESS.name());
  }

  @Test(dependsOnMethods = "testGetWorkflowContext")
  public void testCreateWorkflow() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    TaskDriver driver = getTaskDriver(CLUSTER_NAME);

    // Create one time workflow
    Entity entity = Entity.entity(WORKFLOW_INPUT, MediaType.APPLICATION_JSON_TYPE);
    put("clusters/" + CLUSTER_NAME + "/workflows/" + TEST_WORKFLOW_NAME, null, entity,
        Response.Status.OK.getStatusCode());

    WorkflowConfig workflowConfig = driver.getWorkflowConfig(TEST_WORKFLOW_NAME);
    Assert.assertNotNull(workflowConfig);
    Assert.assertEquals(workflowConfig.getJobDag().getAllNodes().size(), 2);

    // Create JobQueue
    JobQueue.Builder jobQueue = new JobQueue.Builder(TEST_QUEUE_NAME)
        .setWorkflowConfig(driver.getWorkflowConfig(TEST_WORKFLOW_NAME));
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(Collections
            .singletonMap(WorkflowAccessor.WorkflowProperties.WorkflowConfig.name(),
                jobQueue.build().getWorkflowConfig().getRecord().getSimpleFields())),
        MediaType.APPLICATION_JSON_TYPE);
    put("clusters/" + CLUSTER_NAME + "/workflows/" + TEST_QUEUE_NAME, null, entity,
        Response.Status.OK.getStatusCode());

    workflowConfig = driver.getWorkflowConfig(TEST_QUEUE_NAME);
    Assert.assertNotNull(workflowConfig);
    Assert.assertTrue(workflowConfig.isJobQueue());
    Assert.assertEquals(workflowConfig.getJobDag().getAllNodes().size(), 0);
  }

  @Test(dependsOnMethods = "testCreateWorkflow")
  public void testUpdateWorkflow() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    TaskDriver driver = getTaskDriver(CLUSTER_NAME);

    Entity entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/workflows/" + TEST_QUEUE_NAME,
        ImmutableMap.of("command", "stop"), entity, Response.Status.OK.getStatusCode());
    Assert
        .assertEquals(driver.getWorkflowConfig(TEST_QUEUE_NAME).getTargetState(), TargetState.STOP);

    post("clusters/" + CLUSTER_NAME + "/workflows/" + TEST_QUEUE_NAME,
        ImmutableMap.of("command", "resume"), entity, Response.Status.OK.getStatusCode());
    Assert.assertEquals(driver.getWorkflowConfig(TEST_QUEUE_NAME).getTargetState(),
        TargetState.START);
  }

  @Test(dependsOnMethods = "testUpdateWorkflow")
  public void testGetAndUpdateWorkflowContentStore() throws IOException, InterruptedException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String workflowName = "Workflow_0";
    TaskDriver driver = getTaskDriver(CLUSTER_NAME);
    // Wait for workflow to start processing
    driver.pollForWorkflowState(workflowName, TaskState.IN_PROGRESS, TaskState.COMPLETED, TaskState.FAILED);
    String uri = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/userContent";

    String body =
        get(uri, null, Response.Status.OK.getStatusCode(), true);
    Map<String, String> contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {});
    Assert.assertTrue(contentStore.isEmpty());

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

  @Test(dependsOnMethods = "testGetAndUpdateWorkflowContentStore")
  public void testInvalidGetAndUpdateWorkflowContentStore() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String validURI = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/userContent";
    String invalidURI = "clusters/" + CLUSTER_NAME + "/workflows/xxx/userContent"; // workflow not exist
    Entity validEntity = Entity.entity("{\"k1\":\"v1\"}", MediaType.APPLICATION_JSON_TYPE);
    Entity invalidEntity = Entity.entity("{\"k1\":{}}", MediaType.APPLICATION_JSON_TYPE); // not Map<String, String>
    Map<String, String> validCmd = ImmutableMap.of("command", "update");
    Map<String, String> invalidCmd = ImmutableMap.of("command", "delete"); // cmd not supported

    get(invalidURI, null, Response.Status.NOT_FOUND.getStatusCode(), false);
    // The following expects a OK because if the usercontent ZNode is not there, it is created
    post(invalidURI, validCmd, validEntity, Response.Status.OK.getStatusCode());

    post(validURI, invalidCmd, validEntity, Response.Status.BAD_REQUEST.getStatusCode());
    post(validURI, validCmd, invalidEntity, Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test(dependsOnMethods = "testInvalidGetAndUpdateWorkflowContentStore")
  public void testDeleteWorkflow() throws InterruptedException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    TaskDriver driver = getTaskDriver(CLUSTER_NAME);

    int currentWorkflowNumbers = driver.getWorkflows().size();

    delete("clusters/" + CLUSTER_NAME + "/workflows/" + TEST_WORKFLOW_NAME,
        Response.Status.OK.getStatusCode());
    delete("clusters/" + CLUSTER_NAME + "/workflows/" + TEST_QUEUE_NAME,
        Response.Status.OK.getStatusCode());

    Thread.sleep(500);
    Assert.assertEquals(driver.getWorkflows().size(), currentWorkflowNumbers - 2);
  }
}
