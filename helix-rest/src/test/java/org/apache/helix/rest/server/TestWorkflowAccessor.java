package org.apache.helix.rest.server;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.TestHelper;
import org.apache.helix.rest.server.resources.WorkflowAccessor;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.codehaus.jackson.JsonNode;
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
        get("clusters/" + CLUSTER_NAME + "/workflows", Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String workflowsStr = node.get(WorkflowAccessor.WorkflowProperties.Workflows.name()).toString();
    Set<String> workflows = OBJECT_MAPPER.readValue(workflowsStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(workflows, _workflowMap.get(CLUSTER_NAME).keySet());
  }

  @Test(dependsOnMethods = "testGetWorkflows")
  public void testGetWorkflow() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME,
        Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertNotNull(node.get(WorkflowAccessor.WorkflowProperties.WorkflowConfig.name()));
    Assert.assertNotNull(node.get(WorkflowAccessor.WorkflowProperties.WorkflowContext.name()));
    String workflowId =
        node.get(WorkflowAccessor.WorkflowProperties.WorkflowConfig.name()).get("WorkflowID")
            .getTextValue();
    Assert.assertEquals(workflowId, WORKFLOW_NAME);
  }

  @Test(dependsOnMethods = "testGetWorkflow")
  public void testGetWorkflowConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/configs",
        Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String workflowId = node.get("WorkflowID").getTextValue();
    Assert.assertEquals(workflowId, WORKFLOW_NAME);
  }

  @Test(dependsOnMethods = "testGetWorkflowConfig")
  public void testGetWorkflowContext() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/workflows/" + WORKFLOW_NAME + "/context",
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
