package org.apache.helix.integration.task;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDeleteWorkflow extends TaskTestBase  {
  private static final int DELETE_DELAY = 2000;

  private HelixAdmin admin;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numParitions = 1;
    admin = _gSetupTool.getClusterManagementTool();
    super.beforeClass();
  }

  @Test
  public void testDeleteWorkflow() throws InterruptedException {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1)
        .setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("job1", jobBuilder);
    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName,
        TaskUtil.getNamespacedJobName(jobQueueName, "job1"), TaskState.IN_PROGRESS);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this job queue
    Assert.assertNotNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNotNull(_driver.getWorkflowContext(jobQueueName));
    Assert.assertNotNull(admin.getResourceIdealState(CLUSTER_NAME, jobQueueName));

    // Pause the Controller so that the job queue won't get deleted
    admin.enableCluster(CLUSTER_NAME, false);

    // Attempt the deletion and time out
    try {
      _driver.deleteAndWaitForCompletion(jobQueueName, DELETE_DELAY);
      Assert.fail("Delete must time out and throw a HelixException with the Controller paused, but did not!");
    } catch (HelixException e) {
      // Pass
    }

    // Resume the Controller and call delete again
    admin.enableCluster(CLUSTER_NAME, true);
    _driver.deleteAndWaitForCompletion(jobQueueName, DELETE_DELAY);

    // Check that the deletion operation completed
    Assert.assertNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNull(_driver.getWorkflowContext(jobQueueName));
    Assert.assertNull(admin.getResourceIdealState(CLUSTER_NAME, jobQueueName));
  }

  @Test
  public void testDeleteWorkflowForcefully() throws InterruptedException {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1)
        .setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("job1", jobBuilder);
    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "job1"),
        TaskState.IN_PROGRESS);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this job queue
    Assert.assertNotNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNotNull(_driver.getWorkflowContext(jobQueueName));
    Assert.assertNotNull(_driver.getJobConfig(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNotNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNotNull(admin.getResourceIdealState(CLUSTER_NAME, jobQueueName));

    // Delete the idealstate of workflow
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuild = accessor.keyBuilder();
    accessor.removeProperty(keyBuild.idealStates(jobQueueName));
    Assert.assertNull(admin.getResourceIdealState(CLUSTER_NAME, jobQueueName));

    // Attempt the deletion and and it should time out since idealstate does not exist anymore.
    try {
      _driver.deleteAndWaitForCompletion(jobQueueName, DELETE_DELAY);
      Assert.fail("Delete must time out and throw a HelixException with the Controller paused, but did not!");
    } catch (HelixException e) {
      // Pass
    }

    // delete forcefully
    _driver.delete(jobQueueName, true);

    Assert.assertNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNull(_driver.getWorkflowContext(jobQueueName));
    Assert.assertNull(_driver.getJobConfig(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNull(admin.getResourceIdealState(CLUSTER_NAME, jobQueueName));
  }

  @Test
  public void testDeleteHangingJobs() throws InterruptedException {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1)
        .setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("job1", jobBuilder);
    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "job1"),
        TaskState.IN_PROGRESS);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this job queue
    Assert.assertNotNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNotNull(_driver.getWorkflowContext(jobQueueName));
    Assert.assertNotNull(_driver.getJobConfig(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNotNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNotNull(admin.getResourceIdealState(CLUSTER_NAME, jobQueueName));

    // Delete the idealstate, workflowconfig and context of workflow
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuild = accessor.keyBuilder();
    accessor.removeProperty(keyBuild.idealStates(jobQueueName));
    accessor.removeProperty(keyBuild.resourceConfig(jobQueueName));
    accessor.removeProperty(keyBuild.workflowContext(jobQueueName));

    Assert.assertNull(admin.getResourceIdealState(CLUSTER_NAME, jobQueueName));
    Assert.assertNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNull(_driver.getWorkflowContext(jobQueueName));

    // attemp to delete the job and it should fail with exception.
    try {
      _driver.deleteJob(jobQueueName, "job1");
      Assert.fail("Delete must be rejected and throw a HelixException, but did not!");
    } catch (IllegalArgumentException e) {
      // Pass
    }

    // delete forcefully
    _driver.deleteJob(jobQueueName, "job1", true);

    Assert.assertNull(_driver.getJobConfig(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNull(admin
        .getResourceIdealState(CLUSTER_NAME, TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
  }
}
