package org.apache.helix.integration.task;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDeleteWorkflow extends TaskTestBase  {
  private static final int DELETE_DELAY = 3000;

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
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.TIMEOUT_CONFIG, "1000000"));

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
}