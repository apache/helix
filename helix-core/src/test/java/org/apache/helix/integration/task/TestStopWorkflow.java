package org.apache.helix.integration.task;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStopWorkflow extends TaskTestBase {
  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    super.beforeClass();
  }

  @Test
  public void testStopWorkflow() throws InterruptedException {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1)
        .setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.SUCCESS_COUNT_BEFORE_FAIL, "1"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("job1_will_succeed", jobBuilder);
    jobQueue.enqueueJob("job2_will_fail", jobBuilder);
    _driver.start(jobQueue.build());

    // job1 should succeed and job2 should fail, wait until that happens
    _driver.pollForJobState(jobQueueName,
        TaskUtil.getNamespacedJobName(jobQueueName, "job2_will_fail"), TaskState.FAILED);

    Assert.assertTrue(_driver.getWorkflowContext(jobQueueName).getWorkflowState().equals(TaskState.IN_PROGRESS));

    // Now stop the workflow, and it should be stopped because all jobs have completed or failed.
    _driver.waitToStop(jobQueueName, 4000);

    Assert.assertTrue(_driver.getWorkflowContext(jobQueueName).getWorkflowState().equals(TaskState.STOPPED));
  }
}