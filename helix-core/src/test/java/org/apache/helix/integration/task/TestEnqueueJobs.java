package org.apache.helix.integration.task;

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestEnqueueJobs extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  @Test
  public void testJobQueueAddingJobsOneByOne() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName);
    WorkflowConfig.Builder workflowCfgBuilder = new WorkflowConfig.Builder().setWorkflowId(queueName).setParallelJobs(1);
    _driver.start(builder.setWorkflowConfig(workflowCfgBuilder.build()).build());
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2);
    _driver.enqueueJob(queueName, "JOB0", jobBuilder);
    for (int i = 1; i < 5; i++) {
      _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + (i - 1)),
          10000L, TaskState.COMPLETED);
      _driver.waitToStop(queueName, 5000L);
      _driver.enqueueJob(queueName, "JOB" + i, jobBuilder);
      _driver.resume(queueName);
    }

    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + 4),
        TaskState.COMPLETED);
  }

  @Test
  public void testJobQueueAddingJobsAtSametime() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName);
    WorkflowConfig.Builder workflowCfgBuilder =
        new WorkflowConfig.Builder().setWorkflowId(queueName).setParallelJobs(1);
    _driver.start(builder.setWorkflowConfig(workflowCfgBuilder.build()).build());

    // Adding jobs
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2);
    _driver.waitToStop(queueName, 5000L);
    for (int i = 0; i < 5; i++) {
      _driver.enqueueJob(queueName, "JOB" + i, jobBuilder);
    }
    _driver.resume(queueName);

    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + 4),
        TaskState.COMPLETED);
  }

  @Test
  public void testJobSubmitGenericWorkflows() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2);
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    for (int i = 0; i < 5; i++) {
      builder.addJob("JOB" + i, jobBuilder);
    }

    /**
     * Dependency visualization
     *               JOB0
     *
     *             /   |    \
     *
     *         JOB1 <-JOB2   JOB4
     *
     *                 |     /
     *
     *                JOB3
     */

    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addParentChildDependency("JOB0", "JOB2");
    builder.addParentChildDependency("JOB0", "JOB4");
    builder.addParentChildDependency("JOB1", "JOB2");
    builder.addParentChildDependency("JOB2", "JOB3");
    builder.addParentChildDependency("JOB4", "JOB3");
    _driver.start(builder.build());

    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);
  }
}