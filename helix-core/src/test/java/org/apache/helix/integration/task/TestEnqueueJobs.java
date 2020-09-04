package org.apache.helix.integration.task;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.testng.Assert;
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

    List<String> jobNames = new ArrayList<>();
    List<JobConfig.Builder> jobBuilders = new ArrayList<>();

    _driver.waitToStop(queueName, 5000L);

    for (int i = 0; i < 5; i++) {
      jobNames.add("JOB" + i);
      jobBuilders.add(jobBuilder);
    }
    // Add jobs as batch to the queue
    _driver.enqueueJobs(queueName, jobNames, jobBuilders);

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

  @Test
  public void testQueueParallelJobs() throws InterruptedException {
    final int parallelJobs = 3;
    final int numberOfJobsAddedBeforeControllerSwitch = 4;
    final int totalNumberOfJobs = 7;
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName);
    WorkflowConfig.Builder workflowCfgBuilder = new WorkflowConfig.Builder()
        .setWorkflowId(queueName).setParallelJobs(parallelJobs).setAllowOverlapJobAssignment(true);
    _driver.start(builder.setWorkflowConfig(workflowCfgBuilder.build()).build());
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(Collections.singletonMap(MockTask.JOB_DELAY, "10000"));

    _driver.waitToStop(queueName, 5000L);

    // Add 4 jobs to the queue
    List<String> jobNames = new ArrayList<>();
    List<JobConfig.Builder> jobBuilders = new ArrayList<>();
    for (int i = 0; i < numberOfJobsAddedBeforeControllerSwitch; i++) {
      jobNames.add("JOB" + i);
      jobBuilders.add(jobBuilder);
    }
    _driver.enqueueJobs(queueName, jobNames, jobBuilders);

    _driver.resume(queueName);

    // Wait until all of the enqueued jobs (Job0 to Job3) are finished
    for (int i = 0; i < numberOfJobsAddedBeforeControllerSwitch; i++) {
      _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + i),
          TaskState.COMPLETED);
    }

    // Stop the Controller
    _controller.syncStop();

    // Add 3 more jobs to the queue which should run in parallel after the Controller is started
    jobNames.clear();
    jobBuilders.clear();
    for (int i = numberOfJobsAddedBeforeControllerSwitch; i < totalNumberOfJobs; i++) {
      jobNames.add("JOB" + i);
      jobBuilders.add(jobBuilder);
    }
    _driver.enqueueJobs(queueName, jobNames, jobBuilders);

    // Start the Controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Wait until all of the newly added jobs (Job4 to Job6) are finished
    for (int i = numberOfJobsAddedBeforeControllerSwitch; i < totalNumberOfJobs; i++) {
      _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + i),
          TaskState.COMPLETED);
    }

    // Make sure the jobs have been running in parallel by checking the jobs start time and finish
    // time
    long maxStartTime = Long.MIN_VALUE;
    long minFinishTime = Long.MAX_VALUE;

    for (int i = numberOfJobsAddedBeforeControllerSwitch; i < totalNumberOfJobs; i++) {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(queueName, "JOB" + i));
      maxStartTime = Long.max(maxStartTime, jobContext.getStartTime());
      minFinishTime = Long.min(minFinishTime, jobContext.getFinishTime());
    }
    Assert.assertTrue(minFinishTime > maxStartTime);
  }

  @Test
  public void testQueueJobsMaxCapacity() throws InterruptedException {
    final int numberOfJobsAddedInitially = 4;
    final int queueCapacity = 5;
    final String newJobName = "NewJob";
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName);
    WorkflowConfig.Builder workflowCfgBuilder =
        new WorkflowConfig.Builder().setWorkflowId(queueName).setParallelJobs(1)
            .setAllowOverlapJobAssignment(true).setCapacity(queueCapacity);
    _driver.start(builder.setWorkflowConfig(workflowCfgBuilder.build()).build());
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(Collections.singletonMap(MockTask.JOB_DELAY, "1000"));

    _driver.waitToStop(queueName, 5000L);

    // Add 4 jobs to the queue
    List<String> jobNames = new ArrayList<>();
    List<JobConfig.Builder> jobBuilders = new ArrayList<>();
    for (int i = 0; i < numberOfJobsAddedInitially; i++) {
      jobNames.add("JOB" + i);
      jobBuilders.add(jobBuilder);
    }
    _driver.enqueueJobs(queueName, jobNames, jobBuilders);

    _driver.resume(queueName);

    // Wait until all of the enqueued jobs (Job0 to Job3) are finished
    for (int i = 0; i < numberOfJobsAddedInitially; i++) {
      _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + i),
          TaskState.COMPLETED);
    }

    boolean exceptionHappenedWhileAddingNewJob = false;
    try {
      // This call will produce the exception because 4 jobs have been already added
      // By adding the new job the queue will hit its capacity limit
      _driver.enqueueJob(queueName, newJobName, jobBuilder);
    } catch (Exception e) {
      exceptionHappenedWhileAddingNewJob = true;
    }
    Assert.assertTrue(exceptionHappenedWhileAddingNewJob);

    // Make sure that jobConfig has not been created
    JobConfig jobConfig =
        _driver.getJobConfig(TaskUtil.getNamespacedJobName(queueName, newJobName));
    Assert.assertNull(jobConfig);
  }
}
