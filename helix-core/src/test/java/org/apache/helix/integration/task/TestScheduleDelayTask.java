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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestScheduleDelayTask extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 1;
    _numReplicas = 1;
    _numParitions = 1;
    super.beforeClass();
  }

  @Test
  public void testScheduleDelayTaskWithDelayTime() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG);

    builder.addParentChildDependency("Job1", "Job4");
    builder.addParentChildDependency("Job2", "Job4");
    builder.addParentChildDependency("Job3", "Job4");
    builder.addJob("Job1", jobBuilder);
    builder.addJob("Job2", jobBuilder);
    builder.addJob("Job3", jobBuilder);
    builder.addJob("Job4", jobBuilder.setExecutionDelay(2000L));

    _driver.start(builder.build());
    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, "Job4"),
        TaskState.COMPLETED);

    long jobFinishTime = 0L;
    for (int i = 1; i <= 3; i++) {
      jobFinishTime = Math.max(jobFinishTime,
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "Job1"))
              .getFinishTime());
    }
    long jobTwoStartTime =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "Job4")).getStartTime();

    Assert.assertTrue(jobTwoStartTime - jobFinishTime >= 2000L);
  }

  @Test
  public void testScheduleDelayTaskWithStartTime() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG);

    long currentTime = System.currentTimeMillis();
    builder.addParentChildDependency("Job1", "Job2");
    builder.addJob("Job1", jobBuilder);
    builder.addJob("Job2", jobBuilder.setExecutionStart(currentTime + 5000L));

    _driver.start(builder.build());
    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, "Job2"),
        TaskState.COMPLETED);

    long jobTwoStartTime =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "Job2")).getStartTime();

    Assert.assertTrue(jobTwoStartTime - currentTime >= 5000L);
  }

  @Test
  public void testJobQueueDelay() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    JobQueue.Builder queueBuild = TaskTestUtil.buildJobQueue(workflowName);

    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG);

    for (int i = 1; i < 4; i++) {
      queueBuild.enqueueJob("Job" + i, jobBuilder);
    }
    queueBuild.enqueueJob("Job4", jobBuilder.setExecutionDelay(2000L));

    _driver.start(queueBuild.build());
    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, "Job4"),
        TaskState.COMPLETED);

    long jobFinishTime =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "Job3")).getFinishTime();

    long jobTwoStartTime =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "Job4")).getStartTime();

    Assert.assertTrue(jobTwoStartTime - jobFinishTime >= 2000L);
  }

  @Test
  public void testDeplayTimeAndStartTime() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG);

    builder.addParentChildDependency("Job1", "Job2");

    long currentTime = System.currentTimeMillis();
    builder.addJob("Job1", jobBuilder);
    builder
        .addJob("Job2", jobBuilder.setExecutionDelay(2000L).setExecutionStart(currentTime + 5000L));

    _driver.start(builder.build());
    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, "Job2"),
        TaskState.COMPLETED);

    long jobTwoStartTime =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "Job2")).getStartTime();

    Assert.assertTrue(jobTwoStartTime - currentTime >= 5000L);
  }
}
