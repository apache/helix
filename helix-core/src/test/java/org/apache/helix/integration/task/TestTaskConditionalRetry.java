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

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test Conditional Task Retry
 */
public class TestTaskConditionalRetry extends TaskTestBase {

  @Test
  public void test() throws Exception {
    int taskRetryCount = 5;
    int num_tasks = 5;

    String jobResource = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = new JobConfig.Builder();
    jobBuilder.setCommand(MockTask.TASK_COMMAND).setTimeoutPerTask(10000)
        .setMaxAttemptsPerTask(taskRetryCount).setFailureThreshold(Integer.MAX_VALUE);

    // create each task configs.
    final int abortedTask = 1;
    final int failedTask = 2;
    final int exceptionTask = 3;

    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int j = 0; j < num_tasks; j++) {
      TaskConfig.Builder configBuilder = new TaskConfig.Builder().setTaskId("task_" + j);
      switch (j) {
      case abortedTask:
        configBuilder.addConfig(MockTask.TASK_RESULT_STATUS, TaskResult.Status.FATAL_FAILED.name());
        break;
      case failedTask:
        configBuilder.addConfig(MockTask.TASK_RESULT_STATUS, TaskResult.Status.FAILED.name());
        break;
      case exceptionTask:
        configBuilder.addConfig(MockTask.THROW_EXCEPTION, Boolean.TRUE.toString());
        break;
      default:
        break;
      }
      configBuilder.setTargetPartition(String.valueOf(j));
      taskConfigs.add(configBuilder.build());
    }
    jobBuilder.addTaskConfigs(taskConfigs);

    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();

    _driver.start(flow);

    // Wait until the job completes.
    _driver.pollForWorkflowState(jobResource, TaskState.COMPLETED);

    JobContext ctx = _driver.getJobContext(TaskUtil.getNamespacedJobName(jobResource));
    for (int i = 0; i < num_tasks; i++) {
      TaskPartitionState state = ctx.getPartitionState(i);
      int retriedCount = ctx.getPartitionNumAttempts(i);
      String taskId = ctx.getTaskIdForPartition(i);

      if (taskId.equals("task_" + abortedTask)) {
        Assert.assertEquals(state, TaskPartitionState.TASK_ABORTED);
        Assert.assertEquals(retriedCount, 1);
      } else if (taskId.equals("task_" + failedTask) || taskId.equals("task_" + exceptionTask)) {
        Assert.assertEquals(state, TaskPartitionState.TASK_ERROR);
        Assert.assertEquals(taskRetryCount, retriedCount);
      } else {
        Assert.assertEquals(state, TaskPartitionState.COMPLETED);
        Assert.assertEquals(retriedCount, 1);
      }
    }
  }
}
