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

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * TestTaskNumAttempts tests that number of attempts for tasks are incremented correctly respecting
 * the retry delay.
 * NOTE: This test relies heavily upon timing.
 */
public class TestTaskNumAttempts extends TaskTestBase {
  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    super.beforeClass();
  }

  @Test
  public void testTaskNumAttemptsWithDelay() throws Exception {
    int maxAttempts = Integer.MAX_VALUE; // Allow the count to increase infinitely
    // Use a delay that's long enough for multiple rounds of pipeline
    long retryDelay = 4000L;

    String workflowName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG)
        .setMaxAttemptsPerTask(maxAttempts).setCommand(MockTask.TASK_COMMAND)
        .setWorkflow(workflowName).setFailureThreshold(Integer.MAX_VALUE)
        .setTaskRetryDelay(retryDelay).setJobCommandConfigMap(
            ImmutableMap.of(MockTask.FAILURE_COUNT_BEFORE_SUCCESS, String.valueOf(maxAttempts)));
    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(workflowName, jobBuilder).build();
    _driver.start(flow);

    // Wait until the job completes.
    _driver.pollForWorkflowState(workflowName, TaskState.IN_PROGRESS);

    JobContext jobContext =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, workflowName));
    int expectedAttempts = jobContext.getPartitionNumAttempts(0);

    // Check 3 times to see if maxAttempts match up
    for (int i = 0; i < 3; i++) {
      // Add a small delay to make sure the check falls in the middle of the scheduling timeline
      Thread.sleep(retryDelay + 1000L);
      JobContext ctx =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, workflowName));
      expectedAttempts++;
      Assert.assertEquals(ctx.getPartitionNumAttempts(0), expectedAttempts);
    }
  }
}
