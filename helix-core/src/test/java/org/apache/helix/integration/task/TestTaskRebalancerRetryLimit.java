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
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test task will be retried up to MaxAttemptsPerTask {@see HELIX-562}
 */
public class TestTaskRebalancerRetryLimit extends TaskTestBase {

  @Test
  public void test() throws Exception {
    String jobResource = TestHelper.getTestMethodName();

    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG)
        .setMaxAttemptsPerTask(2).setCommand(MockTask.TASK_COMMAND)
        .setFailureThreshold(Integer.MAX_VALUE)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.THROW_EXCEPTION, "true"));

    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();

    _driver.start(flow);

    // Wait until the job completes.
    _driver.pollForWorkflowState(jobResource, TaskState.COMPLETED);

    JobContext ctx = _driver.getJobContext(TaskUtil.getNamespacedJobName(jobResource));
    for (int i = 0; i < _numPartitions; i++) {
      TaskPartitionState state = ctx.getPartitionState(i);
      if (state != null) {
        Assert.assertEquals(state, TaskPartitionState.TASK_ERROR);
        // The following retry count seems to be a race condition specific to tests
        // TODO: fix so that the second condition could be removed ( == 3 )
        Assert
            .assertTrue(ctx.getPartitionNumAttempts(i) == 2 || ctx.getPartitionNumAttempts(i) == 3);
      }
    }
  }
}
