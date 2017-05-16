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
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestTaskRetryDelay extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numParitions = 1;
    super.beforeClass();
  }

  @Test public void testTaskRetryWithDelay() throws Exception {
    String jobResource = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG)
        .setMaxAttemptsPerTask(2).setCommand(MockTask.TASK_COMMAND).setWorkflow(jobResource)
        .setFailureThreshold(Integer.MAX_VALUE).setTaskRetryDelay(2000L)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.FAILURE_COUNT_BEFORE_SUCCESS, "2"));
    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();
    _driver.start(flow);

    // Wait until the job completes.
    _driver.pollForWorkflowState(jobResource, TaskState.COMPLETED);

    long startTime = _driver.getWorkflowContext(jobResource).getStartTime();
    long finishedTime = _driver.getWorkflowContext(jobResource).getFinishTime();

    // It should finished at least 4 secs
    Assert.assertTrue(finishedTime - startTime >= 2000L);
  }

  @Test public void testTaskRetryWithoutDelay() throws Exception {
    String jobResource = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG)
        .setMaxAttemptsPerTask(2).setCommand(MockTask.TASK_COMMAND)
        .setFailureThreshold(Integer.MAX_VALUE)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.FAILURE_COUNT_BEFORE_SUCCESS, "1"));
    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();
    _driver.start(flow);

    // Wait until the job completes.
    _driver.pollForWorkflowState(jobResource, TaskState.COMPLETED);

    long startTime = _driver.getWorkflowContext(jobResource).getStartTime();
    long finishedTime = _driver.getWorkflowContext(jobResource).getFinishTime();

    // It should finished at less than 2.5 sec
    Assert.assertTrue(finishedTime - startTime <= 2500L);
  }
}

