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
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Test To check if the Execution Delay is respected.
 */
public class TestStopWorkflowWithExecutionDelay extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
  }

  @Test
  public void testStopWorkflowWithExecutionDelay() throws Exception {
    // Execution Delay is set to be 20 milliseconds.
    final long executionDelay = 20L;
    // Timeout per task has been set to be a large number.
    final long timeout = 60000L;
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    // Workflow DAG Schematic:
    //          JOB0
    //           /\
    //          /  \
    //         /    \
    //       JOB1   JOB2

    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(timeout).setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(timeout).setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    JobConfig.Builder jobBuilder3 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(timeout).setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addParentChildDependency("JOB0", "JOB2");
    builder.addJob("JOB0", jobBuilder.setExecutionDelay(executionDelay));
    builder.addJob("JOB1", jobBuilder2);
    builder.addJob("JOB2", jobBuilder3);

    _driver.start(builder.build());

    // Wait until Workflow Context is created. and running.
    _driver.pollForWorkflowState(workflowName, TaskState.IN_PROGRESS);

    // Check the Job0 is running.
    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, "JOB0"),
        TaskState.IN_PROGRESS);

    // Stop the workflow
    _driver.stop(workflowName);

    _driver.pollForWorkflowState(workflowName, TaskState.STOPPED);
  }
}
