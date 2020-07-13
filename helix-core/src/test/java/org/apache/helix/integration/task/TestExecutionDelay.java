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
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test To check if the Execution Delay is respected.
 */
public class TestExecutionDelay extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
  }

  @Test
  public void testExecutionDelay() throws Exception {
    // Execution Delay is set to be a large number. Hence, the workflow should not be completed
    // soon.
    final long executionDelay = 100000000000L;
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    // Workflow DAG Schematic:
    //          JOB0
    //           /\
    //          /  \
    //         /    \
    //       JOB1   JOB2

    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    JobConfig.Builder jobBuilder3 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addParentChildDependency("JOB0", "JOB2");
    builder.addJob("JOB0", jobBuilder.setExecutionDelay(executionDelay));
    builder.addJob("JOB1", jobBuilder2);
    builder.addJob("JOB2", jobBuilder3);

    // Since the execution delay is set as a long time, it is expected to reach time out.
    // If the workflow completed without any exception, it means that ExecutionDelay has not been
    // respected.
    _driver.start(builder.build());

    // Verify workflow Context is created.
    // Wait until Workflow Context is created.
    boolean workflowCreated = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      return wCtx1 != null;
    }, 30 * 1000);
    Assert.assertTrue(workflowCreated);

    // Verify StartTime is set for the Job0.
    // Wait until startTime is set.
    boolean startTimeSet = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      long startTime = -1;
      try {
        startTime = wCtx1.getJobStartTime(TaskUtil.getNamespacedJobName(workflowName, "JOB0"));
      } catch (Exception e) {
        // pass
      }
      return startTime != -1;
    }, 30 * 1000);
    Assert.assertTrue(startTimeSet);

    // Verify JobContext is not created. Creation of the JobContext means that the job is ran or
    // in-progress. The absence of JobContext implies that the job did not run.
    boolean isJobStarted = TestHelper.verify(() -> {
      JobContext jobCtx0 =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "JOB0"));
      JobContext jobCtx1 =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "JOB1"));
      JobContext jobCtx2 =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "JOB2"));
      return (jobCtx0 != null || jobCtx1 != null || jobCtx2 != null);
    }, 30 * 1000);
    Assert.assertFalse(isJobStarted);

    WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
    Assert.assertNull(wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName, "JOB0")));
    Assert.assertNull(wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName, "JOB1")));
    Assert.assertNull(wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName, "JOB2")));
  }
}
