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

import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Test to check workflow expiry time behavior.
 */
public class TestWorkflowExpiryTime extends TaskTestBase {
  private HelixAdmin _admin;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _admin = _gSetupTool.getClusterManagementTool();
  }

  @Test
  public void testFailedWorkflowExpiryTime() throws Exception {
    final long expiryTime = 2000L;
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder1 = new Workflow.Builder(workflowName);

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(
            ImmutableMap.of(MockTask.TASK_RESULT_STATUS, TaskResult.Status.FATAL_FAILED.name()));

    builder1.addJob("JOB0", jobBuilder1);
    builder1.setExpiry(expiryTime);

    _driver.start(builder1.build());

    // Wait until workflow is Failed
    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);


    // workflowIsCleanedUp should be false and workflow should not be deleted
    boolean workflowIsCleanedUp = TestHelper.verify(() -> {
      WorkflowContext wCtx = _driver.getWorkflowContext(workflowName);
      WorkflowConfig wCfg = _driver.getWorkflowConfig(workflowName);
      IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, workflowName);
      return (wCtx == null || wCfg == null || idealState == null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertFalse(workflowIsCleanedUp);
  }

  @Test
  public void testCompletedWorkflowExpiryTime() throws Exception {
    final long expiryTime = 2000L;
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder1 = new Workflow.Builder(workflowName);

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(
            ImmutableMap.of(MockTask.TASK_RESULT_STATUS, TaskResult.Status.COMPLETED.name()));

    builder1.addJob("JOB0", jobBuilder1);
    builder1.setExpiry(expiryTime);

    _driver.start(builder1.build());

    // Wait until workflow is Completed
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    // workflowIsCleanedUp should be true and workflow should be deleted
    boolean workflowIsCleanedUp = TestHelper.verify(() -> {
      WorkflowContext wCtx = _driver.getWorkflowContext(workflowName);
      WorkflowConfig wCfg = _driver.getWorkflowConfig(workflowName);
      IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, workflowName);
      return (wCtx == null && wCfg == null && idealState == null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(workflowIsCleanedUp);
  }
}
