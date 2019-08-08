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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/*
This test checks the functionality of the forcedelete in various conditions.
 */

public class TestForceDeleteWorkflow extends TaskTestBase {
  private HelixAdmin admin;
  // Delay considered for force deletion and delay considered for disableling the  cluster.
  private static final int DELETE_DELAY = 2000;
  private long Disable_Delay = 1000L;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    admin = _gSetupTool.getClusterManagementTool();
    createManagers();

  }

  @Test
  public void testDeleteCompletedWorkflowForcefully() throws Exception {
    // Create a simple workflow and wait for its completion. Then delete the IdealState,
    // WorkflowContext and WorkflowConfig.
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    //Workflow Schematic:
    //               JOB0
    //                /\
    //               /  \
    //             JOB1 JOB2

    JobConfig.Builder jobBuilder0 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addParentChildDependency("JOB0", "JOB2");
    builder.addJob("JOB0", jobBuilder0);
    builder.addJob("JOB1", jobBuilder1);
    builder.addJob("JOB2", jobBuilder2);

    _driver.start(builder.build());

    // Wait until workflow is created and completed.
    boolean isWorkflowCompleted = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      return (wCtx1 != null && wCtx1.getWorkflowState() == TaskState.COMPLETED);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowCompleted);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this
    // workflow
    Assert.assertNotNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNotNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNotNull(admin.getResourceIdealState(CLUSTER_NAME, workflowName));

    // Disable the cluster before Force Delete
    admin.enableCluster(CLUSTER_NAME, false);
    Thread.sleep(Disable_Delay);

    _driver.delete(workflowName, true);
    Thread.sleep(DELETE_DELAY);

    // Enable the cluster before Force Delete
    admin.enableCluster(CLUSTER_NAME, true);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed deleted for this
    // workflow
    Assert.assertNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNull(admin.getResourceIdealState(CLUSTER_NAME, workflowName));
  }

  @Test
  public void testDeleteRunningWorkflowForcefully() throws Exception {
    // Create a simple workflow and wait until it reaches the Running state. Then delete the
    // IdealState,
    // WorkflowContext and WorkflowConfig.
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    //Workflow Schematic:
    //               JOB0
    //                /\
    //               /  \
    //             JOB1 JOB2

    JobConfig.Builder jobBuilder0 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addParentChildDependency("JOB0", "JOB2");
    builder.addJob("JOB0", jobBuilder0);
    builder.addJob("JOB1", jobBuilder1);
    builder.addJob("JOB2", jobBuilder2);

    _driver.start(builder.build());

    // Wait until workflow is created and goes to running stage.
    boolean isWorkflowRunning = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      return (wCtx1 != null && wCtx1.getWorkflowState() == TaskState.IN_PROGRESS);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowRunning);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this
    // workflow
    Assert.assertNotNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNotNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNotNull(admin.getResourceIdealState(CLUSTER_NAME, workflowName));

    // Disable the cluster before Force Delete
    admin.enableCluster(CLUSTER_NAME, false);
    Thread.sleep(Disable_Delay);

    _driver.delete(workflowName, true);
    Thread.sleep(DELETE_DELAY);

    // Enable the cluster before Force Delete
    admin.enableCluster(CLUSTER_NAME, true);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed deleted for this
    // workflow
    Assert.assertNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNull(admin.getResourceIdealState(CLUSTER_NAME, workflowName));
  }

  @Test
  public void testDeleteStoppedWorkflowForcefully() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    //Workflow Schematic:
    //               JOB0
    //                /\
    //               /  \
    //             JOB1 JOB2

    JobConfig.Builder jobBuilder0 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addParentChildDependency("JOB0", "JOB2");
    builder.addJob("JOB0", jobBuilder0);
    builder.addJob("JOB1", jobBuilder1);
    builder.addJob("JOB2", jobBuilder2);

    _driver.start(builder.build());
    // Wait until workflow is created.
    boolean isWorkflowCreated = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      return (wCtx1 != null);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowCreated);

    _driver.stop(workflowName);
    // Wait until workflow is stopped.
    boolean isWorkflowStopped = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      return (wCtx1.getWorkflowState() == TaskState.STOPPED);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowStopped);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this
    // workflow
    Assert.assertNotNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNotNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNotNull(admin.getResourceIdealState(CLUSTER_NAME, workflowName));

    // Disable the cluster before Force Delete
    admin.enableCluster(CLUSTER_NAME, false);
    Thread.sleep(Disable_Delay);

    _driver.delete(workflowName, true);
    Thread.sleep(DELETE_DELAY);

    // Enable the cluster before Force Delete
    admin.enableCluster(CLUSTER_NAME, true);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed deleted for this
    // workflow
    Assert.assertNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNull(admin.getResourceIdealState(CLUSTER_NAME, workflowName));
  }

  @Test
  public void testDeleteNotStartedWorkflowForcefully() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    //Workflow Schematic:
    //               JOB0
    //                /\
    //               /  \
    //             JOB1 JOB2

    JobConfig.Builder jobBuilder0 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    // Execution delay if set to be large number in order to have enough time to change the status
    // of the workflow to not started.
    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addParentChildDependency("JOB0", "JOB2");
    builder.addJob("JOB0", jobBuilder0.setExecutionDelay(10000000000L));
    builder.addJob("JOB1", jobBuilder1);
    builder.addJob("JOB2", jobBuilder2);

    _driver.start(builder.build());
    // Wait until workflow is created.
    boolean isWorkflowCreated = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      return (wCtx1 != null);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowCreated);

    // Change the workflow state to Not-Started and set its start time of the workflow.
    long startTime = System.currentTimeMillis() + 100000000L;
    WorkflowContext workflowContext = TaskTestUtil.buildWorkflowContext(workflowName,
        TaskState.NOT_STARTED, startTime, TaskState.NOT_STARTED);
    setWorkflowContext(_manager, workflowName, workflowContext);

    // Wait until workflow is stopped.
    boolean isWorkflowInNotStartedState = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      return (wCtx1.getWorkflowState() == TaskState.NOT_STARTED);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowInNotStartedState);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this
    // workflow
    Assert.assertNotNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNotNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNotNull(admin.getResourceIdealState(CLUSTER_NAME, workflowName));

    // Disable the cluster before Force Delete
    admin.enableCluster(CLUSTER_NAME, false);
    Thread.sleep(Disable_Delay);

    _driver.delete(workflowName, true);
    Thread.sleep(DELETE_DELAY);

    // Enable the cluster before Force Delete
    admin.enableCluster(CLUSTER_NAME, true);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed deleted for this
    // workflow
    Assert.assertNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNull(admin.getResourceIdealState(CLUSTER_NAME, workflowName));

  }

  private void setWorkflowContext(HelixManager manager, String workflow,
      WorkflowContext workflowContext) {
    String CONTEXT_NODE = "Context";
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, workflow, CONTEXT_NODE),
        workflowContext.getRecord(), AccessOption.PERSISTENT);
  }
}
