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
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/*
This test checks the functionality of the ForceDelete in various conditions.
 */
public class TestForceDeleteWorkflow extends TaskTestBase {
  // Three types of execution times have been considered in this test.
  private static final String SHORT_EXECUTION_TIME = "1000";
  private static final String MEDIUM_EXECUTION_TIME = "10000";
  private static final String LONG_EXECUTION_TIME = "100000";
  // Long delay to simulate the tasks that do not stop.
  private static final String STOP_DELAY = "1000000";
  private HelixAdmin _admin;
  protected HelixManager _manager;
  protected TaskDriver _driver;
  protected MockParticipantManager[] _participantsNew;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    // Stop participants that have been started in super class
    for (int i = 0; i < _numNodes; i++) {
      super.stopParticipant(i);
    }

    // Start new participants that have new TaskStateModel (DelayedStopTask) information
    _participantsNew = new MockParticipantManager[_numNodes];
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_NEW_" + (_startPort + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    // Start participants that have DelayedStopTask Transition Model
    for (int i = 0; i < _numNodes; i++) {
      Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
      taskFactoryReg.put(DelayedStopTask.TASK_COMMAND, DelayedStopTask::new);
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participantsNew[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participantsNew[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participantsNew[i], taskFactoryReg));
      _participantsNew[i].syncStart();
    }

    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    _driver = new TaskDriver(_manager);

    _admin = _gSetupTool.getClusterManagementTool();
  }

  @Test
  public void testDeleteCompletedWorkflowForcefully() throws Exception {
    // Create a simple workflow and wait for its completion. Then delete the IdealState,
    // WorkflowContext and WorkflowConfig.
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, SHORT_EXECUTION_TIME, "0");
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
    Assert.assertNotNull(_admin.getResourceIdealState(CLUSTER_NAME, workflowName));

    // Stop the Controller
    _controller.syncStop();

    _driver.delete(workflowName, true);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed deleted for this
    // workflow.
    boolean isWorkflowDeleted = TestHelper.verify(() -> {
      WorkflowConfig wfcfg = _driver.getWorkflowConfig(workflowName);
      WorkflowContext wfctx = _driver.getWorkflowContext(workflowName);
      IdealState is = _admin.getResourceIdealState(CLUSTER_NAME, workflowName);
      return (wfcfg == null && wfctx == null && is == null);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowDeleted);

    // Start the Controller
    String controllerName = CONTROLLER_PREFIX + "_1";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
  }

  @Test(dependsOnMethods = "testDeleteCompletedWorkflowForcefully")
  public void testDeleteRunningWorkflowForcefully() throws Exception {
    // Create a simple workflow and wait until it reaches the Running state. Then delete the
    // IdealState, WorkflowContext and WorkflowConfig.
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, LONG_EXECUTION_TIME, "0");
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
    Assert.assertNotNull(_admin.getResourceIdealState(CLUSTER_NAME, workflowName));

    // Check if the Job is running
    boolean isJobRunning = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      TaskState job0 = wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName, "JOB0"));
      TaskState job1 = wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName, "JOB1"));
      TaskState job2 = wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName, "JOB2"));
      return (wCtx1 != null && (job0 == TaskState.IN_PROGRESS || job1 == TaskState.IN_PROGRESS
          || job2 == TaskState.IN_PROGRESS));
    }, 60 * 1000);
    Assert.assertTrue(isJobRunning);

    // Stop the Controller
    _controller.syncStop();

    _driver.delete(workflowName, true);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed deleted for this
    // workflow
    boolean isWorkflowDeleted = TestHelper.verify(() -> {
      WorkflowConfig wfcfg = _driver.getWorkflowConfig(workflowName);
      WorkflowContext wfctx = _driver.getWorkflowContext(workflowName);
      IdealState is = _admin.getResourceIdealState(CLUSTER_NAME, workflowName);
      return (wfcfg == null && wfctx == null && is == null);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowDeleted);

    // Start the Controller
    String controllerName = CONTROLLER_PREFIX + "_2";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
  }

  @Test(dependsOnMethods = "testDeleteRunningWorkflowForcefully")
  public void testDeleteStoppedWorkflowForcefully() throws Exception {
    // Create a simple workflow. Stop the workflow and wait for completion. Delete the workflow
    // forcefully afterwards
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, MEDIUM_EXECUTION_TIME, "0");
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
    // workflow.
    Assert.assertNotNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNotNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNotNull(_admin.getResourceIdealState(CLUSTER_NAME, workflowName));

    // Stop the Controller
    _controller.syncStop();

    _driver.delete(workflowName, true);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed deleted for this
    // workflow.
    boolean isWorkflowDeleted = TestHelper.verify(() -> {
      WorkflowConfig wfcfg = _driver.getWorkflowConfig(workflowName);
      WorkflowContext wfctx = _driver.getWorkflowContext(workflowName);
      IdealState is = _admin.getResourceIdealState(CLUSTER_NAME, workflowName);
      return (wfcfg == null && wfctx == null && is == null);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowDeleted);

    // Start the Controller
    String controllerName = CONTROLLER_PREFIX + "_3";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
  }

  @Test(dependsOnMethods = "testDeleteStoppedWorkflowForcefully")
  public void testDeleteStoppingStuckWorkflowForcefully() throws Exception {
    // In this test, Stuck workflow indicates a workflow that is in Stopping state (user requested
    // to stop the workflow) but its tasks are stuck (or taking a long time to stop) in RUNNING
    // state.
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder =
        createCustomWorkflow(workflowName, MEDIUM_EXECUTION_TIME, STOP_DELAY);
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
      return wCtx1.getWorkflowState() == TaskState.STOPPING;
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowStopped);

    boolean isJobStopped = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      return wCtx1
          .getJobState(TaskUtil.getNamespacedJobName(workflowName + "_JOB0")) == TaskState.STOPPED;
    }, 60 * 1000);
    Assert.assertFalse(isJobStopped);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this
    // workflow.
    Assert.assertNotNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNotNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNotNull(_admin.getResourceIdealState(CLUSTER_NAME, workflowName));

    // Stop the Controller.
    _controller.syncStop();

    _driver.delete(workflowName, true);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed deleted for this
    // workflow.
    boolean isWorkflowDeleted = TestHelper.verify(() -> {
      WorkflowConfig wfcfg = _driver.getWorkflowConfig(workflowName);
      WorkflowContext wfctx = _driver.getWorkflowContext(workflowName);
      IdealState is = _admin.getResourceIdealState(CLUSTER_NAME, workflowName);
      return (wfcfg == null && wfctx == null && is == null);
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowDeleted);
  }

  private Workflow.Builder createCustomWorkflow(String workflowName, String executionTime,
      String stopDelay) {
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    //Workflow Schematic:
    //               JOB0
    //                /\
    //               /  \
    //             JOB1 JOB2

    JobConfig.Builder jobBuilder0 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(DelayedStopTask.JOB_DELAY, executionTime))
        .setJobCommandConfigMap(ImmutableMap.of(DelayedStopTask.JOB_DELAY_CANCEL, stopDelay));

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(DelayedStopTask.JOB_DELAY, executionTime))
        .setJobCommandConfigMap(ImmutableMap.of(DelayedStopTask.JOB_DELAY_CANCEL, stopDelay));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, executionTime))
        .setJobCommandConfigMap(ImmutableMap.of(DelayedStopTask.JOB_DELAY_CANCEL, stopDelay));

    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addParentChildDependency("JOB0", "JOB2");

    builder.addJob("JOB0", jobBuilder0);
    builder.addJob("JOB1", jobBuilder1);
    builder.addJob("JOB2", jobBuilder2);

    return builder;
  }

  /**
   * A mock task that extents MockTask class and delays cancellation of the tasks.
   */
  private class DelayedStopTask extends MockTask {
    public static final String JOB_DELAY_CANCEL = "Delay";
    private long _delayCancel;

    DelayedStopTask(TaskCallbackContext context) {
      super(context);
      Map<String, String> cfg = context.getJobConfig().getJobCommandConfigMap();
      if (cfg == null) {
        cfg = new HashMap<>();
      }
      _delayCancel =
          cfg.containsKey(JOB_DELAY_CANCEL) ? Long.parseLong(cfg.get(JOB_DELAY_CANCEL)) : 0L;
    }

    @Override
    public void cancel() {
      try {
        Thread.sleep(_delayCancel);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      super.cancel();
    }
  }
}
