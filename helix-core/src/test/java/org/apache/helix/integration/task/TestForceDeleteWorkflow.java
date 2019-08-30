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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.HelixAdmin;
import org.apache.helix.task.TaskUtil;
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
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/*
This test checks the functionality of the ForceDelete in various conditions.
 */
public class TestForceDeleteWorkflow extends TaskTestBase {
  private static final long LONG_TIMEOUT = 200000L;
  // Three types of execution times have been considered in this test.
  private static final String SHORT_EXECUTION_TIME = "1000";
  private static final String MEDIUM_EXECUTION_TIME = "10000";
  private static final String LONG_EXECUTION_TIME = "100000";
  // Long delay to simulate the tasks that do not stop.
  private static final String STOP_DELAY = "1000000";
  private HelixAdmin _admin;
  protected HelixManager _manager;
  protected TaskDriver _driver;

  // These AtomicIntegers are used to check tasks are stuck in Task.cancel().
  private static final AtomicInteger CANCEL_COUNT = new AtomicInteger(0);
  private static final AtomicInteger STOP_COUNT = new AtomicInteger(0);

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    // Stop participants that have been started in super class
    for (int i = 0; i < _numNodes; i++) {
      super.stopParticipant(i);
    }

    // Check that participants are actually stopped
    for (int i = 0; i < _numNodes; i++) {
      Assert.assertFalse(_participants[i].isConnected());
    }

    // Start new participants that have new TaskStateModel (DelayedStopTask) information
    _participants = new MockParticipantManager[_numNodes];
    for (int i = 0; i < _numNodes; i++) {
      Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
      taskFactoryReg.put(DelayedStopTask.TASK_COMMAND, DelayedStopTask::new);
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
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
    // WorkflowContext and WorkflowConfig using ForceDelete.
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, SHORT_EXECUTION_TIME, "0");
    _driver.start(builder.build());

    // Wait until workflow is created and completed.
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

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
  }

  @Test(dependsOnMethods = "testDeleteCompletedWorkflowForcefully")
  public void testDeleteRunningWorkflowForcefully() throws Exception {
    // Create a simple workflow and wait until it reaches the Running state. Then delete the
    // IdealState, WorkflowContext and WorkflowConfig using ForceDelete.

    // Start the Controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, LONG_EXECUTION_TIME, "0");
    _driver.start(builder.build());

    // Wait until workflow is created and running.
    _driver.pollForWorkflowState(workflowName, TaskState.IN_PROGRESS);

    // Check the status of JOB0 to make sure it is running.
    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, "JOB0"),
        TaskState.IN_PROGRESS);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this
    // workflow
    Assert.assertNotNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNotNull(_driver.getWorkflowContext(workflowName));
    Assert.assertNotNull(_admin.getResourceIdealState(CLUSTER_NAME, workflowName));

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
  }

  @Test(dependsOnMethods = "testDeleteRunningWorkflowForcefully")
  public void testDeleteStoppedWorkflowForcefully() throws Exception {
    // Create a simple workflow. Stop the workflow and wait until it's fully stopped. Then delete
    // the IdealState, WorkflowContext and WorkflowConfig using ForceDelete.

    // Start the Controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, MEDIUM_EXECUTION_TIME, "0");
    _driver.start(builder.build());

    // Wait until workflow is created and running.
    _driver.pollForWorkflowState(workflowName, TaskState.IN_PROGRESS);

    _driver.stop(workflowName);

    // Wait until workflow is stopped.
    _driver.pollForWorkflowState(workflowName, TaskState.STOPPED);

    // Wait until workflow is stopped. Also, jobs should be either stopped or not created (null).
    boolean areJobsStopped = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      TaskState job0 = wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName, "JOB0"));
      TaskState job1 = wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName, "JOB1"));
      TaskState job2 = wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName, "JOB2"));
      return ((job0 == null || job0 == TaskState.STOPPED)
          && (job1 == null || job1 == TaskState.STOPPED)
          && (job2 == null || job2 == TaskState.STOPPED));
    }, 60 * 1000);
    Assert.assertTrue(areJobsStopped);

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
  }

  @Test(dependsOnMethods = "testDeleteStoppedWorkflowForcefully")
  public void testDeleteStoppingStuckWorkflowForcefully() throws Exception {
    // In this test, Stuck workflow indicates a workflow that is in STOPPING state (user requested
    // to stop the workflow), its JOBs are also in STOPPING state but the tasks are stuck (or taking
    // a long time to stop) in RUNNING state.

    // Reset the cancel and stop counts
    CANCEL_COUNT.set(0);
    STOP_COUNT.set(0);

    // Start the Controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, LONG_EXECUTION_TIME, STOP_DELAY);
    _driver.start(builder.build());

    // Wait until workflow is created and running.
    _driver.pollForWorkflowState(workflowName, TaskState.IN_PROGRESS);

    _driver.stop(workflowName);

    // Wait until workflow is is in Stopping state.
    _driver.pollForWorkflowState(workflowName, TaskState.STOPPING);

    // Check the status of JOB0 to make sure it is stopping.
    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, "JOB0"),
        TaskState.STOPPING);

    // Since ConcurrentTasksPerInstance is set to be 1, the total number of CANCEL_COUNT is expected
    // to be equal to number of nodes
    boolean haveAllCancelsCalled = TestHelper.verify(() -> {
      return CANCEL_COUNT.get() == _numNodes;
    }, 60 * 1000);
    Assert.assertTrue(haveAllCancelsCalled);

    // Check that STOP_COUNT is 0. This checks that no tasks' Task.cancel() have returned, verifying
    // that they are indeed stuck.
    Assert.assertEquals(STOP_COUNT.get(), 0);

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
    // Workflow Schematic:
    //               JOB0
    //                /\
    //               /  \
    //             JOB1 JOB2

    JobConfig.Builder jobBuilder0 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(LONG_TIMEOUT).setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, executionTime,
            DelayedStopTask.JOB_DELAY_CANCEL, stopDelay));

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(LONG_TIMEOUT).setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, executionTime,
            DelayedStopTask.JOB_DELAY_CANCEL, stopDelay));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(LONG_TIMEOUT).setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, executionTime,
            DelayedStopTask.JOB_DELAY_CANCEL, stopDelay));

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
    private static final String JOB_DELAY_CANCEL = "DelayCancel";
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
      // Increment the cancel count so we know cancel() has been called
      CANCEL_COUNT.incrementAndGet();

      try {
        Thread.sleep(_delayCancel);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      super.cancel();

      // Increment the stop count so we know cancel() has finished and returned
      STOP_COUNT.incrementAndGet();
    }
  }
}
