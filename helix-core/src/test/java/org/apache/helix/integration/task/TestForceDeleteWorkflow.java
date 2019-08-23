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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
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
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/*
This test checks the functionality of the ForceDelete in various conditions.
 */
public class TestForceDeleteWorkflow extends ZkTestBase {
  protected int _numNodes = 5;
  protected int _startPort = 12918;
  protected int _numPartitions = 20;
  protected int _numReplicas = 3;
  protected int _numDbs = 1;

  protected ClusterControllerManager _controller;
  protected HelixManager _manager;
  protected TaskDriver _driver;

  protected List<String> _testDbs = new ArrayList<>();

  protected final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  protected MockParticipantManager[] _participants;

  protected ZkHelixClusterVerifier _clusterVerifier;
  private HelixAdmin _admin;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _participants = new MockParticipantManager[_numNodes];
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    setupParticipants(_gSetupTool);
    setupDBs();

    for (int i = 0; i < _numNodes; i++) {
      startParticipant(ZK_ADDR, i);
    }

    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient).build();

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    _admin = _gSetupTool.getClusterManagementTool();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }
    for (int i = 0; i < _numNodes; i++) {
      stopParticipant(i);
    }
    deleteCluster(CLUSTER_NAME);
  }

  protected void setupDBs() {
    setupDBs(_gSetupTool);
  }

  protected void setupDBs(ClusterSetup clusterSetup) {
    // Set up target db
    boolean partitionVary = true;
    if (_numDbs > 1) {
      for (int i = 0; i < _numDbs; i++) {
        int varyNum = partitionVary ? 10 * i : 0;
        String db = WorkflowGenerator.DEFAULT_TGT_DB + i;
        clusterSetup.addResourceToCluster(CLUSTER_NAME, db, _numPartitions + varyNum,
            MASTER_SLAVE_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.toString());
        clusterSetup.rebalanceStorageCluster(CLUSTER_NAME, db, _numReplicas);
        _testDbs.add(db);
      }
    } else {
      clusterSetup.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB,
          _numPartitions, MASTER_SLAVE_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.name());
      clusterSetup.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB,
          _numReplicas);
    }
  }

  protected void setupParticipants(ClusterSetup setupTool) {
    _participants = new MockParticipantManager[_numNodes];
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
  }

  protected void startParticipant(String zkAddr, int i) {
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put(DelayedStopTask.TASK_COMMAND, DelayedStopTask::new);
    String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
    _participants[i] = new MockParticipantManager(zkAddr, CLUSTER_NAME, instanceName);

    // Register a Task state model factory.
    StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
    stateMachine.registerStateModelFactory("Task",
        new TaskStateModelFactory(_participants[i], taskFactoryReg));
    _participants[i].syncStart();
  }

  protected void stopParticipant(int i) {
    if (_participants.length <= i) {
      throw new HelixException(
          String.format("Can't stop participant %s, only %s participants" + "were set up.", i,
              _participants.length));
    }
    if (_participants[i] != null && _participants[i].isConnected()) {
      _participants[i].syncStop();
    }
  }

  @Test
  public void testDeleteCompletedWorkflowForcefully() throws Exception {
    // Create a simple workflow and wait for its completion. Then delete the IdealState,
    // WorkflowContext and WorkflowConfig.
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, "1000", "0");
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
    Workflow.Builder builder = createCustomWorkflow(workflowName, "100000", "0");
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
    // Create a simple workflow. Stop the workflow and wait for completion. Delete the workflow forcefully afterwards
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, "10000", "0");
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
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = createCustomWorkflow(workflowName, "10000", "1000000");
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
      return ((wCtx1.getWorkflowState() == TaskState.STOPPED
          || wCtx1.getWorkflowState() == TaskState.STOPPING));
    }, 60 * 1000);
    Assert.assertTrue(isWorkflowStopped);

    boolean isJobStopped = TestHelper.verify(() -> {
      WorkflowContext wCtx1 = _driver.getWorkflowContext(workflowName);
      return wCtx1.getJobState(TaskUtil.getNamespacedJobName(workflowName + "_JOB0"))==TaskState.STOPPED;
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

    // Start the Controller.
    String controllerName = CONTROLLER_PREFIX + "_2";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
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
