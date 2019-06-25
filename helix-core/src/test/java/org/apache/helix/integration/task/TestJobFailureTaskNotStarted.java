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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.statemodel.MockTaskStateModelFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskSynchronizedTestBase;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestJobFailureTaskNotStarted extends TaskSynchronizedTestBase {
  private static final String DB_NAME = WorkflowGenerator.DEFAULT_TGT_DB;
  private static final String UNBALANCED_DB_NAME = "UnbalancedDB";
  private MockParticipantManager _blockedParticipant;
  private MockParticipantManager _normalParticipant;

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants = new MockParticipantManager[_numNodes];
    _numDbs = 1;
    _numNodes = 2;
    _numPartitions = 2;
    _numReplicas = 1;

    _gSetupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    startParticipantsWithStuckTaskStateModelFactory();
    createManagers();
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX);
    _controller.syncStart();

    // Enable cancellation
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
  }

  protected void startParticipantsWithStuckTaskStateModelFactory() {

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new MockTask(context);
      }
    });

    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);

    _participants[0] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instances.get(0));
    StateMachineEngine stateMachine = _participants[0].getStateMachineEngine();
    stateMachine.registerStateModelFactory("Task",
        new MockTaskStateModelFactory(_participants[0], taskFactoryReg));
    _participants[0].syncStart();
    _blockedParticipant = _participants[0];

    _participants[1] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instances.get(1));
    stateMachine = _participants[1].getStateMachineEngine();
    stateMachine.registerStateModelFactory("Task",
        new TaskStateModelFactory(_participants[1], taskFactoryReg));
    _participants[1].syncStart();
    _normalParticipant = _participants[1];
  }

  @Test
  public void testTaskNotStarted() throws InterruptedException {
    setupUnbalancedDB();

    final String BLOCK_WORKFLOW_NAME = "blockWorkflow";
    final String FAIL_WORKFLOW_NAME = "failWorkflow";

    final String FAIL_JOB_NAME = "failJob";

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    final int numTask =
        configAccessor.getClusterConfig(CLUSTER_NAME).getMaxConcurrentTaskPerInstance();

    // Tasks targeting the unbalanced DB, the instance is setup to stuck on INIT->RUNNING, so it
    // takes all threads
    // on that instance.
    JobConfig.Builder blockJobBuilder = new JobConfig.Builder().setWorkflow(BLOCK_WORKFLOW_NAME)
        .setTargetResource(UNBALANCED_DB_NAME)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setCommand(MockTask.TASK_COMMAND).setNumConcurrentTasksPerInstance(numTask);

    Workflow.Builder blockWorkflowBuilder =
        new Workflow.Builder(BLOCK_WORKFLOW_NAME).addJob("blockJob", blockJobBuilder);
    _driver.start(blockWorkflowBuilder.build());

    Assert.assertTrue(TaskTestUtil.pollForAllTasksBlock(_manager.getHelixDataAccessor(),
        _blockedParticipant.getInstanceName(), numTask, 10000));

    // Now, all HelixTask threads are stuck at INIT->RUNNING for task state transition(user task
    // can't be submitted)
    // New tasks assigned to the instance won't start INIT->RUNNING transition at all.

    // A to-be-failed job, 2 tasks, 1 stuck and 1 fail, making the job fail.
    JobConfig.Builder failJobBuilder =
        new JobConfig.Builder().setWorkflow(FAIL_WORKFLOW_NAME).setTargetResource(DB_NAME)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND).setJobCommandConfigMap(
                ImmutableMap.of(MockTask.TASK_RESULT_STATUS, TaskResult.Status.FAILED.name()));

    Workflow.Builder failWorkflowBuilder =
        new Workflow.Builder(FAIL_WORKFLOW_NAME).addJob(FAIL_JOB_NAME, failJobBuilder);

    _driver.start(failWorkflowBuilder.build());

    _driver.pollForJobState(FAIL_WORKFLOW_NAME,
        TaskUtil.getNamespacedJobName(FAIL_WORKFLOW_NAME, FAIL_JOB_NAME), TaskState.FAILED);
    _driver.pollForWorkflowState(FAIL_WORKFLOW_NAME, TaskState.FAILED);

    JobContext jobContext =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(FAIL_WORKFLOW_NAME, FAIL_JOB_NAME));
    for (int pId : jobContext.getPartitionSet()) {
      String assignedParticipant = jobContext.getAssignedParticipant(pId);
      if (assignedParticipant == null) {
        continue; // May not have been assigned at all due to quota limitations
      }
      if (jobContext.getAssignedParticipant(pId).equals(_blockedParticipant.getInstanceName())) {
        Assert.assertEquals(jobContext.getPartitionState(pId), TaskPartitionState.TASK_ABORTED);
      } else if (assignedParticipant.equals(_normalParticipant.getInstanceName())) {
        Assert.assertEquals(jobContext.getPartitionState(pId), TaskPartitionState.TASK_ERROR);
      } else {
        throw new HelixException("There should be only 2 instances, 1 blocked, 1 normal.");
      }
    }
  }

  private void setupUnbalancedDB() throws InterruptedException {
    // Start with Full-Auto mode to create the partitions, Semi-Auto won't create partitions.
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, UNBALANCED_DB_NAME, 50, MASTER_SLAVE_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, UNBALANCED_DB_NAME, 1);

    // Set preference list to put all partitions to one instance.
    IdealState idealState = _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, UNBALANCED_DB_NAME);
    Set<String> partitions = idealState.getPartitionSet();
    for (String partition : partitions) {
      idealState.setPreferenceList(partition,
          Lists.newArrayList(_blockedParticipant.getInstanceName()));
    }
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);

    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, UNBALANCED_DB_NAME,
        idealState);

    Assert.assertTrue(_clusterVerifier.verifyByPolling(10000, 100));
  }
}
