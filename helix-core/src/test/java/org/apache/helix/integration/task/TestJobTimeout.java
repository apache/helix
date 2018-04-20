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
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskSynchronizedTestBase;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public final class TestJobTimeout extends TaskSynchronizedTestBase {

  private ClusterControllerManager _controller;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 2;
    _numParitions = 2;
    _numReplicas = 1; // only Master, no Slave
    _numDbs = 1;
    _participants =  new MockParticipantManager[_numNodes];
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursively(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    startParticipants();
    createManagers();
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX);
    _controller.syncStart();
  }

  @Test
  public void testTaskRunningIndefinitely() throws InterruptedException {
    // first job runs indefinitely and timeout, the second job runs successfully, the workflow succeed.
    final String FIRST_JOB = "first_job";
    final String SECOND_JOB = "second_job";
    final String WORKFLOW_NAME = TestHelper.getTestMethodName();
    final String DB_NAME = WorkflowGenerator.DEFAULT_TGT_DB;

    JobConfig.Builder firstJobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW_NAME)
        .setTargetResource(DB_NAME)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999")) // task stuck
        .setTimeout(1000);

    JobConfig.Builder secondJobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW_NAME)
        .setTargetResource(DB_NAME)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setCommand(MockTask.TASK_COMMAND)
        .setIgnoreDependentJobFailure(true); // ignore first job's timeout

    WorkflowConfig.Builder workflowConfigBuilder = new WorkflowConfig.Builder(WORKFLOW_NAME)
        .setFailureThreshold(
            1); // workflow ignores first job's timeout and schedule second job and succeed.

    Workflow.Builder workflowBuilder = new Workflow.Builder(WORKFLOW_NAME)
        .setWorkflowConfig(workflowConfigBuilder.build())
        .addJob(FIRST_JOB, firstJobBuilder)
        .addJob(SECOND_JOB, secondJobBuilder)
        .addParentChildDependency(FIRST_JOB, SECOND_JOB);

    _driver.start(workflowBuilder.build());

    _driver.pollForJobState(WORKFLOW_NAME, TaskUtil.getNamespacedJobName(WORKFLOW_NAME, FIRST_JOB),
        TaskState.TIMED_OUT);
    _driver.pollForJobState(WORKFLOW_NAME, TaskUtil.getNamespacedJobName(WORKFLOW_NAME, SECOND_JOB),
        TaskState.COMPLETED);
    _driver.pollForWorkflowState(WORKFLOW_NAME, TaskState.COMPLETED);

    JobContext jobContext = _driver.getJobContext(TaskUtil.getNamespacedJobName(WORKFLOW_NAME, FIRST_JOB));
    for (int pId : jobContext.getPartitionSet()) {
      // All tasks aborted because of job timeout
      Assert.assertEquals(jobContext.getPartitionState(pId), TaskPartitionState.TASK_ABORTED);
    }
  }

  @Test
  public void testNoSlaveToRunTask() throws InterruptedException {
    // first job can't be assigned to any instance and stuck, timeout, second job runs and workflow succeed.
    final String FIRST_JOB = "first_job";
    final String SECOND_JOB = "second_job";
    final String WORKFLOW_NAME = TestHelper.getTestMethodName();
    final String DB_NAME = WorkflowGenerator.DEFAULT_TGT_DB;

    JobConfig.Builder firstJobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW_NAME)
        .setTargetResource(DB_NAME)
        // since replica=1, there's no Slave, but target only Slave, the task can't be assigned, job stuck and timeout
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.SLAVE.name()))
        .setCommand(MockTask.TASK_COMMAND)
        .setTimeout(1000);

    JobConfig.Builder secondJobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW_NAME)
        .setTargetResource(DB_NAME)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setCommand(MockTask.TASK_COMMAND)
        .setIgnoreDependentJobFailure(true); // ignore first job's timeout

    WorkflowConfig.Builder workflowConfigBuilder = new WorkflowConfig.Builder(WORKFLOW_NAME)
        .setFailureThreshold(
            1); // workflow ignores first job's timeout and schedule second job and succeed.

    Workflow.Builder workflowBuilder = new Workflow.Builder(WORKFLOW_NAME)
        .setWorkflowConfig(workflowConfigBuilder.build())
        .addJob(FIRST_JOB, firstJobBuilder)
        .addJob(SECOND_JOB, secondJobBuilder)
        .addParentChildDependency(FIRST_JOB, SECOND_JOB);

    _driver.start(workflowBuilder.build());

    _driver.pollForJobState(WORKFLOW_NAME, TaskUtil.getNamespacedJobName(WORKFLOW_NAME, FIRST_JOB),
        TaskState.TIMED_OUT);
    _driver.pollForJobState(WORKFLOW_NAME, TaskUtil.getNamespacedJobName(WORKFLOW_NAME, SECOND_JOB),
        TaskState.COMPLETED);
    _driver.pollForWorkflowState(WORKFLOW_NAME, TaskState.COMPLETED);

    JobContext jobContext = _driver.getJobContext(TaskUtil.getNamespacedJobName(WORKFLOW_NAME, FIRST_JOB));
    for (int pId : jobContext.getPartitionSet()) {
      // No task assigned for first job
      Assert.assertEquals(jobContext.getPartitionState(pId), null);
    }
  }
}
