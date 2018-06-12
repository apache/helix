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
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskSynchronizedTestBase;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public final class TestRebalanceRunningTask extends TaskSynchronizedTestBase {
  private final String JOB = "test_job";
  private String WORKFLOW;
  private final String DATABASE = WorkflowGenerator.DEFAULT_TGT_DB;
  private final int _initialNumNodes = 1;

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants =  new MockParticipantManager[_numNodes];
    _numNodes = 2;
    _numParitions = 2;
    _numReplicas = 1; // only Master, no Slave
    _numDbs = 1;

    _gSetupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();

    createManagers();
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX);
    _controller.syncStart();
  }

  @BeforeMethod
  public void beforeMethod() throws InterruptedException {
    startParticipants(_initialNumNodes);
    Thread.sleep(1000);
  }

  @AfterMethod
  public void afterMethod() {
    stopParticipants();
    MockTask._signalFail = false;
  }

  private boolean checkTasksOnDifferentInstances() {
    return new TaskTestUtil.Poller() {
      @Override
      public boolean check() {
        try {
          return getNumOfInstances() > 1;
        } catch (NullPointerException e) {
          return false;
        }
      }
    }.poll();
  }

  private boolean checkTasksOnSameInstances() {
    return new TaskTestUtil.Poller() {
      @Override
      public boolean check() {
        try {
          return getNumOfInstances() == 1;
        } catch (NullPointerException e) {
          return false;
        }
      }
    }.poll();
  }

  private int getNumOfInstances() {
    JobContext jobContext = _driver.getJobContext(TaskUtil.getNamespacedJobName(WORKFLOW, JOB));
    Set<String> instances = new HashSet<String>();
    for (int pId : jobContext.getPartitionSet()) {
      instances.add(jobContext.getAssignedParticipant(pId));
    }
    return instances.size();
  }

  /**
   * Task type: generic
   * Rebalance raunning task: disabled
   * Story: 1 node is down
   */
  @Test
  public void testGenericTaskAndDisabledRebalanceAndNodeDown() throws InterruptedException {
    WORKFLOW = TestHelper.getTestMethodName();
    startParticipant(_initialNumNodes);

    JobConfig.Builder jobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW)
        .setNumberOfTasks(10) // should be enough for consistent hashing to place tasks on
        // different instances
        .setNumConcurrentTasksPerInstance(100)
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999")); // task stuck

    Workflow.Builder workflowBuilder = new Workflow.Builder(WORKFLOW)
        .addJob(JOB, jobBuilder);

    _driver.start(workflowBuilder.build());

    Assert.assertTrue(checkTasksOnDifferentInstances());
    // Stop a participant, tasks rebalanced to the same instance
    stopParticipant(_initialNumNodes);
    Assert.assertTrue(checkTasksOnSameInstances());
  }

  /**
   * Task type: generic
   * Rebalance raunning task: disabled
   * Story: new node added, then current task fails
   */
  @Test
  public void testGenericTaskAndDisabledRebalanceAndNodeAddedAndTaskFail() throws InterruptedException {
    WORKFLOW = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW)
        .setNumberOfTasks(10)
        .setNumConcurrentTasksPerInstance(100)
        .setCommand(MockTask.TASK_COMMAND)
        .setFailureThreshold(10)
        .setMaxAttemptsPerTask(2)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999")); // task stuck

    Workflow.Builder workflowBuilder = new Workflow.Builder(WORKFLOW)
        .addJob(JOB, jobBuilder);

    _driver.start(workflowBuilder.build());

    // All tasks stuck on the same instance
    Assert.assertTrue(checkTasksOnSameInstances());
    // Add a new instance
    startParticipant(_initialNumNodes);
    Thread.sleep(3000);
    // All tasks still stuck on the same instance, because RebalanceRunningTask is disabled
    Assert.assertTrue(checkTasksOnSameInstances());
    // Signal to fail all tasks
    MockTask._signalFail = true;
    // After fail, some task will be re-assigned to the new node.
    // This doesn't require RebalanceRunningTask to be enabled
    Assert.assertTrue(checkTasksOnDifferentInstances());
  }

  /**
   * Task type: generic
   * Rebalance raunning task: enabled
   * Story: new node added
   */
  @Test
  public void testGenericTaskAndEnabledRebalanceAndNodeAdded() throws InterruptedException {
    WORKFLOW = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW)
        .setNumberOfTasks(10)
        .setNumConcurrentTasksPerInstance(100)
        .setCommand(MockTask.TASK_COMMAND)
        .setRebalanceRunningTask(true)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999")); // task stuck

    Workflow.Builder workflowBuilder = new Workflow.Builder(WORKFLOW)
        .addJob(JOB, jobBuilder);

    _driver.start(workflowBuilder.build());

    // All tasks stuck on the same instance
    Assert.assertTrue(checkTasksOnSameInstances());
    // Add a new instance, and some running tasks will be rebalanced to the new node
    startParticipant(_initialNumNodes);
    Assert.assertTrue(checkTasksOnDifferentInstances());
  }

  /**
   * Task type: fixed target
   * Rebalance raunning task: disabled
   * Story: 1 node is down
   */
  @Test
  public void testFixedTargetTaskAndDisabledRebalanceAndNodeDown() throws InterruptedException {
    WORKFLOW = TestHelper.getTestMethodName();
    startParticipant(_initialNumNodes);

    JobConfig.Builder jobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW)
        .setTargetResource(DATABASE)
        .setNumConcurrentTasksPerInstance(100)
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder = new Workflow.Builder(WORKFLOW)
        .addJob(JOB, jobBuilder);

    _driver.start(workflowBuilder.build());

    Assert.assertTrue(checkTasksOnDifferentInstances());
    // Stop a participant and partitions will be moved to the same instance,
    // and tasks rebalanced accordingly
    stopParticipant(_initialNumNodes);
    Assert.assertTrue(checkTasksOnSameInstances());
  }

  /**
   * Task type: fixed target
   * Rebalance raunning task: disabled
   * Story: new node added
   */
  @Test
  public void testFixedTargetTaskAndDisabledRebalanceAndNodeAdded() throws InterruptedException {
    WORKFLOW = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW)
        .setTargetResource(DATABASE)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setNumConcurrentTasksPerInstance(100)
        .setFailureThreshold(2)
        .setMaxAttemptsPerTask(2)
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(
            ImmutableMap.of(MockTask.JOB_DELAY, "99999999")); // task stuck

    Workflow.Builder workflowBuilder = new Workflow.Builder(WORKFLOW).addJob(JOB, jobBuilder);

    _driver.start(workflowBuilder.build());

    // All tasks stuck on the same instance
    Assert.assertTrue(checkTasksOnSameInstances());
    // Add a new instance, partition is rebalanced
    startParticipant(_initialNumNodes);
    ZkHelixClusterVerifier clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setResources(Sets.newHashSet(DATABASE)).build();
    Assert.assertTrue(clusterVerifier.verify(10*1000));
    // Running tasks are also rebalanced, even though RebalanceRunningTask is disabled
    Assert.assertTrue(checkTasksOnDifferentInstances());
  }

  /**
   * Task type: fixed target
   * Rebalance raunning task: enabled
   * Story: new node added
   */
  @Test
  public void testFixedTargetTaskAndEnabledRebalanceAndNodeAdded() throws InterruptedException {
    WORKFLOW = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW)
        .setTargetResource(DATABASE)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setNumConcurrentTasksPerInstance(100)
        .setRebalanceRunningTask(true)
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(
            ImmutableMap.of(MockTask.JOB_DELAY, "99999999")); // task stuck

    Workflow.Builder workflowBuilder = new Workflow.Builder(WORKFLOW).addJob(JOB, jobBuilder);

    _driver.start(workflowBuilder.build());

    // All tasks stuck on the same instance
    Assert.assertTrue(checkTasksOnSameInstances());
    // Add a new instance, partition is rebalanced
    startParticipant(_initialNumNodes);
    ZkHelixClusterVerifier clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setResources(Sets.newHashSet(DATABASE)).build();
    Assert.assertTrue(clusterVerifier.verify(10*1000));
    // Running tasks are also rebalanced
    Assert.assertTrue(checkTasksOnDifferentInstances());
  }
}
