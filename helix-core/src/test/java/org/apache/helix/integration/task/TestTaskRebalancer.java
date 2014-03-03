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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class TestTaskRebalancer extends ZkIntegrationTestBase {
  private static final int n = 5;
  private static final int START_PORT = 12918;
  private static final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_REPLICAS = 3;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private final MockParticipantManager[] _participants = new MockParticipantManager[n];
  private ClusterControllerManager _controller;

  private HelixManager _manager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < n; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // Set up target db
    setupTool.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, NUM_PARTITIONS,
        MASTER_SLAVE_STATE_MODEL);
    setupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, NUM_REPLICAS);

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("Reindex", new TaskFactory() {
      @Override
      public Task createNewTask(String config) {
        return new ReindexTask(config);
      }
    });

    // start dummy participants
    for (int i = 0; i < n; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory(StateModelDefId.from("Task"),
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // create cluster manager
    _manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR,
            ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    // _controller = null;
    for (int i = 0; i < n; i++) {
      _participants[i].syncStop();
      // _participants[i] = null;
    }

    _manager.disconnect();
  }

  @Test
  public void basic() throws Exception {
    basic(100);
  }

  @Test
  public void zeroTaskCompletionTime() throws Exception {
    basic(0);
  }

  @Test
  public void testExpiry() throws Exception {
    String taskName = "Expiry";
    long expiry = 1000;
    Workflow flow =
        WorkflowGenerator
            .generateDefaultSingleTaskWorkflowBuilderWithExtraConfigs(taskName,
                TaskConfig.COMMAND_CONFIG, String.valueOf(100)).setExpiry(expiry).build();

    _driver.start(flow);
    TestUtil.pollForWorkflowState(_manager, taskName, TaskState.IN_PROGRESS);

    // Running workflow should have config and context viewable through accessor
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey workflowCfgKey = accessor.keyBuilder().resourceConfig(taskName);
    String workflowPropStoreKey =
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, taskName);

    // Ensure context and config exist
    Assert.assertTrue(_manager.getHelixPropertyStore().exists(workflowPropStoreKey,
        AccessOption.PERSISTENT));
    Assert.assertNotSame(accessor.getProperty(workflowCfgKey), null);

    // Wait for task to finish and expire
    TestUtil.pollForWorkflowState(_manager, taskName, TaskState.COMPLETED);
    Thread.sleep(expiry);
    _driver.invokeRebalance();
    Thread.sleep(expiry);

    // Ensure workflow config and context were cleaned up by now
    Assert.assertFalse(_manager.getHelixPropertyStore().exists(workflowPropStoreKey,
        AccessOption.PERSISTENT));
    Assert.assertEquals(accessor.getProperty(workflowCfgKey), null);
  }

  private void basic(long taskCompletionTime) throws Exception {
    // We use a different resource name in each test method as a work around for a helix participant
    // bug where it does
    // not clear locally cached state when a resource partition is dropped. Once that is fixed we
    // should change these
    // tests to use the same resource name and implement a beforeMethod that deletes the task
    // resource.
    final String taskResource = "basic" + taskCompletionTime;
    Workflow flow =
        WorkflowGenerator.generateDefaultSingleTaskWorkflowBuilderWithExtraConfigs(taskResource,
            TaskConfig.COMMAND_CONFIG, String.valueOf(taskCompletionTime)).build();
    _driver.start(flow);

    // Wait for task completion
    TestUtil.pollForWorkflowState(_manager, taskResource, TaskState.COMPLETED);

    // Ensure all partitions are completed individually
    TaskContext ctx =
        TaskUtil.getTaskContext(_manager, TaskUtil.getNamespacedTaskName(taskResource));
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      Assert.assertEquals(ctx.getPartitionState(i), TaskPartitionState.COMPLETED);
      Assert.assertEquals(ctx.getPartitionNumAttempts(i), 1);
    }
  }

  @Test
  public void partitionSet() throws Exception {
    final String taskResource = "partitionSet";
    ImmutableList<Integer> targetPartitions = ImmutableList.of(1, 2, 3, 5, 8, 13);

    // construct and submit our basic workflow
    Workflow flow =
        WorkflowGenerator.generateDefaultSingleTaskWorkflowBuilderWithExtraConfigs(taskResource,
            TaskConfig.COMMAND_CONFIG, String.valueOf(100), TaskConfig.MAX_ATTEMPTS_PER_PARTITION,
            String.valueOf(1), TaskConfig.TARGET_PARTITIONS, Joiner.on(",").join(targetPartitions))
            .build();
    _driver.start(flow);

    // wait for task completeness/timeout
    TestUtil.pollForWorkflowState(_manager, taskResource, TaskState.COMPLETED);

    // see if resulting context completed successfully for our partition set
    String namespacedName = TaskUtil.getNamespacedTaskName(taskResource);

    TaskContext ctx = TaskUtil.getTaskContext(_manager, namespacedName);
    WorkflowContext workflowContext = TaskUtil.getWorkflowContext(_manager, taskResource);
    Assert.assertNotNull(ctx);
    Assert.assertNotNull(workflowContext);
    Assert.assertEquals(workflowContext.getTaskState(namespacedName), TaskState.COMPLETED);
    for (int i : targetPartitions) {
      Assert.assertEquals(ctx.getPartitionState(i), TaskPartitionState.COMPLETED);
      Assert.assertEquals(ctx.getPartitionNumAttempts(i), 1);
    }
  }

  @Test
  public void testRepeatedWorkflow() throws Exception {
    String workflowName = "SomeWorkflow";
    Workflow flow =
        WorkflowGenerator.generateDefaultRepeatedTaskWorkflowBuilder(workflowName).build();
    new TaskDriver(_manager).start(flow);

    // Wait until the task completes
    TestUtil.pollForWorkflowState(_manager, workflowName, TaskState.COMPLETED);

    // Assert completion for all tasks within two minutes
    for (String task : flow.getTaskConfigs().keySet()) {
      TestUtil.pollForTaskState(_manager, workflowName, task, TaskState.COMPLETED);
    }
  }

  @Test
  public void timeouts() throws Exception {
    final String taskResource = "timeouts";
    Workflow flow =
        WorkflowGenerator.generateDefaultSingleTaskWorkflowBuilderWithExtraConfigs(taskResource,
            TaskConfig.MAX_ATTEMPTS_PER_PARTITION, String.valueOf(2),
            TaskConfig.TIMEOUT_PER_PARTITION, String.valueOf(100)).build();
    _driver.start(flow);

    // Wait until the task reports failure.
    TestUtil.pollForWorkflowState(_manager, taskResource, TaskState.FAILED);

    // Check that all partitions timed out up to maxAttempts
    TaskContext ctx =
        TaskUtil.getTaskContext(_manager, TaskUtil.getNamespacedTaskName(taskResource));
    int maxAttempts = 0;
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      TaskPartitionState state = ctx.getPartitionState(i);
      if (state != null) {
        Assert.assertEquals(state, TaskPartitionState.TIMED_OUT);
        maxAttempts = Math.max(maxAttempts, ctx.getPartitionNumAttempts(i));
      }
    }
    Assert.assertEquals(maxAttempts, 2);
  }

  @Test
  public void testIndependentTask() throws Exception {
    final String taskResource = "independentTask";
    Map<String, String> config = new TreeMap<String, String>();
    config.put("TargetPartitions", "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19");
    config.put("Command", "Reindex");
    config.put("CommandConfig", String.valueOf(200));
    config.put("TimeoutPerPartition", String.valueOf(10 * 1000));
    Workflow flow =
        WorkflowGenerator.generateSingleTaskWorkflowBuilder(taskResource, config).build();
    _driver.start(flow);

    // Wait for task completion
    TestUtil.pollForWorkflowState(_manager, taskResource, TaskState.COMPLETED);

    // Ensure all partitions are completed individually
    TaskContext ctx =
        TaskUtil.getTaskContext(_manager, TaskUtil.getNamespacedTaskName(taskResource));
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      Assert.assertEquals(ctx.getPartitionState(i), TaskPartitionState.COMPLETED);
      Assert.assertEquals(ctx.getPartitionNumAttempts(i), 1);
    }
  }

  @Test
  public void testIndependentRepeatedWorkflow() throws Exception {
    final String workflowName = "independentTaskWorkflow";
    Map<String, String> config = new TreeMap<String, String>();
    config.put("TargetPartitions", "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19");
    config.put("Command", "Reindex");
    config.put("CommandConfig", String.valueOf(200));
    config.put("TimeoutPerPartition", String.valueOf(10 * 1000));
    Workflow flow =
        WorkflowGenerator.generateRepeatedTaskWorkflowBuilder(workflowName, config).build();
    new TaskDriver(_manager).start(flow);

    // Wait until the task completes
    TestUtil.pollForWorkflowState(_manager, workflowName, TaskState.COMPLETED);

    // Assert completion for all tasks within two minutes
    for (String task : flow.getTaskConfigs().keySet()) {
      TestUtil.pollForTaskState(_manager, workflowName, task, TaskState.COMPLETED);
    }
  }

  private static class ReindexTask implements Task {
    private final long _delay;
    private volatile boolean _canceled;

    public ReindexTask(String cfg) {
      _delay = Long.parseLong(cfg);
    }

    @Override
    public TaskResult run() {
      long expiry = System.currentTimeMillis() + _delay;
      long timeLeft;
      while (System.currentTimeMillis() < expiry) {
        if (_canceled) {
          timeLeft = expiry - System.currentTimeMillis();
          return new TaskResult(TaskResult.Status.CANCELED, String.valueOf(timeLeft < 0 ? 0
              : timeLeft));
        }
        sleep(50);
      }
      timeLeft = expiry - System.currentTimeMillis();
      return new TaskResult(TaskResult.Status.COMPLETED,
          String.valueOf(timeLeft < 0 ? 0 : timeLeft));
    }

    @Override
    public void cancel() {
      _canceled = true;
    }

    private static void sleep(long d) {
      try {
        Thread.sleep(d);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
