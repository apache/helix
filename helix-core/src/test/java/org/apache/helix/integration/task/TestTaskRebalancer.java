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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class TestTaskRebalancer extends ZkTestBase {
  private static final int n = 5;
  private static final int START_PORT = 12918;
  private static final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private static final String TIMEOUT_CONFIG = "Timeout";
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_REPLICAS = 3;
  private final String CLUSTER_NAME = "TestTaskRebalancer";
  private final MockParticipant[] _participants = new MockParticipant[n];
  private MockController _controller;

  private HelixManager _manager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_zkclient.exists(namespace)) {
      _zkclient.deleteRecursive(namespace);
    }

    _setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < n; i++) {
      String storageNodeName = "localhost_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // Set up target db
    _setupTool.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, NUM_PARTITIONS,
        MASTER_SLAVE_STATE_MODEL);
    _setupTool
        .rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, NUM_REPLICAS);

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("Reindex", new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new ReindexTask(context);
      }
    });

    // start dummy participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (START_PORT + i);
      _participants[i] = new MockParticipant(_zkaddr, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory(StateModelDefId.from("Task"),
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }

    // start controller
    String controllerName = "controller_0";
    _controller = new MockController(_zkaddr, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // create cluster manager
    _manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR,
            _zkaddr);
    _manager.connect();
    _driver = new TaskDriver(_manager);

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
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
    String jobName = "Expiry";
    long expiry = 1000;
    Map<String, String> commandConfig = ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(100));
    Workflow flow =
        WorkflowGenerator
            .generateDefaultSingleJobWorkflowBuilderWithExtraConfigs(jobName, commandConfig)
            .setExpiry(expiry).build();

    _driver.start(flow);
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.IN_PROGRESS);

    // Running workflow should have config and context viewable through accessor
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey workflowCfgKey = accessor.keyBuilder().resourceConfig(jobName);
    String workflowPropStoreKey =
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobName);

    // Ensure context and config exist
    Assert.assertTrue(_manager.getHelixPropertyStore().exists(workflowPropStoreKey,
        AccessOption.PERSISTENT));
    Assert.assertNotSame(accessor.getProperty(workflowCfgKey), null);

    // Wait for job to finish and expire
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.COMPLETED);
    Thread.sleep(expiry);
    _driver.invokeRebalance();
    Thread.sleep(expiry);

    // Ensure workflow config and context were cleaned up by now
    Assert.assertFalse(_manager.getHelixPropertyStore().exists(workflowPropStoreKey,
        AccessOption.PERSISTENT));
    Assert.assertEquals(accessor.getProperty(workflowCfgKey), null);
  }

  private void basic(long jobCompletionTime) throws Exception {
    // We use a different resource name in each test method as a work around for a helix participant
    // bug where it does
    // not clear locally cached state when a resource partition is dropped. Once that is fixed we
    // should change these
    // tests to use the same resource name and implement a beforeMethod that deletes the task
    // resource.
    final String jobResource = "basic" + jobCompletionTime;
    Map<String, String> commandConfig =
        ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(jobCompletionTime));
    Workflow flow =
        WorkflowGenerator.generateDefaultSingleJobWorkflowBuilderWithExtraConfigs(jobResource,
            commandConfig).build();
    _driver.start(flow);

    // Wait for job completion
    TestUtil.pollForWorkflowState(_manager, jobResource, TaskState.COMPLETED);

    // Ensure all partitions are completed individually
    JobContext ctx = TaskUtil.getJobContext(_manager, TaskUtil.getNamespacedJobName(jobResource));
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      Assert.assertEquals(ctx.getPartitionState(i), TaskPartitionState.COMPLETED);
      Assert.assertEquals(ctx.getPartitionNumAttempts(i), 1);
    }
  }

  @Test
  public void partitionSet() throws Exception {
    final String jobResource = "partitionSet";
    ImmutableList<String> targetPartitions =
        ImmutableList.of("TestDB_1", "TestDB_2", "TestDB_3", "TestDB_5", "TestDB_8", "TestDB_13");

    // construct and submit our basic workflow
    Map<String, String> commandConfig = ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(100));
    Workflow flow =
        WorkflowGenerator.generateDefaultSingleJobWorkflowBuilderWithExtraConfigs(jobResource,
            commandConfig, JobConfig.MAX_ATTEMPTS_PER_TASK, String.valueOf(1),
            JobConfig.TARGET_PARTITIONS, Joiner.on(",").join(targetPartitions)).build();
    _driver.start(flow);

    // wait for job completeness/timeout
    TestUtil.pollForWorkflowState(_manager, jobResource, TaskState.COMPLETED);

    // see if resulting context completed successfully for our partition set
    String namespacedName = TaskUtil.getNamespacedJobName(jobResource);

    JobContext ctx = TaskUtil.getJobContext(_manager, namespacedName);
    WorkflowContext workflowContext = TaskUtil.getWorkflowContext(_manager, jobResource);
    Assert.assertNotNull(ctx);
    Assert.assertNotNull(workflowContext);
    Assert.assertEquals(workflowContext.getJobState(namespacedName), TaskState.COMPLETED);
    for (String pName : targetPartitions) {
      int i = ctx.getPartitionsByTarget().get(pName).get(0);
      Assert.assertEquals(ctx.getPartitionState(i), TaskPartitionState.COMPLETED);
      Assert.assertEquals(ctx.getPartitionNumAttempts(i), 1);
    }
  }

  @Test
  public void testRepeatedWorkflow() throws Exception {
    String workflowName = "SomeWorkflow";
    Workflow flow =
        WorkflowGenerator.generateDefaultRepeatedJobWorkflowBuilder(workflowName).build();
    new TaskDriver(_manager).start(flow);

    // Wait until the workflow completes
    TestUtil.pollForWorkflowState(_manager, workflowName, TaskState.COMPLETED);

    // Assert completion for all tasks within two minutes
    for (String task : flow.getJobConfigs().keySet()) {
      TestUtil.pollForJobState(_manager, workflowName, task, TaskState.COMPLETED);
    }
  }

  @Test
  public void timeouts() throws Exception {
    final String jobResource = "timeouts";
    Workflow flow =
        WorkflowGenerator.generateDefaultSingleJobWorkflowBuilderWithExtraConfigs(jobResource,
            WorkflowGenerator.DEFAULT_COMMAND_CONFIG, JobConfig.MAX_ATTEMPTS_PER_TASK,
            String.valueOf(2), JobConfig.TIMEOUT_PER_TASK, String.valueOf(100)).build();
    _driver.start(flow);

    // Wait until the job reports failure.
    TestUtil.pollForWorkflowState(_manager, jobResource, TaskState.FAILED);

    // Check that all partitions timed out up to maxAttempts
    JobContext ctx = TaskUtil.getJobContext(_manager, TaskUtil.getNamespacedJobName(jobResource));
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
  public void testNamedQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    JobQueue queue = new JobQueue.Builder(queueName).build();
    _driver.createQueue(queue);

    // Enqueue jobs
    Set<String> master = Sets.newHashSet("MASTER");
    Set<String> slave = Sets.newHashSet("SLAVE");
    JobConfig.Builder job1 =
        new JobConfig.Builder().setCommand("Reindex")
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(master);
    JobConfig.Builder job2 =
        new JobConfig.Builder().setCommand("Reindex")
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(slave);
    _driver.enqueueJob(queueName, "masterJob", job1);
    _driver.enqueueJob(queueName, "slaveJob", job2);

    // Ensure successful completion
    String namespacedJob1 = queueName + "_masterJob";
    String namespacedJob2 = queueName + "_slaveJob";
    TestUtil.pollForJobState(_manager, queueName, namespacedJob1, TaskState.COMPLETED);
    TestUtil.pollForJobState(_manager, queueName, namespacedJob2, TaskState.COMPLETED);
    JobContext masterJobContext = TaskUtil.getJobContext(_manager, namespacedJob1);
    JobContext slaveJobContext = TaskUtil.getJobContext(_manager, namespacedJob2);

    // Ensure correct ordering
    long job1Finish = masterJobContext.getFinishTime();
    long job2Start = slaveJobContext.getStartTime();
    Assert.assertTrue(job2Start >= job1Finish);

    // Flush queue and check cleanup
    _driver.flushQueue(queueName);
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(namespacedJob1)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(namespacedJob1)));
    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(namespacedJob2)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(namespacedJob2)));
    WorkflowConfig workflowCfg = TaskUtil.getWorkflowCfg(_manager, queueName);
    JobDag dag = workflowCfg.getJobDag();
    Assert.assertFalse(dag.getAllNodes().contains(namespacedJob1));
    Assert.assertFalse(dag.getAllNodes().contains(namespacedJob2));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namespacedJob1));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namespacedJob2));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namespacedJob1));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namespacedJob2));
  }

  private static class ReindexTask implements Task {
    private final long _delay;
    private volatile boolean _canceled;

    public ReindexTask(TaskCallbackContext context) {
      JobConfig jobCfg = context.getJobConfig();
      Map<String, String> cfg = jobCfg.getJobCommandConfigMap();
      if (cfg == null) {
        cfg = Collections.emptyMap();
      }
      _delay = cfg.containsKey(TIMEOUT_CONFIG) ? Long.parseLong(cfg.get(TIMEOUT_CONFIG)) : 200L;
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
