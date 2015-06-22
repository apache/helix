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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.integration.task.TestTaskRebalancerStopResume.ReindexTask;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.ScheduleConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskResult.Status;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestIndependentTaskRebalancer extends ZkTestBase {
  private static final int n = 5;
  private static final int START_PORT = 12918;
  private final String CLUSTER_NAME = "TestIndependentTaskRebalancer";
  private final MockParticipant[] _participants = new MockParticipant[n];
  private MockController _controller;
  private Set<String> _invokedClasses = Sets.newHashSet();
  private Map<String, Integer> _runCounts = Maps.newHashMap();

  private HelixManager _manager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_zkclient.exists(namespace)) {
      _zkclient.deleteRecursive(namespace);
    }

    // Setup cluster and instances
    _setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < n; i++) {
      String storageNodeName = "localhost_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // start dummy participants
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (START_PORT + i);

      // Set task callbacks
      Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
      taskFactoryReg.put("TaskOne", new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new TaskOne(context, instanceName);
        }
      });
      taskFactoryReg.put("TaskTwo", new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new TaskTwo(context, instanceName);
        }
      });
      taskFactoryReg.put("SingleFailTask", new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new SingleFailTask();
        }
      });

      _participants[i] = new MockParticipant(_zkaddr, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory(StateModelDefId.from("Task"),
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }

    // Start controller
    String controllerName = "controller_0";
    _controller = new MockController(_zkaddr, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Start an admin connection
    _manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR,
            _zkaddr);
    _manager.connect();
    _driver = new TaskDriver(_manager);
  }

  @BeforeMethod
  public void beforeMethod() {
    _invokedClasses.clear();
    _runCounts.clear();
  }

  @Test
  public void testDifferentTasks() throws Exception {
    // Create a job with two different tasks
    String jobName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(jobName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(2);
    TaskConfig taskConfig1 = new TaskConfig("TaskOne", null, true);
    TaskConfig taskConfig2 = new TaskConfig("TaskTwo", null, true);
    taskConfigs.add(taskConfig1);
    taskConfigs.add(taskConfig2);
    workflowBuilder.addTaskConfigs(jobName, taskConfigs);
    workflowBuilder.addConfig(jobName, JobConfig.COMMAND, "DummyCommand");
    Map<String, String> jobConfigMap = Maps.newHashMap();
    jobConfigMap.put("Timeout", "1000");
    workflowBuilder.addJobCommandConfigMap(jobName, jobConfigMap);
    _driver.start(workflowBuilder.build());

    // Ensure the job completes
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.IN_PROGRESS);
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.COMPLETED);

    // Ensure that each class was invoked
    Assert.assertTrue(_invokedClasses.contains(TaskOne.class.getName()));
    Assert.assertTrue(_invokedClasses.contains(TaskTwo.class.getName()));
  }

  @Test
  public void testThresholdFailure() throws Exception {
    // Create a job with two different tasks
    String jobName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(jobName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(2);
    Map<String, String> taskConfigMap = Maps.newHashMap(ImmutableMap.of("fail", "" + true));
    TaskConfig taskConfig1 = new TaskConfig("TaskOne", taskConfigMap, false);
    TaskConfig taskConfig2 = new TaskConfig("TaskTwo", null, false);
    taskConfigs.add(taskConfig1);
    taskConfigs.add(taskConfig2);
    workflowBuilder.addTaskConfigs(jobName, taskConfigs);
    workflowBuilder.addConfig(jobName, JobConfig.COMMAND, "DummyCommand");
    workflowBuilder.addConfig(jobName, JobConfig.FAILURE_THRESHOLD, "" + 1);
    Map<String, String> jobConfigMap = Maps.newHashMap();
    jobConfigMap.put("Timeout", "1000");
    workflowBuilder.addJobCommandConfigMap(jobName, jobConfigMap);
    _driver.start(workflowBuilder.build());

    // Ensure the job completes
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.IN_PROGRESS);
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.COMPLETED);

    // Ensure that each class was invoked
    Assert.assertTrue(_invokedClasses.contains(TaskOne.class.getName()));
    Assert.assertTrue(_invokedClasses.contains(TaskTwo.class.getName()));
  }

  @Test
  public void testOptionalTaskFailure() throws Exception {
    // Create a job with two different tasks
    String jobName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(jobName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(2);
    Map<String, String> taskConfigMap = Maps.newHashMap(ImmutableMap.of("fail", "" + true));
    TaskConfig taskConfig1 = new TaskConfig("TaskOne", taskConfigMap, true);
    TaskConfig taskConfig2 = new TaskConfig("TaskTwo", null, false);
    taskConfigs.add(taskConfig1);
    taskConfigs.add(taskConfig2);
    workflowBuilder.addTaskConfigs(jobName, taskConfigs);
    workflowBuilder.addConfig(jobName, JobConfig.COMMAND, "DummyCommand");
    Map<String, String> jobConfigMap = Maps.newHashMap();
    jobConfigMap.put("Timeout", "1000");
    workflowBuilder.addJobCommandConfigMap(jobName, jobConfigMap);
    _driver.start(workflowBuilder.build());

    // Ensure the job completes
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.IN_PROGRESS);
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.COMPLETED);

    // Ensure that each class was invoked
    Assert.assertTrue(_invokedClasses.contains(TaskOne.class.getName()));
    Assert.assertTrue(_invokedClasses.contains(TaskTwo.class.getName()));
  }

  @Test
  public void testReassignment() throws Exception {
    final int NUM_INSTANCES = 2;
    String jobName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(jobName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(2);
    Map<String, String> taskConfigMap =
        Maps.newHashMap(ImmutableMap.of("fail", "" + true, "failInstance", "localhost_"
            + START_PORT));
    TaskConfig taskConfig1 = new TaskConfig("TaskOne", taskConfigMap, false);
    taskConfigs.add(taskConfig1);
    workflowBuilder.addTaskConfigs(jobName, taskConfigs);
    workflowBuilder.addConfig(jobName, JobConfig.COMMAND, "DummyCommand");
    workflowBuilder.addConfig(jobName, JobConfig.MAX_FORCED_REASSIGNMENTS_PER_TASK, ""
        + (NUM_INSTANCES - 1)); // this ensures that every instance gets one chance
    Map<String, String> jobConfigMap = Maps.newHashMap();
    jobConfigMap.put("Timeout", "1000");
    workflowBuilder.addJobCommandConfigMap(jobName, jobConfigMap);
    _driver.start(workflowBuilder.build());

    // Ensure the job completes
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.IN_PROGRESS);
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.COMPLETED);

    // Ensure that the class was invoked
    Assert.assertTrue(_invokedClasses.contains(TaskOne.class.getName()));

    // Ensure that this was tried on two different instances, the first of which exhausted the
    // attempts number, and the other passes on the first try
    Assert.assertEquals(_runCounts.size(), NUM_INSTANCES);
    Assert.assertTrue(_runCounts.values().contains(
        JobConfig.DEFAULT_MAX_ATTEMPTS_PER_TASK / NUM_INSTANCES));
    Assert.assertTrue(_runCounts.values().contains(1));
  }

  @Test
  public void testOneTimeScheduled() throws Exception {
    String jobName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(jobName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(1);
    Map<String, String> taskConfigMap = Maps.newHashMap();
    TaskConfig taskConfig1 = new TaskConfig("TaskOne", taskConfigMap, false);
    taskConfigs.add(taskConfig1);
    workflowBuilder.addTaskConfigs(jobName, taskConfigs);
    workflowBuilder.addConfig(jobName, JobConfig.COMMAND, "DummyCommand");
    Map<String, String> jobConfigMap = Maps.newHashMap();
    jobConfigMap.put("Timeout", "1000");
    workflowBuilder.addJobCommandConfigMap(jobName, jobConfigMap);
    long inFiveSeconds = System.currentTimeMillis() + (5 * 1000);
    workflowBuilder.setScheduleConfig(ScheduleConfig.oneTimeDelayedStart(new Date(inFiveSeconds)));
    _driver.start(workflowBuilder.build());

    // Ensure the job completes
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.IN_PROGRESS);
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.COMPLETED);

    // Ensure that the class was invoked
    Assert.assertTrue(_invokedClasses.contains(TaskOne.class.getName()));

    // Check that the workflow only started after the start time (with a 1 second buffer)
    WorkflowContext workflowCtx = TaskUtil.getWorkflowContext(_manager, jobName);
    long startTime = workflowCtx.getStartTime();
    Assert.assertTrue((startTime + 1000) >= inFiveSeconds);
  }

  @Test
  public void testDelayedRetry() throws Exception {
    // Create a single job with single task, set retry delay
    int delay = 3000;
    String jobName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(jobName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(1);
    Map<String, String> taskConfigMap = Maps.newHashMap();
    TaskConfig taskConfig1 = new TaskConfig("SingleFailTask", taskConfigMap, false);
    taskConfigs.add(taskConfig1);
    workflowBuilder.addTaskConfigs(jobName, taskConfigs);
    workflowBuilder.addConfig(jobName, JobConfig.COMMAND, "DummyCommand");
    workflowBuilder.addConfig(jobName, JobConfig.TASK_RETRY_DELAY, String.valueOf(delay));
    Map<String, String> jobConfigMap = Maps.newHashMap();
    workflowBuilder.addJobCommandConfigMap(jobName, jobConfigMap);
    SingleFailTask.hasFailed = false;
    _driver.start(workflowBuilder.build());

    // Ensure completion
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.COMPLETED);

    // Ensure a single retry happened
    JobContext jobCtx = TaskUtil.getJobContext(_manager, jobName + "_" + jobName);
    Assert.assertEquals(jobCtx.getPartitionNumAttempts(0), 2);
    Assert.assertTrue(jobCtx.getFinishTime() - jobCtx.getStartTime() >= delay);
  }

  private class TaskOne extends ReindexTask {
    private final boolean _shouldFail;
    private final String _instanceName;

    public TaskOne(TaskCallbackContext context, String instanceName) {
      super(context);

      // Check whether or not this task should succeed
      TaskConfig taskConfig = context.getTaskConfig();
      boolean shouldFail = false;
      if (taskConfig != null) {
        Map<String, String> configMap = taskConfig.getConfigMap();
        if (configMap != null && configMap.containsKey("fail")
            && Boolean.parseBoolean(configMap.get("fail"))) {
          // if a specific instance is specified, only fail for that one
          shouldFail =
              !configMap.containsKey("failInstance")
                  || configMap.get("failInstance").equals(instanceName);
        }
      }
      _shouldFail = shouldFail;

      // Initialize the count for this instance if not already done
      if (!_runCounts.containsKey(instanceName)) {
        _runCounts.put(instanceName, 0);
      }
      _instanceName = instanceName;
    }

    @Override
    public TaskResult run() {
      _invokedClasses.add(getClass().getName());
      _runCounts.put(_instanceName, _runCounts.get(_instanceName) + 1);

      // Fail the task if it should fail
      if (_shouldFail) {
        return new TaskResult(Status.ERROR, null);
      }

      return super.run();
    }
  }

  private class TaskTwo extends TaskOne {
    public TaskTwo(TaskCallbackContext context, String instanceName) {
      super(context, instanceName);
    }
  }

  private static class SingleFailTask implements Task {
    public static boolean hasFailed = false;

    @Override
    public TaskResult run() {
      if (!hasFailed) {
        hasFailed = true;
        return new TaskResult(Status.ERROR, null);
      }
      return new TaskResult(Status.COMPLETED, null);
    }

    @Override
    public void cancel() {
    }
  }
}
