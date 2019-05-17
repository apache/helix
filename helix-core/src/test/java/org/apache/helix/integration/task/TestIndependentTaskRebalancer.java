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
import com.google.common.collect.Maps;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
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
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

public class TestIndependentTaskRebalancer extends TaskTestBase {
  private Set<String> _invokedClasses = Sets.newHashSet();
  private Map<String, Integer> _runCounts = Maps.newHashMap();
  private static final AtomicBoolean _failureCtl = new AtomicBoolean(true);

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants = new MockParticipantManager[_numNodes];

    _gSetupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // start dummy participants
    for (int i = 0; i < _numNodes; i++) {
      final String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);

      // Set task callbacks
      Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
      taskFactoryReg.put("TaskOne", context -> new TaskOne(context, instanceName));
      taskFactoryReg.put("TaskTwo", context -> new TaskTwo(context, instanceName));
      taskFactoryReg.put("ControllableFailTask", context -> new Task() {
        @Override
        public TaskResult run() {
          if (_failureCtl.get()) {
            return new TaskResult(Status.FAILED, null);
          } else {
            return new TaskResult(Status.COMPLETED, null);
          }
        }

        @Override
        public void cancel() {

        }
      });
      taskFactoryReg.put("SingleFailTask", context -> new SingleFailTask());

      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }

    // Start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Start an admin connection
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
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
    TaskConfig taskConfig1 = new TaskConfig("TaskOne", null);
    TaskConfig taskConfig2 = new TaskConfig("TaskTwo", null);
    taskConfigs.add(taskConfig1);
    taskConfigs.add(taskConfig2);
    Map<String, String> jobCommandMap = Maps.newHashMap();
    jobCommandMap.put("Timeout", "1000");
    JobConfig.Builder jobBuilder = new JobConfig.Builder().setCommand("DummyCommand")
        .addTaskConfigs(taskConfigs).setJobCommandConfigMap(jobCommandMap);
    workflowBuilder.addJob(jobName, jobBuilder);
    _driver.start(workflowBuilder.build());

    // Ensure the job completes
    _driver.pollForWorkflowState(jobName, TaskState.COMPLETED);

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
    TaskConfig taskConfig1 = new TaskConfig("TaskOne", taskConfigMap);
    TaskConfig taskConfig2 = new TaskConfig("TaskTwo", null);
    taskConfigs.add(taskConfig1);
    taskConfigs.add(taskConfig2);
    Map<String, String> jobConfigMap = Maps.newHashMap();
    jobConfigMap.put("Timeout", "1000");
    JobConfig.Builder jobBuilder = new JobConfig.Builder().setCommand("DummyCommand")
        .setFailureThreshold(1).addTaskConfigs(taskConfigs).setJobCommandConfigMap(jobConfigMap);
    workflowBuilder.addJob(jobName, jobBuilder);
    _driver.start(workflowBuilder.build());

    // Ensure the job completes
    _driver.pollForWorkflowState(jobName, TaskState.IN_PROGRESS);
    _driver.pollForWorkflowState(jobName, TaskState.COMPLETED);

    // Ensure that each class was invoked
    Assert.assertTrue(_invokedClasses.contains(TaskOne.class.getName()));
    Assert.assertTrue(_invokedClasses.contains(TaskTwo.class.getName()));
  }

  @Test
  public void testReassignment() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobNameSuffix = "job";
    String jobName = String.format("%s_%s", workflowName, jobNameSuffix);
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(2);

    TaskConfig taskConfig1 = new TaskConfig("ControllableFailTask", new HashMap<>());
    taskConfigs.add(taskConfig1);
    Map<String, String> jobCommandMap = Maps.newHashMap();
    jobCommandMap.put("Timeout", "1000");

    // Retry forever
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setCommand("DummyCommand").addTaskConfigs(taskConfigs)
            .setJobCommandConfigMap(jobCommandMap).setMaxAttemptsPerTask(Integer.MAX_VALUE);
    workflowBuilder.addJob(jobNameSuffix, jobBuilder);

    _driver.start(workflowBuilder.build());

    // Poll to ensure that the gets re-attempted first
    int trial = 0;
    while (trial < 1000) { // 100 sec
      JobContext jctx = _driver.getJobContext(jobName);
      if (jctx != null && jctx.getPartitionNumAttempts(0) > 1) {
        break;
      }
      Thread.sleep(100);
      trial += 1;
    }

    if (trial == 1000) {
      // Fail if no re-attempts
      Assert.fail("Job " + jobName + " is not retried");
    }

    // Signal the next retry to be successful
    _failureCtl.set(false);

    // Verify that retry will go on and the workflow will finally complete
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);
  }

  @Test
  public void testOneTimeScheduled() throws Exception {
    String jobName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(jobName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(1);
    Map<String, String> taskConfigMap = Maps.newHashMap();
    TaskConfig taskConfig1 = new TaskConfig("TaskOne", taskConfigMap);
    taskConfigs.add(taskConfig1);
    Map<String, String> jobCommandMap = Maps.newHashMap();
    jobCommandMap.put(MockTask.JOB_DELAY, "1000");

    JobConfig.Builder jobBuilder = new JobConfig.Builder().setCommand("DummyCommand")
        .addTaskConfigs(taskConfigs).setJobCommandConfigMap(jobCommandMap);
    workflowBuilder.addJob(jobName, jobBuilder);

    long inFiveSeconds = System.currentTimeMillis() + (5 * 1000);
    workflowBuilder.setScheduleConfig(ScheduleConfig.oneTimeDelayedStart(new Date(inFiveSeconds)));
    _driver.start(workflowBuilder.build());

    // Ensure the job completes
    _driver.pollForWorkflowState(jobName, TaskState.IN_PROGRESS);
    _driver.pollForWorkflowState(jobName, TaskState.COMPLETED);

    // Ensure that the class was invoked
    Assert.assertTrue(_invokedClasses.contains(TaskOne.class.getName()));

    // Check that the workflow only started after the start time (with a 1 second buffer)
    WorkflowContext workflowCtx = _driver.getWorkflowContext(jobName);
    long startTime = workflowCtx.getStartTime();
    Assert.assertTrue(startTime <= inFiveSeconds);
  }

  @Test
  public void testDelayedRetry() throws Exception {
    // Create a single job with single task, set retry delay
    int delay = 3000;
    String jobName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(jobName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(1);
    Map<String, String> taskConfigMap = Maps.newHashMap();
    TaskConfig taskConfig1 = new TaskConfig("SingleFailTask", taskConfigMap);
    taskConfigs.add(taskConfig1);
    Map<String, String> jobCommandMap = Maps.newHashMap();

    JobConfig.Builder jobBuilder = new JobConfig.Builder().setCommand("DummyCommand")
        .setTaskRetryDelay(delay).addTaskConfigs(taskConfigs).setJobCommandConfigMap(jobCommandMap);
    workflowBuilder.addJob(jobName, jobBuilder);

    SingleFailTask.hasFailed = false;
    _driver.start(workflowBuilder.build());

    // Ensure completion
    _driver.pollForWorkflowState(jobName, TaskState.COMPLETED);

    // Ensure a single retry happened
    JobContext jobCtx = _driver.getJobContext(jobName + "_" + jobName);
    Assert.assertEquals(jobCtx.getPartitionNumAttempts(0), 2);
    Assert.assertTrue(jobCtx.getFinishTime() - jobCtx.getStartTime() >= delay);
  }

  private class TaskOne extends MockTask {
    private final boolean _shouldFail;
    private final String _instanceName;

    TaskOne(TaskCallbackContext context, String instanceName) {
      super(context);

      // Check whether or not this task should succeed
      TaskConfig taskConfig = context.getTaskConfig();
      boolean shouldFail = false;
      if (taskConfig != null) {
        Map<String, String> configMap = taskConfig.getConfigMap();
        if (configMap != null && configMap.containsKey("fail")
            && Boolean.parseBoolean(configMap.get("fail"))) {
          // if a specific instance is specified, only fail for that one
          shouldFail = !configMap.containsKey("failInstance")
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
    public synchronized TaskResult run() {
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
    TaskTwo(TaskCallbackContext context, String instanceName) {
      super(context, instanceName);
    }
  }

  private static class SingleFailTask implements Task {
    static boolean hasFailed = false;

    @Override
    public synchronized TaskResult run() {
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