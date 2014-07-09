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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TestTaskRebalancerStopResume.ReindexTask;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
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
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestIndependentTaskRebalancer extends ZkIntegrationTestBase {
  private static final int n = 5;
  private static final int START_PORT = 12918;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private final MockParticipantManager[] _participants = new MockParticipantManager[n];
  private ClusterControllerManager _controller;
  private Set<String> _invokedClasses = Sets.newHashSet();

  private HelixManager _manager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    // Setup cluster and instances
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < n; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // Set task callbacks
    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("TaskOne", new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new TaskOne(context);
      }
    });
    taskFactoryReg.put("TaskTwo", new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new TaskTwo(context);
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

    // Start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Start an admin connection
    _manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR,
            ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);
  }

  @BeforeMethod
  public void beforeMethod() {
    _invokedClasses.clear();
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
    workflowBuilder.addJobConfigMap(jobName, jobConfigMap);
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
    workflowBuilder.addJobConfigMap(jobName, jobConfigMap);
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
    workflowBuilder.addJobConfigMap(jobName, jobConfigMap);
    _driver.start(workflowBuilder.build());

    // Ensure the job completes
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.IN_PROGRESS);
    TestUtil.pollForWorkflowState(_manager, jobName, TaskState.COMPLETED);

    // Ensure that each class was invoked
    Assert.assertTrue(_invokedClasses.contains(TaskOne.class.getName()));
    Assert.assertTrue(_invokedClasses.contains(TaskTwo.class.getName()));
  }

  private class TaskOne extends ReindexTask {
    private final boolean _shouldFail;

    public TaskOne(TaskCallbackContext context) {
      super(context);

      // Check whether or not this task should succeed
      TaskConfig taskConfig = context.getTaskConfig();
      boolean shouldFail = false;
      if (taskConfig != null) {
        Map<String, String> configMap = taskConfig.getConfigMap();
        if (configMap != null && configMap.containsKey("fail")
            && Boolean.parseBoolean(configMap.get("fail"))) {
          shouldFail = true;
        }
      }
      _shouldFail = shouldFail;
    }

    @Override
    public TaskResult run() {
      _invokedClasses.add(getClass().getName());

      // Fail the task if it should fail
      if (_shouldFail) {
        return new TaskResult(Status.ERROR, null);
      }

      return super.run();
    }
  }

  private class TaskTwo extends TaskOne {
    public TaskTwo(TaskCallbackContext context) {
      super(context);
    }
  }
}
