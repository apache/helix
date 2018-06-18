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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

public class TestGenericTaskAssignmentCalculator extends TaskTestBase {
  private Set<String> _invokedClasses = Sets.newHashSet();
  private Map<String, Integer> _runCounts = Maps.newHashMap();
  private TaskConfig _taskConfig;
  private Map<String, String> _jobCommandMap;
  private boolean failTask;

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants = new MockParticipantManager[_numNodes];

    // Setup cluster and instances
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // start dummy participants
    for (int i = 0; i < _numNodes; i++) {
      final String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);

      // Set task callbacks
      Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();

      taskFactoryReg.put("TaskOne", new TaskFactory() {
        @Override public Task createNewTask(TaskCallbackContext context) {
          return new TaskOne(context, instanceName);
        }
      });

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
    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);

    Map<String, String> taskConfigMap = Maps.newHashMap();
    _taskConfig = new TaskConfig("TaskOne", taskConfigMap);
    _jobCommandMap = Maps.newHashMap();
  }

  @Test
  public void testMultipleJobAssignment() throws InterruptedException {
    failTask = false;
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(1);
    taskConfigs.add(_taskConfig);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setCommand("DummyCommand").addTaskConfigs(taskConfigs)
            .setJobCommandConfigMap(_jobCommandMap);

    for (int i = 0; i < 25; i++) {
      workflowBuilder.addJob("JOB" + i, jobBuilder);
    }

    _driver.start(workflowBuilder.build());
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    Assert.assertEquals(_runCounts.size(), 5);
  }

  @Test
  public void testMultipleTaskAssignment() throws InterruptedException {
    failTask = false;
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);

    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(20);
    for (int i = 0; i < 50; i++) {
      Map<String, String> taskConfigMap = Maps.newHashMap();
      taskConfigs.add(new TaskConfig("TaskOne", taskConfigMap));
    }
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setCommand("DummyCommand").setJobCommandConfigMap(_jobCommandMap)
            .addTaskConfigs(taskConfigs);
    workflowBuilder.addJob("JOB", jobBuilder);
    _driver.start(workflowBuilder.build());
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    Assert.assertEquals(_runCounts.size(), 5);
  }

  @Test
  public void testAbortTaskForWorkflowFail()
      throws InterruptedException {
    failTask = true;
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(1);
    taskConfigs.add(_taskConfig);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setCommand("DummyCommand").addTaskConfigs(taskConfigs)
            .setJobCommandConfigMap(_jobCommandMap);

    for (int i = 0; i < 5; i++) {
      workflowBuilder.addJob("JOB" + i, jobBuilder);
    }

    _driver.start(workflowBuilder.build());
    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);

    int abortedTask = 0;
    for (TaskState jobState : _driver.getWorkflowContext(workflowName).getJobStates().values()) {
      if (jobState == TaskState.ABORTED) {
        abortedTask++;
      }
    }

    Assert.assertEquals(abortedTask, 4);
  }

  private class TaskOne extends MockTask {
    private final String _instanceName;

    public TaskOne(TaskCallbackContext context, String instanceName) {
      super(context);

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
      if (failTask) {
        return new TaskResult(TaskResult.Status.FAILED, "");
      }
      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }
  }
}
