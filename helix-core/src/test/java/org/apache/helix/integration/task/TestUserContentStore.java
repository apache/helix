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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.UserContentStore;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestUserContentStore extends TaskTestBase {

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

      taskFactoryReg.put("ContentStoreTask", new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new ContentStoreTask();
        }
      });

      taskFactoryReg.put("TaskOne", new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new TaskOne();
        }
      });

      taskFactoryReg.put("TaskTwo", new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new TaskTwo();
        }
      });

      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task", new TaskStateModelFactory(_participants[i],
          taskFactoryReg));
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

  @Test
  public void testWorkflowAndJobTaskUserContentStore() throws InterruptedException {
    String jobName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(jobName);
    List<TaskConfig> taskConfigs = Lists.newArrayListWithCapacity(1);
    Map<String, String> taskConfigMap = Maps.newHashMap();
    TaskConfig taskConfig1 = new TaskConfig("ContentStoreTask", taskConfigMap);
    taskConfigs.add(taskConfig1);
    Map<String, String> jobCommandMap = Maps.newHashMap();
    jobCommandMap.put("Timeout", "1000");

    JobConfig.Builder jobBuilder = new JobConfig.Builder().setCommand("DummyCommand")
        .addTaskConfigs(taskConfigs).setWorkflow(jobName)
        .setJobCommandConfigMap(jobCommandMap);
    workflowBuilder.addJob(jobName, jobBuilder);

    _driver.start(workflowBuilder.build());
    _driver.pollForWorkflowState(jobName, TaskState.COMPLETED);
    Assert
        .assertEquals(_driver.getWorkflowContext(jobName).getWorkflowState(), TaskState.COMPLETED);
  }

  @Test
  public void testJobContentPutAndGetWithDependency() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName, 0, 100);

    List<TaskConfig> taskConfigs1 = Lists.newArrayListWithCapacity(1);
    List<TaskConfig> taskConfigs2 = Lists.newArrayListWithCapacity(1);
    Map<String, String> taskConfigMap1 = Maps.newHashMap();
    Map<String, String> taskConfigMap2 = Maps.newHashMap();
    TaskConfig taskConfig1 = new TaskConfig("TaskOne", taskConfigMap1);
    TaskConfig taskConfig2 = new TaskConfig("TaskTwo", taskConfigMap2);

    taskConfigs1.add(taskConfig1);
    taskConfigs2.add(taskConfig2);
    Map<String, String> jobCommandMap = Maps.newHashMap();
    jobCommandMap.put("Timeout", "1000");

    JobConfig.Builder jobBuilder1 =
        new JobConfig.Builder().setCommand("DummyCommand").addTaskConfigs(taskConfigs1)
            .setJobCommandConfigMap(jobCommandMap).setWorkflow(queueName);
    JobConfig.Builder jobBuilder2 =
        new JobConfig.Builder().setCommand("DummyCommand").addTaskConfigs(taskConfigs2)
            .setJobCommandConfigMap(jobCommandMap).setWorkflow(queueName);

    queueBuilder.enqueueJob(queueName + 0, jobBuilder1);
    queueBuilder.enqueueJob(queueName + 1, jobBuilder2);

    _driver.start(queueBuilder.build());
    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, queueName + 1),
        TaskState.COMPLETED);
    Assert.assertEquals(_driver.getWorkflowContext(queueName)
        .getJobState(TaskUtil.getNamespacedJobName(queueName, queueName + 1)), TaskState.COMPLETED);
  }

  private static class ContentStoreTask extends UserContentStore implements Task {

    @Override public TaskResult run() {
      putUserContent("ContentTest", "Value1", Scope.JOB);
      putUserContent("ContentTest", "Value2", Scope.WORKFLOW);
      putUserContent("ContentTest", "Value3", Scope.TASK);
      if (!getUserContent("ContentTest", Scope.JOB).equals("Value1") || !getUserContent(
          "ContentTest", Scope.WORKFLOW).equals("Value2") || !getUserContent("ContentTest",
          Scope.TASK).equals("Value3")) {
        return new TaskResult(TaskResult.Status.FAILED, null);
      }
      return new TaskResult(TaskResult.Status.COMPLETED, null);
    }

    @Override public void cancel() {
    }
  }


  private static class TaskOne extends UserContentStore implements Task {

    @Override public TaskResult run() {
      putUserContent("RaceTest", "RaceValue", Scope.WORKFLOW);
      return new TaskResult(TaskResult.Status.COMPLETED, null);
    }

    @Override public void cancel() {
    }
  }

  private static class TaskTwo extends UserContentStore implements Task {

    @Override public TaskResult run() {
      if (!getUserContent("RaceTest", Scope.WORKFLOW).equals("RaceValue")) {
        return new TaskResult(TaskResult.Status.FAILED, null);
      }
      return new TaskResult(TaskResult.Status.COMPLETED, null);

    }

    @Override public void cancel() {
    }
  }
}
