package org.apache.helix.task;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestGetSetUserContentStore extends TaskTestBase {
  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_JOB = 5;
  private Map<String, String> _jobCommandMap;

  private final CountDownLatch allTasksReady = new CountDownLatch(NUM_JOB);
  private final CountDownLatch adminReady = new CountDownLatch(1);

  private enum TaskDumpResultKey {
    WorkflowContent,
    JobContent,
    TaskContent
  }

  private class TaskRecord {
    String workflowName;
    String jobName;
    String taskPartitionId;

    TaskRecord(String workflow, String job, String task) {
      workflowName = workflow;
      jobName = job;
      taskPartitionId = task;
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants = new MockParticipantManager[_numNodes];
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursively(namespace);
    }

    // Setup cluster and instances
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // start dummy participants
    for (int i = 0; i < _numNodes; i++) {
      final String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);

      // Set task callbacks
      Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
      TaskFactory shortTaskFactory = WriteTask::new;
      taskFactoryReg.put("WriteTask", shortTaskFactory);

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

    _jobCommandMap = new HashMap<>();
  }

  @Test
  public void testGetUserContentStore() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    Map<String, TaskRecord> recordMap = new HashMap<>();
    // Create 5 jobs with 1 WriteTask each
    for (int i = 0; i < NUM_JOB; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("WriteTask", new HashMap<>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap);
      String jobName = "JOB" + i;
      String taskPartitionId = "0";
      workflowBuilder.addJob(jobName, jobConfigBulider);
      recordMap.put(jobName, new TaskRecord(workflowName, jobName, taskPartitionId));
    }

    // Start the workflow and wait for all tasks started
    _driver.start(workflowBuilder.build());
    allTasksReady.await();

    // add "workflow":"workflow" to the workflow's user content
    _driver.addOrUpdateWorkflowUserContentMap(workflowName,
        Collections.singletonMap(workflowName, workflowName));
    for (TaskRecord rec : recordMap.values()) {
      // add "job":"job" to the job's user content
      String namespacedJobName = TaskUtil.getNamespacedJobName(rec.workflowName, rec.jobName);
      _driver.addOrUpdateJobUserContentMap(rec.workflowName, rec.jobName,
          Collections.singletonMap(namespacedJobName, namespacedJobName));

      String namespacedTaskName =
          TaskUtil.getNamespacedTaskName(namespacedJobName, rec.taskPartitionId);
      // add "taskId":"taskId" to the task's user content
      _driver.addOrUpdateTaskUserContentMap(rec.workflowName, rec.jobName, rec.taskPartitionId,
          Collections.singletonMap(namespacedTaskName, namespacedTaskName));
    }
    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    // Aggregate key-value mappings in UserContentStore
    for (TaskRecord rec : recordMap.values()) {
      Assert.assertEquals(_driver.getWorkflowUserContentMap(rec.workflowName)
              .get(TaskDumpResultKey.WorkflowContent.name()),
          constructContentStoreResultString(rec.workflowName, rec.workflowName));

      String namespacedJobName = TaskUtil.getNamespacedJobName(rec.workflowName, rec.jobName);
      Assert.assertEquals(_driver.getJobUserContentMap(rec.workflowName, rec.jobName)
              .get(TaskDumpResultKey.JobContent.name()),
          constructContentStoreResultString(namespacedJobName, namespacedJobName));

      String namespacedTaskName =
          TaskUtil.getNamespacedTaskName(namespacedJobName, rec.taskPartitionId);
      Assert.assertEquals(
          _driver.getTaskUserContentMap(rec.workflowName, rec.jobName, rec.taskPartitionId)
              .get(TaskDumpResultKey.TaskContent.name()),
          constructContentStoreResultString(namespacedTaskName, namespacedTaskName));
    }
  }

  /**
   * A mock task that writes to UserContentStore. MockTask extends UserContentStore.
   */
  private class WriteTask extends MockTask {

    WriteTask(TaskCallbackContext context) {
      super(context);
    }

    @Override
    public TaskResult run() {
      allTasksReady.countDown();
      try {
        adminReady.await();
      } catch (Exception e) {
        return new TaskResult(TaskResult.Status.FATAL_FAILED, e.getMessage());
      }
      String workflowStoreContent = constructContentStoreResultString(_workflowName, getUserContent(_workflowName, Scope.WORKFLOW));
      String jobStoreContent = constructContentStoreResultString(_jobName, getUserContent(_jobName, Scope.JOB));
      String taskStoreContent = constructContentStoreResultString(_taskName, getUserContent(_taskName, Scope.TASK));
      putUserContent(TaskDumpResultKey.WorkflowContent.name(), workflowStoreContent, Scope.WORKFLOW);
      putUserContent(TaskDumpResultKey.JobContent.name(), jobStoreContent, Scope.JOB);
      putUserContent(TaskDumpResultKey.TaskContent.name(), taskStoreContent, Scope.TASK);
      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }
  }

  private static String constructContentStoreResultString(String key, String value) {
    return String.format("%s::%s", key, value);
  }
}
