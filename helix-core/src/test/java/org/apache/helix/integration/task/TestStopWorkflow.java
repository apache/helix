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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestStopWorkflow extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    super.beforeClass();
  }

  @Test
  public void testStopWorkflow() throws InterruptedException {
    stopTestSetup(5);

    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.SUCCESS_COUNT_BEFORE_FAIL, "1"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("job1_will_succeed", jobBuilder);
    jobQueue.enqueueJob("job2_will_fail", jobBuilder);
    _driver.start(jobQueue.build());

    // job1 should succeed and job2 should fail, wait until that happens
    _driver.pollForJobState(jobQueueName,
        TaskUtil.getNamespacedJobName(jobQueueName, "job2_will_fail"), TaskState.FAILED);

    Assert.assertEquals(TaskState.IN_PROGRESS,
        _driver.getWorkflowContext(jobQueueName).getWorkflowState());

    // Now stop the workflow, and it should be stopped because all jobs have completed or failed.
    _driver.waitToStop(jobQueueName, 4000);
    _driver.pollForWorkflowState(jobQueueName, TaskState.STOPPED);

    Assert.assertEquals(TaskState.STOPPED,
        _driver.getWorkflowContext(jobQueueName).getWorkflowState());

    cleanupParticipants(5);
  }

  /**
   * Tests that stopping a workflow does result in its task ending up in STOPPED state.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testStopWorkflow")
  public void testStopTask() throws Exception {
    stopTestSetup(1);

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    for (int i = 0; i < 1; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("StopTask", new HashMap<>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand("Dummy")
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(new HashMap<>());
      workflowBuilder.addJob("JOB" + i, jobConfigBulider);
    }

    _driver.start(workflowBuilder.build());
    _driver.pollForWorkflowState(workflowName, TaskState.IN_PROGRESS);

    // Stop the workflow
    _driver.stop(workflowName);
    _driver.pollForWorkflowState(workflowName, TaskState.STOPPED);

    Assert.assertEquals(TaskDriver.getWorkflowContext(_manager, workflowName).getWorkflowState(),
        TaskState.STOPPED);

    cleanupParticipants(1);
  }

  /**
   * Tests that stop() indeed frees up quotas for tasks belonging to the stopped workflow.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testStopTask")
  public void testStopTaskForQuota() throws Exception {
    stopTestSetup(1);

    String workflowNameToStop = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilderToStop = new Workflow.Builder(workflowNameToStop);
    WorkflowConfig.Builder configBuilderToStop = new WorkflowConfig.Builder(workflowNameToStop);
    configBuilderToStop.setAllowOverlapJobAssignment(true);
    workflowBuilderToStop.setWorkflowConfig(configBuilderToStop.build());

    // First create 50 jobs so that all 40 threads will be taken up
    for (int i = 0; i < 50; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("StopTask", new HashMap<>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand("Dummy")
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(new HashMap<>());
      workflowBuilderToStop.addJob("JOB" + i, jobConfigBulider);
    }

    _driver.start(workflowBuilderToStop.build());
    _driver.pollForWorkflowState(workflowNameToStop, TaskState.IN_PROGRESS);

    // Stop the workflow
    _driver.stop(workflowNameToStop);

    _driver.pollForWorkflowState(workflowNameToStop, TaskState.STOPPED);
    Assert.assertEquals(
        TaskDriver.getWorkflowContext(_manager, workflowNameToStop).getWorkflowState(),
        TaskState.STOPPED); // Check that the workflow has been stopped

    // Generate another workflow to be completed this time around
    String workflowToComplete = TestHelper.getTestMethodName() + "ToComplete";
    Workflow.Builder workflowBuilderToComplete = new Workflow.Builder(workflowToComplete);
    WorkflowConfig.Builder configBuilderToComplete = new WorkflowConfig.Builder(workflowToComplete);
    configBuilderToComplete.setAllowOverlapJobAssignment(true);
    workflowBuilderToComplete.setWorkflowConfig(configBuilderToComplete.build());

    // Create 20 jobs that should complete
    for (int i = 0; i < 20; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("CompleteTask", new HashMap<>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand("Dummy")
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(new HashMap<>());
      workflowBuilderToComplete.addJob("JOB" + i, jobConfigBulider);
    }

    // Start the workflow to be completed
    _driver.start(workflowBuilderToComplete.build());
    _driver.pollForWorkflowState(workflowToComplete, TaskState.COMPLETED);
    Assert.assertEquals(
        TaskDriver.getWorkflowContext(_manager, workflowToComplete).getWorkflowState(),
        TaskState.COMPLETED);

    cleanupParticipants(1);
  }

  /**
   * Test that there is no thread leak when stopping and resuming.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testStopTaskForQuota")
  public void testResumeTaskForQuota() throws Exception {
    stopTestSetup(1);

    String workflowName_1 = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder_1 = new Workflow.Builder(workflowName_1);
    WorkflowConfig.Builder configBuilder_1 = new WorkflowConfig.Builder(workflowName_1);
    configBuilder_1.setAllowOverlapJobAssignment(true);
    workflowBuilder_1.setWorkflowConfig(configBuilder_1.build());

    // 30 jobs run first
    for (int i = 0; i < 30; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("StopTask", new HashMap<>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand("Dummy")
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(new HashMap<>());
      workflowBuilder_1.addJob("JOB" + i, jobConfigBulider);
    }

    _driver.start(workflowBuilder_1.build());

    // Check the jobs are in progress and the tasks are running.
    // Each job has one task. Hence, we just check the state of the partition 0.
    for (int i = 0; i < 30; i++) {
      String jobName = workflowName_1 + "_JOB" + i;
      _driver.pollForJobState(workflowName_1, jobName, TaskState.IN_PROGRESS);
      boolean isTaskInRunningState = TestHelper.verify(() -> {
        JobContext jobContext = _driver.getJobContext(jobName);
        String state = jobContext.getMapField(0).get("STATE");
        return (state!= null && state.equals("RUNNING"));
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(isTaskInRunningState);
    }

    _driver.stop(workflowName_1);
    _driver.pollForWorkflowState(workflowName_1, TaskState.STOPPED);

    _driver.resume(workflowName_1);

    // Check the jobs are in progress and the tasks are running.
    // Each job has one task. Hence, we just check the state of the partition 0.
    for (int i = 0; i < 30; i++) {
      String jobName = workflowName_1 + "_JOB" + i;
      _driver.pollForJobState(workflowName_1, jobName, TaskState.IN_PROGRESS);
      boolean isTaskInRunningState = TestHelper.verify(() -> {
        JobContext jobContext = _driver.getJobContext(jobName);
        String state = jobContext.getMapField(0).get("STATE");
        return (state!= null && state.equals("RUNNING"));
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(isTaskInRunningState);
    }

    // By now there should only be 30 threads occupied

    String workflowName_2 = TestHelper.getTestMethodName() + "_2";
    Workflow.Builder workflowBuilder_2 = new Workflow.Builder(workflowName_2);
    WorkflowConfig.Builder configBuilder_2 = new WorkflowConfig.Builder(workflowName_2);
    configBuilder_2.setAllowOverlapJobAssignment(true);
    workflowBuilder_2.setWorkflowConfig(configBuilder_2.build());

    // Try to run 10 jobs that complete
    int numJobs = 10;
    for (int i = 0; i < numJobs; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("CompleteTask", new HashMap<>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand("Dummy")
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(new HashMap<>());
      workflowBuilder_2.addJob("JOB" + i, jobConfigBulider);
    }

    // If these jobs complete successfully, then that means there is no thread leak
    _driver.start(workflowBuilder_2.build());
    Assert.assertEquals(_driver.pollForWorkflowState(workflowName_2, TaskState.COMPLETED),
        TaskState.COMPLETED);

    cleanupParticipants(1);
  }

  /**
   * Sets up an environment to make stop task testing easy. Shuts down all Participants and starts
   * only one Participant.
   */
  private void stopTestSetup(int numNodes) {
    // Set task callbacks
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    TaskFactory taskFactory = StopTask::new;
    TaskFactory taskFactoryComplete = MockTask::new;
    taskFactoryReg.put("StopTask", taskFactory);
    taskFactoryReg.put("CompleteTask", taskFactoryComplete);

    stopParticipants();

    for (int i = 0; i < numNodes; i++) {
      String instanceName = _participants[i].getInstanceName();
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[i], taskFactoryReg));

      _participants[i].syncStart();
    }
  }

  private void cleanupParticipants(int numNodes) {
    for (int i = 0; i < numNodes; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
    }
  }

  /**
   * A mock task class that models a short-lived task to be stopped.
   */
  private class StopTask extends MockTask {
    private boolean _stopFlag = false;

    StopTask(TaskCallbackContext context) {
      super(context);
    }

    @Override
    public TaskResult run() {
      _stopFlag = false;
      while (!_stopFlag) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      // This wait is to prevent the task from completing before being stopped
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }

    @Override
    public void cancel() {
      _stopFlag = true;
    }
  }
}
