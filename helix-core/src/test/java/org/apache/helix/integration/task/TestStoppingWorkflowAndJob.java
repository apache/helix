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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.CountDownLatch;

import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * Test to check if workflow correctly behaves when it's target state is set to STOP and the tasks
 * have some delay in their cancellation.
 */
public class TestStoppingWorkflowAndJob extends TaskTestBase {
  private static final String DATABASE = WorkflowGenerator.DEFAULT_TGT_DB;
  private CountDownLatch latch = new CountDownLatch(1);

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 3;
    super.beforeClass();

    // Stop participants that have been started in super class
    for (int i = 0; i < _numNodes; i++) {
      super.stopParticipant(i);
      Assert.assertFalse(_participants[i].isConnected());
    }

    // Start new participants that have new TaskStateModel (NewMockTask) information
    _participants = new MockParticipantManager[_numNodes];
    for (int i = 0; i < _numNodes; i++) {
      startParticipantAndRegisterNewMockTask(i);
    }
  }

  @Test
  public void testStoppingQueueFailToStop() throws Exception {
    // Test to check if waitToStop method correctly throws an Exception if Queue stuck in STOPPING
    // state.
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder0 =
        new JobConfig.Builder().setWorkflow(jobQueueName).setTargetResource(DATABASE)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("JOB0", jobBuilder0);
    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"),
        TaskState.IN_PROGRESS);
    boolean exceptionHappened = false;
    try {
      _driver.waitToStop(jobQueueName, 5000L);
    } catch (HelixException e) {
      exceptionHappened = true;
    }
    _driver.pollForWorkflowState(jobQueueName, TaskState.STOPPING);
    Assert.assertTrue(exceptionHappened);
    latch.countDown();
  }

  @Test(dependsOnMethods = "testStoppingQueueFailToStop")
  public void testStopWorkflowInstanceOffline() throws Exception {
    // Test to check if workflow and jobs go to STOPPED state when the assigned participants of the
    // tasks are not live anymore.
    latch = new CountDownLatch(1);
    int numberOfTasks = 10;

    // Stop all participant except participant0
    for (int i = 1; i < _numNodes; i++) {
      super.stopParticipant(i);
      Assert.assertFalse(_participants[i].isConnected());
    }

    // Start a new workflow
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setWorkflow(workflowName).setNumberOfTasks(numberOfTasks)
            .setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder);

    // Start the workflow and make sure it goes to IN_PROGRESS state.
    _driver.start(workflowBuilder.build());
    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    // Make sure all of the tasks within the job are assigned and have RUNNING state
    String participant0 = PARTICIPANT_PREFIX + "_" + (_startPort + 0);
    for (int i = 0; i < numberOfTasks; i++) {
      final int taskNumber = i;
      Assert.assertTrue(TestHelper.verify(() -> (TaskPartitionState.RUNNING
          .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName))
              .getPartitionState(taskNumber))
          && participant0
              .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName))
                  .getAssignedParticipant(taskNumber))),
          TestHelper.WAIT_DURATION));
    }

    // STOP the workflow and make sure it goes to STOPPING state
    _driver.stop(workflowName);
    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.STOPPING);

    // Stop the remaining participant and the controller
    _controller.syncStop();
    super.stopParticipant(0);

    // Start the controller and make sure the workflow and job go to STOPPED state
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX);
    _controller.syncStart();

    // Start other participants to allow controller process the workflows. Otherwise due to no
    // global capacity, the workflows will not be processed
    for (int i = 1; i < _numNodes; i++) {
      startParticipantAndRegisterNewMockTask(i);
    }

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.STOPPED);
    _driver.pollForWorkflowState(workflowName, TaskState.STOPPED);

    // Make sure all of the tasks within the job are assigned and have DROPPED state
    for (int i = 0; i < numberOfTasks; i++) {
      final int taskNumber = i;
      Assert.assertTrue(TestHelper.verify(() -> (TaskPartitionState.DROPPED
          .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName))
              .getPartitionState(taskNumber))
          && participant0
              .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName))
                  .getAssignedParticipant(taskNumber))),
          TestHelper.WAIT_DURATION));
    }
    latch.countDown();
  }

  private void startParticipantAndRegisterNewMockTask(int participantIndex) {
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put(NewMockTask.TASK_COMMAND, NewMockTask::new);
    String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + participantIndex);
    _participants[participantIndex] =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

    // Register a Task state model factory.
    StateMachineEngine stateMachine = _participants[participantIndex].getStateMachineEngine();
    stateMachine.registerStateModelFactory("Task",
        new TaskStateModelFactory(_participants[participantIndex], taskFactoryReg));
    _participants[participantIndex].syncStart();
  }

  /**
   * A mock task that extents MockTask class and stuck in running when cancel is called.
   */
  private class NewMockTask extends MockTask {

    NewMockTask(TaskCallbackContext context) {
      super(context);
    }

    @Override
    public void cancel() {
      try {
        latch.await();
      } catch (Exception e) {
        // Pass
      }
      super.cancel();
    }
  }
}
