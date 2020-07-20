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

import java.util.concurrent.CountDownLatch;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;


public class TestStuckTaskQuota extends TaskTestBase {
  private CountDownLatch latch = new CountDownLatch(1);

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 2;
    super.beforeClass();

    // Stop participants that have been started in super class
    for (int i = 0; i < _numNodes; i++) {
      super.stopParticipant(i);
      Assert.assertFalse(_participants[i].isConnected());
    }
    _participants = new MockParticipantManager[_numNodes];

    // Start first participant
    startParticipantAndRegisterNewMockTask(0);
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
  }

  @Test
  public void testStuckTaskQuota() throws Exception {
    String workflowName1 = TestHelper.getTestMethodName() + "_1";
    String workflowName2 = TestHelper.getTestMethodName() + "_2";
    String workflowName3 = TestHelper.getTestMethodName() + "_3";
    String jobName = "JOB0";
    JobConfig.Builder jobBuilder1 =
        new JobConfig.Builder().setWorkflow(workflowName1).setNumberOfTasks(40)
            .setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    JobConfig.Builder jobBuilder2 = new JobConfig.Builder().setWorkflow(workflowName2)
        .setNumberOfTasks(1).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    JobConfig.Builder jobBuilder3 = new JobConfig.Builder().setWorkflow(workflowName3)
        .setNumberOfTasks(1).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName1).addJob(jobName, jobBuilder1);
    Workflow.Builder workflowBuilder2 =
        new Workflow.Builder(workflowName2).addJob(jobName, jobBuilder2);
    Workflow.Builder workflowBuilder3 =
        new Workflow.Builder(workflowName3).addJob(jobName, jobBuilder3);

    _driver.start(workflowBuilder1.build());

    // Make sure the JOB0 of workflow1 is started and all of the tasks are assigned to the
    // participant 0
    _driver.pollForJobState(workflowName1, TaskUtil.getNamespacedJobName(workflowName1, jobName),
        TaskState.IN_PROGRESS);

    String participant0 = PARTICIPANT_PREFIX + "_" + (_startPort + 0);
    for (int i = 0; i < 40; i++) {
      int finalI = i;
      Assert.assertTrue(TestHelper.verify(() -> (TaskPartitionState.RUNNING
          .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName1, jobName))
              .getPartitionState(finalI))
          && participant0
              .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName1, jobName))
                  .getAssignedParticipant(finalI))),
          TestHelper.WAIT_DURATION));
    }

    // Start the second participant
    startParticipantAndRegisterNewMockTask(1);

    _driver.start(workflowBuilder2.build());
    // Make sure the JOB0 of workflow2 is started and the only task of this job is assigned to
    // participant1
    _driver.pollForJobState(workflowName2, TaskUtil.getNamespacedJobName(workflowName2, jobName),
        TaskState.IN_PROGRESS);
    String participant1 = PARTICIPANT_PREFIX + "_" + (_startPort + 1);
    Assert.assertTrue(TestHelper.verify(() -> (TaskPartitionState.RUNNING.equals(_driver
        .getJobContext(TaskUtil.getNamespacedJobName(workflowName2, jobName)).getPartitionState(0))
        && participant1
            .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName2, jobName))
                .getAssignedParticipant(0))),
        TestHelper.WAIT_DURATION));

    // Delete the workflow1
    _driver.delete(workflowName1);

    // Since the tasks will be stuck for workflow1 after the deletion, the participant 0 is out of
    // capacity. Hence, the new tasks should be assigned to participant 1
    _driver.start(workflowBuilder3.build());

    // Make sure the JOB0 of workflow3 is started and the only task of this job is assigned to
    // participant1
    _driver.pollForJobState(workflowName3, TaskUtil.getNamespacedJobName(workflowName3, jobName),
        TaskState.IN_PROGRESS);

    Assert.assertTrue(TestHelper
        .verify(() -> (TaskPartitionState.RUNNING
            .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName3, jobName))
                .getPartitionState(0))),
            TestHelper.WAIT_DURATION)
        && participant1
            .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName3, jobName))
                .getAssignedParticipant(0)));
    latch.countDown();
    // Stop the workflow2 and workflow3
    _driver.waitToStop(workflowName2, 5000L);
    _driver.waitToStop(workflowName3, 5000L);
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
   * A mock task that extents MockTask class to count the number of cancel messages.
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
