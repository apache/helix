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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Once a job is FAILING, the tasks that are in flight are aborted and the job becomes FAILED as
 * soon as they are done. The tasks that have not been started yet must not be scheduled anymore.
 */
public class TestJobFailingNoNewTasks extends TaskTestBase {
  private static final String TASK_COMMAND = "FailFirstTask";
  private static final int NUM_TASKS = 60;
  // One task per instance can run at the same time.
  private static final int NUM_SLOTS = 5;
  private static final AtomicInteger NUM_STARTED_TASKS = new AtomicInteger(0);

  @Override
  protected void startParticipant(String zkAddr, int i) {
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put(TASK_COMMAND, context -> new FailFirstTask());
    String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
    _participants[i] = new MockParticipantManager(zkAddr, CLUSTER_NAME, instanceName);
    StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
    stateMachine.registerStateModelFactory(TaskConstants.STATE_MODEL_NAME,
        new TaskStateModelFactory(_participants[i], taskFactoryReg));
    _participants[i].syncStart();
  }

  @Test
  public void testNoNewTasksScheduledWhileJobIsFailing() throws Exception {
    Assert.assertEquals(_numNodes, NUM_SLOTS);
    final String workflowName = TestHelper.getTestMethodName();
    final String jobName = "testJob";

    // Many more tasks than the cluster can run at the same time.
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < NUM_TASKS; i++) {
      taskConfigs.add(new TaskConfig(TASK_COMMAND, new HashMap<>()));
    }
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setWorkflow(workflowName).setCommand(TASK_COMMAND)
            .addTaskConfigs(taskConfigs).setNumConcurrentTasksPerInstance(1)
            .setFailureThreshold(0);
    _driver.start(new Workflow.Builder(workflowName).addJob(jobName, jobBuilder).build());

    String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    _driver.pollForJobState(workflowName, namespacedJobName, TaskState.FAILED);
    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);

    // Only the tasks that were scheduled before the job started failing may have run. Without
    // the fix, the slot of the failed task was filled again on every pipeline while the job was
    // FAILING, and each of those tasks was started on a participant only to be aborted.
    Assert.assertTrue(NUM_STARTED_TASKS.get() <= NUM_SLOTS, String
        .format("%d tasks were started, but only %d can run at the same time",
            NUM_STARTED_TASKS.get(), NUM_SLOTS));
    JobContext jobContext = _driver.getJobContext(namespacedJobName);
    int numScheduled = 0;
    for (int pId : jobContext.getPartitionSet()) {
      if (jobContext.getPartitionState(pId) != null) {
        numScheduled++;
      }
    }
    Assert.assertTrue(numScheduled <= NUM_SLOTS,
        String.format("%d tasks were scheduled, expected at most %d", numScheduled, NUM_SLOTS));
  }

  /**
   * The behavior of a task depends on the order in which the tasks are started, so that the test
   * does not depend on which tasks the controller picks first:
   * - the first task fails right away, which makes the job FAILING;
   * - the other tasks of the first batch ignore the cancellation for a while, which keeps the job
   *   FAILING for several pipelines while the slot of the failed task is free;
   * - any later task (there is none unless the controller keeps scheduling) honors the
   *   cancellation right away.
   */
  private static class FailFirstTask implements Task {
    private static final long FAIL_DELAY_MS = 300L;
    private static final long UNCANCELABLE_MS = 3000L;
    private static final long MAX_RUN_MS = 30000L;
    private volatile boolean _canceled = false;

    @Override
    public TaskResult run() {
      int order = NUM_STARTED_TASKS.getAndIncrement();
      try {
        if (order == 0) {
          Thread.sleep(FAIL_DELAY_MS);
          return new TaskResult(TaskResult.Status.FATAL_FAILED, "The first task fails");
        }
        long start = System.currentTimeMillis();
        long uncancelableUntil = start + (order < NUM_SLOTS ? UNCANCELABLE_MS : 0L);
        while (System.currentTimeMillis() < start + MAX_RUN_MS) {
          if (_canceled && System.currentTimeMillis() >= uncancelableUntil) {
            return new TaskResult(TaskResult.Status.CANCELED, "Canceled");
          }
          Thread.sleep(10L);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return new TaskResult(TaskResult.Status.CANCELED, "Interrupted");
      }
      return new TaskResult(TaskResult.Status.COMPLETED, "Completed");
    }

    @Override
    public void cancel() {
      _canceled = true;
    }
  }
}
