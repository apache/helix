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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
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
  // How long the tasks of the first batch ignore the cancellation after the first failure.
  private static final long UNCANCELABLE_MS = 10000L;
  private static final AtomicInteger NUM_STARTED_TASKS = new AtomicInteger(0);
  // When the first task failed, 0 if it has not failed yet.
  private static final AtomicLong FIRST_FAILURE_TIME = new AtomicLong(0L);

  @Override
  protected Map<String, TaskFactory> createTaskFactoryRegistry() {
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put(TASK_COMMAND, context -> new FailFirstTask());
    return taskFactoryReg;
  }

  @Test
  public void testNoNewTasksScheduledWhileJobIsFailing() throws Exception {
    Assert.assertEquals(_numNodes, NUM_SLOTS);
    resetTaskCounters();
    final String workflowName = TestHelper.getTestMethodName();
    final String jobName = "testJob";

    _driver.start(new Workflow.Builder(workflowName).addJob(jobName, createJobBuilder(workflowName))
        .build());

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
   * The instance of a task that is in flight is disabled while the job is FAILING. The controller
   * does not go through the tasks of a disabled instance anymore, so the task is never aborted
   * like the others; it has to be dropped for the job to become FAILED.
   */
  @Test(dependsOnMethods = "testNoNewTasksScheduledWhileJobIsFailing")
  public void testFailingJobFinishesWhenInstanceOfRunningTaskIsDisabled() throws Exception {
    resetTaskCounters();
    final String workflowName = TestHelper.getTestMethodName();
    final String jobName = "testJob";
    _driver.start(new Workflow.Builder(workflowName).addJob(jobName, createJobBuilder(workflowName))
        .build());

    String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    _driver.pollForJobState(workflowName, namespacedJobName, TaskState.FAILING);

    // The tasks of the first batch ignore the cancellation for a while after the first failure,
    // so some of them are still running. The context may not show them as RUNNING right away.
    AtomicReference<String> runningTaskInstance = new AtomicReference<>();
    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext = _driver.getJobContext(namespacedJobName);
      for (int pId : jobContext.getPartitionSet()) {
        if (jobContext.getPartitionState(pId) == TaskPartitionState.RUNNING) {
          runningTaskInstance.set(jobContext.getAssignedParticipant(pId));
          return true;
        }
      }
      return false;
    }, UNCANCELABLE_MS / 2), "No task is running while the job is FAILING");
    String instanceOfRunningTask = runningTaskInstance.get();
    try {
      _gSetupTool.getClusterManagementTool()
          .enableInstance(CLUSTER_NAME, instanceOfRunningTask, false);
      _driver.pollForJobState(workflowName, namespacedJobName, TaskState.FAILED);
      _driver.pollForWorkflowState(workflowName, TaskState.FAILED);
    } finally {
      _gSetupTool.getClusterManagementTool()
          .enableInstance(CLUSTER_NAME, instanceOfRunningTask, true);
    }
  }

  private static void resetTaskCounters() {
    NUM_STARTED_TASKS.set(0);
    FIRST_FAILURE_TIME.set(0L);
  }

  private JobConfig.Builder createJobBuilder(String workflowName) {
    // Many more tasks than the cluster can run at the same time.
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < NUM_TASKS; i++) {
      taskConfigs.add(new TaskConfig(TASK_COMMAND, new HashMap<>()));
    }
    return new JobConfig.Builder().setWorkflow(workflowName).setCommand(TASK_COMMAND)
        .addTaskConfigs(taskConfigs).setNumConcurrentTasksPerInstance(1).setFailureThreshold(0);
  }

  /**
   * The behavior of a task depends on the order in which the tasks are started, so that the test
   * does not depend on which tasks the controller picks first:
   * - the first task fails once all the slots are busy, which makes the job FAILING;
   * - the other tasks of the first batch ignore the cancellation for a while after that failure,
   *   which keeps the job FAILING for several pipelines while the slot of the failed task is free;
   * - any later task (there is none unless the controller keeps scheduling) honors the
   *   cancellation right away.
   */
  private static class FailFirstTask implements Task {
    private static final long FAIL_DELAY_MS = 300L;
    private static final long WAIT_FOR_FIRST_BATCH_MS = 20000L;
    private static final long MAX_RUN_MS = 120000L;
    private volatile boolean _canceled = false;

    @Override
    public TaskResult run() {
      int order = NUM_STARTED_TASKS.getAndIncrement();
      try {
        if (order == 0) {
          long deadline = System.currentTimeMillis() + WAIT_FOR_FIRST_BATCH_MS;
          while (NUM_STARTED_TASKS.get() < NUM_SLOTS && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
          }
          Thread.sleep(FAIL_DELAY_MS);
          FIRST_FAILURE_TIME.set(System.currentTimeMillis());
          return new TaskResult(TaskResult.Status.FATAL_FAILED, "The first task fails");
        }
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + MAX_RUN_MS) {
          if (_canceled && (order >= NUM_SLOTS || isUncancelableWindowOver())) {
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

    private static boolean isUncancelableWindowOver() {
      long firstFailureTime = FIRST_FAILURE_TIME.get();
      return firstFailureTime > 0
          && System.currentTimeMillis() >= firstFailureTime + UNCANCELABLE_MS;
    }

    @Override
    public void cancel() {
      _canceled = true;
    }
  }
}
