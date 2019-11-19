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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestNoDoubleAssign extends TaskTestBase {
  private static final int THREAD_COUNT = 10;
  private static final long CONNECTION_DELAY = 100L;
  private static final long POLL_DELAY = 50L;
  private static final String TASK_DURATION = "200";
  private static final Random RANDOM = new Random();

  private ScheduledExecutorService _executorServicePoll;
  private ScheduledExecutorService _executorServiceConnection;
  private AtomicBoolean _existsDoubleAssign = new AtomicBoolean(false);
  private Set<String> _jobNames = new HashSet<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 0;
    _numPartitions = 0;
    _numReplicas = 0;
    super.beforeClass();
  }

  /**
   * Tests that no Participants have more tasks from the same job than is specified in the config.
   * (MaxConcurrentTaskPerInstance, default value = 1)
   * NOTE: this test is supposed to generate a lot of Participant-side ERROR message (ZkClient
   * already closed!) because we are disconnecting them on purpose.
   */
  @Test
  public void testNoDoubleAssign() throws InterruptedException {
    // Some arbitrary workload that creates a reasonably large amount of tasks
    int workload = 10;

    // Create a workflow with jobs and tasks
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    for (int i = 0; i < workload; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      for (int j = 0; j < workload; j++) {
        String taskID = "JOB_" + i + "_TASK_" + j;
        TaskConfig.Builder taskConfigBuilder = new TaskConfig.Builder();
        taskConfigBuilder.setTaskId(taskID).setCommand(MockTask.TASK_COMMAND)
            .addConfig(MockTask.JOB_DELAY, TASK_DURATION);
        taskConfigs.add(taskConfigBuilder.build());
      }
      String jobName = "JOB_" + i;
      _jobNames.add(workflowName + "_" + jobName); // Add the namespaced job name
      JobConfig.Builder jobBuilder =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(10000)
              .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG)
              .addTaskConfigs(taskConfigs).setIgnoreDependentJobFailure(true)
              .setFailureThreshold(100000)
              .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, TASK_DURATION));
      builder.addJob(jobName, jobBuilder);
    }
    // Start the workflow
    _driver.start(builder.build());
    _driver.pollForWorkflowState(workflowName, TaskState.IN_PROGRESS);
    breakConnection();
    pollForDoubleAssign();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    // Shut down thread pools
    _executorServicePoll.shutdown();
    _executorServiceConnection.shutdown();
    try {
      if (!_executorServicePoll.awaitTermination(180, TimeUnit.SECONDS)) {
        _executorServicePoll.shutdownNow();
      }
      if (!_executorServiceConnection.awaitTermination(180, TimeUnit.SECONDS)) {
        _executorServiceConnection.shutdownNow();
      }
    } catch (InterruptedException e) {
      _executorServicePoll.shutdownNow();
      _executorServiceConnection.shutdownNow();
    }
    Assert.assertFalse(_existsDoubleAssign.get());
  }

  /**
   * Fetch the JobContext for all jobs in ZK and check that no two tasks are running on the same
   * Participant.
   */
  private void pollForDoubleAssign() {
    _executorServicePoll = Executors.newScheduledThreadPool(THREAD_COUNT);
    _executorServicePoll.scheduleAtFixedRate(() -> {
      if (!_existsDoubleAssign.get()) {
        // Get JobContexts and test that they are assigned to disparate Participants
        for (String job : _jobNames) {
          JobContext jobContext = _driver.getJobContext(job);
          if (jobContext == null) {
            continue;
          }
          Set<String> instanceCache = new HashSet<>();
          for (int partition : jobContext.getPartitionSet()) {
            if (jobContext.getPartitionState(partition) == TaskPartitionState.RUNNING) {
              String assignedParticipant = jobContext.getAssignedParticipant(partition);
              if (assignedParticipant != null) {
                if (instanceCache.contains(assignedParticipant)) {
                  // Two tasks running on the same instance at the same time
                  _existsDoubleAssign.set(true);
                  return;
                }
                instanceCache.add(assignedParticipant);
              }
            }
          }
        }
      }
    }, 0L, POLL_DELAY, TimeUnit.MILLISECONDS);
  }

  /**
   * Randomly causes Participants to lost connection temporarily.
   */
  private void breakConnection() {
    _executorServiceConnection = Executors.newScheduledThreadPool(THREAD_COUNT);
    _executorServiceConnection.scheduleAtFixedRate(() -> {
      // Randomly pick a Participant and cause a transient connection issue
      int participantIndex = RANDOM.nextInt(_numNodes);
      _participants[participantIndex].disconnect();
      startParticipant(participantIndex);
    }, 0L, CONNECTION_DELAY, TimeUnit.MILLISECONDS);
  }
}
