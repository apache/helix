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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * This test checks the scheduling decision for the task that has already been assigned to an
 * instance and there exists a message pending for that task.
 */
public class TestUpdatePreviousAssignedTaskStatusWithPendingMessage {
  private static final String WORKFLOW_NAME = "TestWorkflow";
  private static final String INSTANCE_NAME = "TestInstance";
  private static final String JOB_NAME = "TestJob";
  private static final String PARTITION_NAME = "0";
  private static final String TARGET_RESOURCES = "TestDB";
  private static final int PARTITION_ID = 0;

  /**
   * Scenario:
   * JobState = TIMING_OUT
   * Task State: Context= INIT, CurrentState = INIT
   * Pending Message: FromState = INIT, ToState = RUNNING
   */
  @Test
  public void testTaskWithPendingMessageWhileJobTimingOut() {
    JobDispatcher jobDispatcher = new JobDispatcher();
    // Preparing the inputs
    Map<String, SortedSet<Integer>> currentInstanceToTaskAssignments = new HashMap<>();
    SortedSet<Integer> tasks = new TreeSet<>();
    tasks.add(PARTITION_ID);
    currentInstanceToTaskAssignments.put(INSTANCE_NAME, tasks);
    Map<Integer, AbstractTaskDispatcher.PartitionAssignment> paMap = new TreeMap<>();
    CurrentStateOutput currentStateOutput = prepareCurrentState(TaskPartitionState.INIT,
        TaskPartitionState.INIT, TaskPartitionState.RUNNING);
    JobContext jobContext = prepareJobContext(TaskPartitionState.INIT);
    JobConfig jobConfig = prepareJobConfig();
    Map<String, Set<Integer>> tasksToDrop = new HashMap<>();
    tasksToDrop.put(INSTANCE_NAME, new HashSet<>());
    WorkflowControllerDataProvider cache = new WorkflowControllerDataProvider();
    jobDispatcher.updatePreviousAssignedTasksStatus(currentInstanceToTaskAssignments,
        new HashSet<>(), JOB_NAME, currentStateOutput, jobContext, jobConfig, TaskState.TIMING_OUT,
        new HashMap<>(), new HashSet<>(), paMap, TargetState.STOP, new HashSet<>(), cache,
        tasksToDrop);
    Assert.assertEquals(paMap.get(0)._state, TaskPartitionState.INIT.name());
  }

  /**
   * Scenario:
   * JobState = IN_PROGRESS
   * Task State: Context= RUNNING, CurrentState = RUNNING
   * Pending Message: FromState = RUNNING, ToState = DROPPED
   */
  @Test
  public void testTaskWithPendingMessage() {
    JobDispatcher jobDispatcher = new JobDispatcher();
    // Preparing the inputs
    Map<String, SortedSet<Integer>> currentInstanceToTaskAssignments = new HashMap<>();
    SortedSet<Integer> tasks = new TreeSet<>();
    tasks.add(PARTITION_ID);
    currentInstanceToTaskAssignments.put(INSTANCE_NAME, tasks);
    Map<Integer, AbstractTaskDispatcher.PartitionAssignment> paMap = new TreeMap<>();
    CurrentStateOutput currentStateOutput = prepareCurrentState(TaskPartitionState.RUNNING,
        TaskPartitionState.RUNNING, TaskPartitionState.DROPPED);
    JobContext jobContext = prepareJobContext(TaskPartitionState.RUNNING);
    JobConfig jobConfig = prepareJobConfig();
    Map<String, Set<Integer>> tasksToDrop = new HashMap<>();
    tasksToDrop.put(INSTANCE_NAME, new HashSet<>());
    WorkflowControllerDataProvider cache = new WorkflowControllerDataProvider();
    jobDispatcher.updatePreviousAssignedTasksStatus(currentInstanceToTaskAssignments,
        new HashSet<>(), JOB_NAME, currentStateOutput, jobContext, jobConfig, TaskState.IN_PROGRESS,
        new HashMap<>(), new HashSet<>(), paMap, TargetState.START, new HashSet<>(), cache,
        tasksToDrop);
    Assert.assertEquals(paMap.get(0)._state, TaskPartitionState.DROPPED.name());
  }

  private JobConfig prepareJobConfig() {
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
    jobConfigBuilder.setWorkflow(WORKFLOW_NAME);
    jobConfigBuilder.setCommand("TestCommand");
    jobConfigBuilder.setJobId(JOB_NAME);
    List<String> targetPartition = new ArrayList<>();
    jobConfigBuilder.setTargetPartitions(targetPartition);
    List<TaskConfig> taskConfigs = new ArrayList<>();
    TaskConfig.Builder taskConfigBuilder = new TaskConfig.Builder();
    taskConfigBuilder.setTaskId("0");
    taskConfigs.add(taskConfigBuilder.build());
    jobConfigBuilder.addTaskConfigs(taskConfigs);
    return jobConfigBuilder.build();
  }

  private JobContext prepareJobContext(TaskPartitionState taskPartitionState) {
    ZNRecord record = new ZNRecord(JOB_NAME);
    JobContext jobContext = new JobContext(record);
    jobContext.setStartTime(0L);
    jobContext.setName(JOB_NAME);
    jobContext.setStartTime(0L);
    jobContext.setPartitionState(PARTITION_ID, taskPartitionState);
    jobContext.setPartitionTarget(PARTITION_ID, TARGET_RESOURCES + "_0");
    return jobContext;
  }

  private CurrentStateOutput prepareCurrentState(TaskPartitionState currentState,
      TaskPartitionState messageFromState, TaskPartitionState messageToState) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setResourceStateModelDef(JOB_NAME, "TASK");
    Partition taskPartition = new Partition(JOB_NAME + "_" + PARTITION_NAME);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition, INSTANCE_NAME, currentState.name());
    Message message = new Message(Message.MessageType.STATE_TRANSITION, "123456789");
    message.setFromState(messageFromState.name());
    message.setToState(messageToState.name());
    currentStateOutput.setPendingMessage(JOB_NAME, taskPartition, INSTANCE_NAME, message);
    return currentStateOutput;
  }
}
