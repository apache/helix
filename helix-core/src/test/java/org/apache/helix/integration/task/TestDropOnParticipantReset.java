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
import java.util.List;
import org.apache.helix.TestHelper;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestDropOnParticipantReset extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 0;
    _numPartitions = 0;
    _numReplicas = 0;
    _numNodes = 1; // Only bring up 1 instance
    super.beforeClass();
  }

  /**
   * Tests that upon Participant reconnect, the Controller correctly sends the current
   * state of the task partition to DROPPED. This is to avoid a resource leak in case of a
   * Participant disconnect/reconnect and an ensuing reset() on all of the partitions on that
   * Participant.
   */
  @Test
  public void testDropOnParticipantReset() throws InterruptedException {
    // Create a workflow with some long-running jobs in progress
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB";
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int j = 0; j < 2; j++) { // 2 tasks to ensure that they execute
      String taskID = jobName + "_TASK_" + j;
      TaskConfig.Builder taskConfigBuilder = new TaskConfig.Builder();
      taskConfigBuilder.setTaskId(taskID).setCommand(MockTask.TASK_COMMAND)
          .addConfig(MockTask.JOB_DELAY, "3000");
      taskConfigs.add(taskConfigBuilder.build());
    }
    JobConfig.Builder jobBuilder = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setMaxAttemptsPerTask(10).setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG)
        .addTaskConfigs(taskConfigs).setIgnoreDependentJobFailure(true)
        // 1 task at a time
        .setNumConcurrentTasksPerInstance(1);
    builder.addJob(jobName, jobBuilder);

    // Modify maxConcurrentTask for the instance so that it only accepts 1 task at most
    InstanceConfig instanceConfig = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(CLUSTER_NAME, _participants[0].getInstanceName());
    instanceConfig.setMaxConcurrentTask(1);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME,
        _participants[0].getInstanceName(), instanceConfig);

    // Start the workflow
    _driver.start(builder.build());
    _driver.pollForJobState(workflowName, workflowName + "_" + jobName, TaskState.IN_PROGRESS);
    Thread.sleep(1500L); // Wait for the Participant to process the message
    // Stop and start the participant to mimic a connection issue
    _participants[0].syncStop();
    // Upon starting the participant, the first task partition should be dropped and assigned anew
    // on the instance. Then the rest of the tasks will execute and the workflow will complete
    startParticipant(0);

    TaskState workflowState = _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);
    TaskState jobState =
        _driver.pollForJobState(workflowName, workflowName + "_" + jobName, TaskState.COMPLETED);
    Assert.assertEquals(workflowState, TaskState.COMPLETED);
    Assert.assertEquals(jobState, TaskState.COMPLETED);
  }
}
