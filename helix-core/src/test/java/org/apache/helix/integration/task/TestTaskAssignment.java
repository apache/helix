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
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskAssignment extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    _numNodes = 2;
    _instanceGroupTag = true;
    super.beforeClass();
  }

  @Test
  public void testTaskAssignment() throws InterruptedException {
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_" + (_startPort + 0), false);
    String jobResource = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB);

    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();
    _driver.start(flow);

    // Wait 1 sec. The task should not be complete since it is not assigned.
    Thread.sleep(1000L);

    // The task is not assigned so the task state should be null in this case.
    Assert.assertNull(
        _driver.getJobContext(TaskUtil.getNamespacedJobName(jobResource)).getPartitionState(0));
  }

  @Test
  public void testGenericTaskInstanceGroup() throws InterruptedException {
    // Disable the only instance can be assigned.
    String queueName = TestHelper.getTestMethodName();
    String jobName = "Job4InstanceGroup";
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName);
    JobConfig.Builder jobConfig = new JobConfig.Builder();

    List<TaskConfig> taskConfigs = new ArrayList<TaskConfig>();
    int num_tasks = 3;
    for (int j = 0; j < num_tasks; j++) {
      taskConfigs.add(
          new TaskConfig.Builder().setTaskId("task_" + j).setCommand(MockTask.TASK_COMMAND)
              .build());
    }

    jobConfig.addTaskConfigs(taskConfigs);
    jobConfig.setInstanceGroupTag("TESTTAG1");

    queueBuilder.enqueueJob(jobName, jobConfig);
    _driver.start(queueBuilder.build());

    // Wait 1 sec. The task should not be complete since it is not assigned.
    Thread.sleep(1000L);

    // The task is not assigned so the task state should be null in this case.
    String namedSpaceJob = TaskUtil.getNamespacedJobName(queueName, jobName);

    Assert.assertEquals(_driver.getJobContext(namedSpaceJob).getAssignedParticipant(0),
        _participants[1].getInstanceName());
  }
}
