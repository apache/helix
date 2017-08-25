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

import org.apache.helix.TestHelper;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCleanExpiredJobs extends TaskSynchronizedTestBase {
  private ClusterDataCache _cache;

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  @Test
  public void testCleanExpiredJobs() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(workflowName);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG).setExpiry(1L);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 5; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
      TaskUtil.setJobContext(_manager, TaskUtil.getNamespacedJobName(workflowName, "JOB" + i),
          TaskTestUtil.buildJobContext(startTime, startTime, TaskPartitionState.COMPLETED));
    }

    WorkflowContext workflowContext = TaskTestUtil
        .buildWorkflowContext(workflowName, TaskState.IN_PROGRESS, null, TaskState.COMPLETED,
            TaskState.FAILED, TaskState.ABORTED, TaskState.IN_PROGRESS, TaskState.NOT_STARTED);
    _driver.start(builder.build());
    _cache = TaskTestUtil.buildClusterDataCache(_manager.getHelixDataAccessor());
    TaskUtil.setWorkflowContext(_manager, workflowName, workflowContext);
    TaskTestUtil.calculateBestPossibleState(_cache, _manager);
    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(workflowName);
    Assert.assertEquals(workflowConfig.getJobDag().getAllNodes().size(), 3);
  }

  @Test void testNotCleanJobsDueToParentFail() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(workflowName);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG).setExpiry(1L);

    long startTime = System.currentTimeMillis();
    builder.enqueueJob("JOB0", jobBuilder);
    builder.enqueueJob("JOB1", jobBuilder);
    builder.addParentChildDependency("JOB0", "JOB1");
    TaskUtil.setJobContext(_manager, TaskUtil.getNamespacedJobName(workflowName, "JOB0"),
        TaskTestUtil.buildJobContext(startTime, startTime, TaskPartitionState.COMPLETED));

    WorkflowContext workflowContext = TaskTestUtil
        .buildWorkflowContext(workflowName, TaskState.IN_PROGRESS, null, TaskState.FAILED,
            TaskState.FAILED);
    _driver.start(builder.build());
    _cache = TaskTestUtil.buildClusterDataCache(_manager.getHelixDataAccessor());
    TaskUtil.setWorkflowContext(_manager, workflowName, workflowContext);
    TaskTestUtil.calculateBestPossibleState(_cache, _manager);
    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(workflowName);
    Assert.assertEquals(workflowConfig.getJobDag().getAllNodes().size(), 1);
  }
}
