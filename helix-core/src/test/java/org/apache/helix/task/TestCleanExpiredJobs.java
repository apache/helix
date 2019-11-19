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

import java.util.HashSet;
import java.util.Set;

import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCleanExpiredJobs extends TaskSynchronizedTestBase {
  private WorkflowControllerDataProvider _cache;

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  @Test
  public void testCleanExpiredJobs() throws Exception {
    String queue = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queue);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG).setExpiry(1L);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 8; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
    }

    for (int i = 0; i < 8; i++) {
      TaskUtil.setJobContext(_manager, TaskUtil.getNamespacedJobName(queue, "JOB" + i),
          TaskTestUtil.buildJobContext(startTime, startTime, TaskPartitionState.COMPLETED));
    }

    for (int i = 4; i < 6; i++) {
      TaskUtil.setJobContext(_manager, TaskUtil.getNamespacedJobName(queue, "JOB" + i),
          TaskTestUtil
              .buildJobContext(startTime, startTime + 100000, TaskPartitionState.COMPLETED));
    }

    WorkflowContext workflowContext = TaskTestUtil
        .buildWorkflowContext(queue, TaskState.IN_PROGRESS, null, TaskState.COMPLETED,
            TaskState.FAILED, TaskState.ABORTED, TaskState.COMPLETED, TaskState.COMPLETED,
            TaskState.COMPLETED, TaskState.IN_PROGRESS, TaskState.NOT_STARTED);

    Set<String> jobsLeft = new HashSet<String>();
    jobsLeft.add(TaskUtil.getNamespacedJobName(queue, "JOB" + 1));
    jobsLeft.add(TaskUtil.getNamespacedJobName(queue, "JOB" + 2));
    jobsLeft.add(TaskUtil.getNamespacedJobName(queue, "JOB" + 4));
    jobsLeft.add(TaskUtil.getNamespacedJobName(queue, "JOB" + 5));
    jobsLeft.add(TaskUtil.getNamespacedJobName(queue, "JOB" + 6));
    jobsLeft.add(TaskUtil.getNamespacedJobName(queue, "JOB" + 7));

    _driver.start(builder.build());
    _cache = TaskTestUtil.buildDataProvider(_manager.getHelixDataAccessor(), CLUSTER_NAME);
    TaskUtil.setWorkflowContext(_manager, queue, workflowContext);
    TaskTestUtil.calculateTaskSchedulingStage(_cache, _manager);
    Thread.sleep(500);
    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(queue);
    Assert.assertEquals(workflowConfig.getJobDag().getAllNodes(), jobsLeft);
    _cache.requireFullRefresh();
    _cache.refresh(_manager.getHelixDataAccessor());
    TaskTestUtil.calculateTaskSchedulingStage(_cache, _manager);
    Thread.sleep(500);
    workflowContext = _driver.getWorkflowContext(queue);
    Assert.assertTrue(workflowContext.getLastJobPurgeTime() > startTime
        && workflowContext.getLastJobPurgeTime() < System.currentTimeMillis());
  }

  @Test
  void testNotCleanJobsDueToParentFail() throws Exception {
    String queue = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queue);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG).setExpiry(1L);

    long startTime = System.currentTimeMillis();
    builder.enqueueJob("JOB0", jobBuilder);
    builder.enqueueJob("JOB1", jobBuilder);
    builder.addParentChildDependency("JOB0", "JOB1");
    TaskUtil.setJobContext(_manager, TaskUtil.getNamespacedJobName(queue, "JOB0"),
        TaskTestUtil.buildJobContext(startTime, startTime, TaskPartitionState.COMPLETED));

    WorkflowContext workflowContext = TaskTestUtil
        .buildWorkflowContext(queue, TaskState.IN_PROGRESS, null, TaskState.FAILED,
            TaskState.FAILED);
    _driver.start(builder.build());
    _cache = TaskTestUtil.buildDataProvider(_manager.getHelixDataAccessor(), CLUSTER_NAME);
    TaskUtil.setWorkflowContext(_manager, queue, workflowContext);
    TaskTestUtil.calculateTaskSchedulingStage(_cache, _manager);
    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(queue);
    Assert.assertEquals(workflowConfig.getJobDag().getAllNodes().size(), 2);
  }

  @Test
  void testNotCleanJobsThroughEnqueueJob() throws Exception {
    int capacity = 5;
    String queue = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queue, capacity);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG).setExpiry(1L);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < capacity; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
    }

    _driver.start(builder.build());
    try {
      // should fail here since the queue is full.
      _driver.enqueueJob(queue, "JOB" + capacity, jobBuilder);
      Assert.fail("Queue is not full.");
    } catch (HelixException e) {
      Assert.assertTrue(e.getMessage().contains("queue is full"));
    }

    for (int i = 0; i < capacity; i++) {
      TaskUtil.setJobContext(_manager, TaskUtil.getNamespacedJobName(queue, "JOB" + i),
          TaskTestUtil.buildJobContext(startTime, startTime, TaskPartitionState.COMPLETED));
    }

    WorkflowContext workflowContext = TaskTestUtil
        .buildWorkflowContext(queue, TaskState.IN_PROGRESS, null, TaskState.COMPLETED,
            TaskState.COMPLETED, TaskState.FAILED, TaskState.IN_PROGRESS);
    TaskUtil.setWorkflowContext(_manager, queue, workflowContext);

    _driver.enqueueJob(queue, "JOB" + capacity, jobBuilder);

    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(queue);
    Assert.assertEquals(workflowConfig.getJobDag().getAllNodes().size(), capacity - 1);
  }
}
