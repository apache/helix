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

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestJobQueueCleanUp extends TaskTestBase {
  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  @Test
  public void testJobQueueCleanUp() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.SUCCESS_COUNT_BEFORE_FAIL, "2"));
    for (int i = 0; i < 5; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
    }
    _driver.start(builder.build());
    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + 4),
        TaskState.FAILED);
    _driver.cleanupQueue(queueName);
    Assert.assertEquals(_driver.getWorkflowConfig(queueName).getJobDag().size(), 0);
  }

  @Test(dependsOnMethods = "testJobQueueCleanUp")
  public void testJobQueueNotCleanupRunningJobs() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2);
    for (int i = 0; i < 3; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
    }
    builder.enqueueJob("JOB" + 3,
        jobBuilder.setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000000")));
    builder.enqueueJob("JOB" + 4, jobBuilder);
    _driver.start(builder.build());
    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + 3),
        TaskState.IN_PROGRESS);
    _driver.cleanupQueue(queueName);
    Assert.assertEquals(_driver.getWorkflowConfig(queueName).getJobDag().size(), 2);
  }

  @Test(dependsOnMethods = "testJobQueueNotCleanupRunningJobs")
  public void testJobQueueAutoCleanUp() throws Exception {
    int capacity = 10;
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName, capacity);
    WorkflowConfig.Builder cfgBuilder = new WorkflowConfig.Builder(builder.getWorkflowConfig());
    cfgBuilder.setJobPurgeInterval(1000);
    builder.setWorkflowConfig(cfgBuilder.build());

    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2).setJobCommandConfigMap(
            ImmutableMap.of(MockTask.SUCCESS_COUNT_BEFORE_FAIL, String.valueOf(capacity / 2)))
            .setExpiry(200L);
    Set<String> deletedJobs = new HashSet<String>();
    Set<String> remainJobs = new HashSet<String>();
    for (int i = 0; i < capacity; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
      if (i < capacity/2) {
        deletedJobs.add("JOB" + i);
      } else {
        remainJobs.add(TaskUtil.getNamespacedJobName(queueName, "JOB" + i));
      }
    }
    _driver.start(builder.build());
    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + (capacity - 1)), TaskState.FAILED);

    Assert
        .assertTrue(TestHelper.verify(() -> {
          WorkflowConfig config = _driver.getWorkflowConfig(queueName);
          return config.getJobDag().getAllNodes().equals(remainJobs);
        }, TestHelper.WAIT_DURATION));

    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowContext context = _driver.getWorkflowContext(queueName);
      return context.getJobStates().keySet().equals(remainJobs) && remainJobs
          .containsAll(context.getJobStartTimes().keySet());
    }, TestHelper.WAIT_DURATION));

    for (String job : deletedJobs) {
      JobConfig cfg = _driver.getJobConfig(job);
      JobContext ctx = _driver.getJobContext(job);
      Assert.assertNull(cfg);
      Assert.assertNull(ctx);
    }

  }

  @Test(dependsOnMethods = "testJobQueueAutoCleanUp")
  public void testJobQueueFailedCleanUp() throws Exception {
    int capacity = 10;
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName, capacity);
    WorkflowConfig.Builder cfgBuilder = new WorkflowConfig.Builder(builder.getWorkflowConfig());
    cfgBuilder.setJobPurgeInterval(1000);
    builder.setWorkflowConfig(cfgBuilder.build());

    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2).setJobCommandConfigMap(
            ImmutableMap.of(MockTask.SUCCESS_COUNT_BEFORE_FAIL, "0"))
            .setExpiry(200L).setTerminalStateExpiry(200L);
    for (int i = 0; i < capacity; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
    }
    _driver.start(builder.build());

    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowConfig config = _driver.getWorkflowConfig(queueName);
      return config.getJobDag().getAllNodes().isEmpty();
    }, TestHelper.WAIT_DURATION));

    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowContext context = _driver.getWorkflowContext(queueName);
      return context.getJobStates().isEmpty();
    }, TestHelper.WAIT_DURATION));
  }


  @Test(dependsOnMethods = "testJobQueueFailedCleanUp")
  public void testJobQueueTimedOutCleanUp() throws Exception {
    int capacity = 10;
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName, capacity);
    WorkflowConfig.Builder cfgBuilder = new WorkflowConfig.Builder(builder.getWorkflowConfig());
    cfgBuilder.setJobPurgeInterval(1000);
    builder.setWorkflowConfig(cfgBuilder.build());

    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2).setTimeout(100)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"))
            .setTerminalStateExpiry(200L);
    for (int i = 0; i < capacity; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
    }
    _driver.start(builder.build());

    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowConfig config = _driver.getWorkflowConfig(queueName);
      return config.getJobDag().getAllNodes().isEmpty();
    }, TestHelper.WAIT_DURATION));

    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowContext context = _driver.getWorkflowContext(queueName);
      return context.getJobStates().isEmpty();
    }, TestHelper.WAIT_DURATION));
  }
}
