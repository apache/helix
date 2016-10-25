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

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestJobQueueCleanUp extends TaskTestBase {
  @BeforeClass
  public void beforeClass() throws Exception {
    // TODO: Reenable this after Test Refactoring code checkin
    // setSingleTestEnvironment();
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
    _driver.cleanupJobQueue(queueName);
    Assert.assertEquals(_driver.getWorkflowConfig(queueName).getJobDag().size(), 0);
  }

  @Test public void testJobQueueNotCleanupRunningJobs() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queueName);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2);
    for (int i = 0; i < 3; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
    }
    builder.enqueueJob("JOB" + 3,
        jobBuilder.setJobCommandConfigMap(ImmutableMap.of(MockTask.TIMEOUT_CONFIG, "1000000L")));
    builder.enqueueJob("JOB" + 4, jobBuilder);
    _driver.start(builder.build());
    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, "JOB" + 3),
        TaskState.IN_PROGRESS);
    _driver.cleanupJobQueue(queueName);
    Assert.assertEquals(_driver.getWorkflowConfig(queueName).getJobDag().size(), 2);
  }
}
