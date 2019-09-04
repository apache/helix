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

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestDeleteJobFromJobQueue extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    super.beforeClass();
  }

  @Test
  public void testForceDeleteJobFromJobQueue() throws InterruptedException {
    String jobQueueName = TestHelper.getTestMethodName();

    // Create two jobs: job1 will complete fast, and job2 will be stuck in progress. The idea is to
    // force-delete a stuck job (job2).
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10"));
    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000")).setTimeout(100000);

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("job1", jobBuilder);
    jobQueue.enqueueJob("job2", jobBuilder2);
    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "job2"),
        TaskState.IN_PROGRESS);

    try {
      _driver.deleteJob(jobQueueName, "job2");
    } catch (IllegalStateException e) {
      // Expect IllegalStateException because job2 is still in progress
    }

    // The following force delete for the job should go through without getting an exception
    _driver.deleteJob(jobQueueName, "job2", true);

    // Check that the job has been force-deleted (fully gone from ZK)
    Assert.assertNull(_driver.getJobConfig(TaskUtil.getNamespacedJobName(jobQueueName, "job2")));
    Assert.assertNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "job2")));
    Assert.assertNull(_manager.getClusterManagmentTool().getResourceIdealState(CLUSTER_NAME,
        TaskUtil.getNamespacedJobName(jobQueueName, "job2")));
  }
}
