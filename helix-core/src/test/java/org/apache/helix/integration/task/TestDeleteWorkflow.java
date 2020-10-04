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

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestDeleteWorkflow extends TaskTestBase {
  private static final long DELETE_DELAY = 5000L;
  private static final long FORCE_DELETE_BACKOFF = 200L;
  private HelixAdmin admin;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    admin = _gSetupTool.getClusterManagementTool();
    super.beforeClass();
  }

  @Test
  public void testDeleteWorkflow() throws InterruptedException {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("job1", jobBuilder);
    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "job1"),
        TaskState.IN_PROGRESS);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this job
    // queue
    Assert.assertNotNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNotNull(_driver.getWorkflowContext(jobQueueName));

    // Pause the Controller so that the job queue won't get deleted
    admin.enableCluster(CLUSTER_NAME, false);
    Thread.sleep(1000);
    // Attempt the deletion and time out
    try {
      _driver.deleteAndWaitForCompletion(jobQueueName, DELETE_DELAY);
      Assert.fail(
          "Delete must time out and throw a HelixException with the Controller paused, but did not!");
    } catch (HelixException e) {
      // Pass
    }

    // Resume the Controller and call delete again
    admin.enableCluster(CLUSTER_NAME, true);
    _driver.deleteAndWaitForCompletion(jobQueueName, DELETE_DELAY);

    // Check that the deletion operation completed
    Assert.assertNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNull(_driver.getWorkflowContext(jobQueueName));
  }

  @Test
  public void testDeleteWorkflowForcefully() throws InterruptedException {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("job1", jobBuilder);
    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "job1"),
        TaskState.IN_PROGRESS);

    // Check that WorkflowConfig, WorkflowContext, JobConfig, and JobContext are indeed created for this job
    // queue
    Assert.assertNotNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNotNull(_driver.getWorkflowContext(jobQueueName));
    Assert.assertNotNull(_driver.getJobConfig(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert
        .assertNotNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));

    // Pause the Controller so that the job queue won't get deleted
    admin.enableCluster(CLUSTER_NAME, false);
    Thread.sleep(1000);
    // Attempt the deletion and time out
    try {
      _driver.deleteAndWaitForCompletion(jobQueueName, DELETE_DELAY);
      Assert.fail(
          "Delete must time out and throw a HelixException with the Controller paused, but did not!");
    } catch (HelixException e) {
      // Pass
    }
    // delete forcefully
    _driver.delete(jobQueueName, true);

    Assert.assertNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNull(_driver.getWorkflowContext(jobQueueName));
    Assert.assertNull(_driver.getJobConfig(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
  }

  @Test
  public void testDeleteHangingJobs() throws InterruptedException {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("job1", jobBuilder);
    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "job1"),
        TaskState.IN_PROGRESS);

    // Check that WorkflowConfig and WorkflowContext are indeed created for this job queue
    Assert.assertNotNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNotNull(_driver.getWorkflowContext(jobQueueName));
    Assert.assertNotNull(_driver.getJobConfig(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert
        .assertNotNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));

    // Delete the workflowconfig and context of workflow
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuild = accessor.keyBuilder();
    accessor.removeProperty(keyBuild.resourceConfig(jobQueueName));
    accessor.removeProperty(keyBuild.workflowContext(jobQueueName));

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setZkClient(_gZkClient).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // Sometimes it's a ZK write fail - delete one more time to lower test failure rate
    if (_driver.getWorkflowConfig(jobQueueName) != null
        || _driver.getWorkflowContext(jobQueueName) != null) {
      accessor.removeProperty(keyBuild.resourceConfig(jobQueueName));
      accessor.removeProperty(keyBuild.workflowContext(jobQueueName));
    }

    Assert.assertNull(_driver.getWorkflowConfig(jobQueueName));
    Assert.assertNull(_driver.getWorkflowContext(jobQueueName));

    // Attempt to delete the job and it should fail with exception.
    try {
      _driver.deleteJob(jobQueueName, "job1");
      Assert.fail("Delete must be rejected and throw a HelixException, but did not!");
    } catch (IllegalArgumentException e) {
      // Pass
    }

    // delete forcefully a few times with a backoff (the controller may write back the ZNodes
    // because force delete does not remove the job from the cache)
    for (int i = 0; i < 3; i++) {
      try {
        _driver.deleteJob(jobQueueName, "job1", true);
      } catch (Exception e) {
        // Multiple delete calls are okay
      }
      Thread.sleep(FORCE_DELETE_BACKOFF);
    }

    Assert.assertNull(_driver.getJobConfig(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
    Assert.assertNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "job1")));
  }
}
