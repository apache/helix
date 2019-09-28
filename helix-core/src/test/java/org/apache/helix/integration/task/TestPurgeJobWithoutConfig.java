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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * This test checks whether a job without config is being removed from the DAG.
 */
public class TestPurgeJobWithoutConfig extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
  }

  @Test
  public void testPurgeJobWithoutConfig() throws Exception {
    // Timeout per task has been set to be a large number.
    final long timeout = 60000L;
    String jobQueueName = TestHelper.getTestMethodName();

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);

    JobConfig.Builder jobBuilder0 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(timeout).setMaxAttemptsPerTask(1)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(timeout).setMaxAttemptsPerTask(1)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(timeout).setMaxAttemptsPerTask(1)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    jobQueue.enqueueJob("JOB0", jobBuilder0);
    jobQueue.enqueueJob("JOB1", jobBuilder1);
    jobQueue.enqueueJob("JOB2", jobBuilder2);

    _driver.start(jobQueue.build());

    // Wait until Queue is running.
    _driver.pollForWorkflowState(jobQueueName, TaskState.IN_PROGRESS);

    // Check the JOB1 is completed
    String nameSpacedJobName = TaskUtil.getNamespacedJobName(jobQueueName, "JOB1");

    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB1"),
        TaskState.COMPLETED);

    // Remove the config associated with JOB1
    boolean configDeleted =
        removeWorkflowJobConfig(_manager.getHelixDataAccessor(), nameSpacedJobName);
    Assert.assertTrue(configDeleted);

    // Check whether JOB1 has been successfully removed from the DAG
    boolean haveJobDeletedFromDag = TestHelper.verify(() -> {
      WorkflowConfig workflowConfig = _driver.getWorkflowConfig(jobQueueName);
      JobDag dag = workflowConfig.getJobDag();
      return !dag.getAllNodes().contains(nameSpacedJobName);
    }, 60 * 1000);
    Assert.assertTrue(haveJobDeletedFromDag);
  }

  /**
   * This function removes the job config from the ZooKeeper
   * @param accessor
   * @param workflowJobResource
   * @return
   */
  private static boolean removeWorkflowJobConfig(HelixDataAccessor accessor,
      String workflowJobResource) {
    PropertyKey cfgKey = accessor.keyBuilder().resourceConfig(workflowJobResource);
    if (accessor.getPropertyStat(cfgKey) != null) {
      if (!accessor.removeProperty(cfgKey)) {
        return false;
      }
    }
    return true;
  }
}
