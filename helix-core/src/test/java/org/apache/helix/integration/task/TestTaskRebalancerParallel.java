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
import java.util.Collections;
import java.util.List;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterVerifiers.ClusterLiveNodesVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskRebalancerParallel extends TaskTestBase {
  final int PARALLEL_COUNT = 2;
  final int TASK_COUNT = 30;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 4;
    super.beforeClass();
  }

  /**
   * This test starts 4 jobs in job queue, the job all stuck, and verify that
   * (1) the number of running job does not exceed configured max allowed parallel jobs
   * (2) one instance can only be assigned to one job in the workflow
   */
  @Test public void testWhenDisallowOverlapJobAssignment() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    WorkflowConfig.Builder cfgBuilder = new WorkflowConfig.Builder(queueName);
    cfgBuilder.setParallelJobs(PARALLEL_COUNT);
    cfgBuilder.setAllowOverlapJobAssignment(false);

    JobQueue.Builder queueBuild =
        new JobQueue.Builder(queueName).setWorkflowConfig(cfgBuilder.build());
    JobQueue queue = queueBuild.build();
    _driver.createQueue(queue);

    List<JobConfig.Builder> jobConfigBuilders = new ArrayList<JobConfig.Builder>();
    for (String testDbName : _testDbs) {
      jobConfigBuilders.add(
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(testDbName)
              .setTargetPartitionStates(Collections.singleton("SLAVE")));
    }

    _driver.stop(queueName);
    for (int i = 0; i < jobConfigBuilders.size(); ++i) {
      _driver.enqueueJob(queueName, "job_" + (i + 1), jobConfigBuilders.get(i));
    }
    _driver.resume(queueName);
    Thread.sleep(2000);
    Assert.assertTrue(TaskTestUtil.pollForWorkflowParallelState(_driver, queueName));
  }

  /**
   * This test starts 4 jobs in job queue, the job all stuck, and verify that
   * (1) the number of running job does not exceed configured max allowed parallel jobs
   * (2) one instance can be assigned to multiple jobs in the workflow when allow overlap assignment
   */
  @Test (dependsOnMethods = {"testWhenDisallowOverlapJobAssignment"})
  public void testWhenAllowOverlapJobAssignment() throws Exception {
    // Disable all participants except one to enforce all assignment to be on one host
    for (int i = 1; i < _numNodes; i++) {
      _participants[i].syncStop();
    }
    ClusterLiveNodesVerifier verifier = new ClusterLiveNodesVerifier(_gZkClient, CLUSTER_NAME,
        Collections.singletonList(_participants[0].getInstanceName()));
    Assert.assertTrue(verifier.verifyByPolling());

    String queueName = TestHelper.getTestMethodName();

    WorkflowConfig.Builder cfgBuilder = new WorkflowConfig.Builder(queueName);
    cfgBuilder.setParallelJobs(PARALLEL_COUNT);
    cfgBuilder.setAllowOverlapJobAssignment(true);

    JobQueue.Builder queueBuild = new JobQueue.Builder(queueName).setWorkflowConfig(cfgBuilder.build());
    JobQueue queue = queueBuild.build();
    _driver.createQueue(queue);

    // Create jobs that can be assigned to any instances
    List<JobConfig.Builder> jobConfigBuilders = new ArrayList<JobConfig.Builder>();
    for (int i = 0; i < PARALLEL_COUNT; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<TaskConfig>();
      for (int j = 0; j < TASK_COUNT; j++) {
        taskConfigs.add(
            new TaskConfig.Builder().setTaskId("task_" + j).setCommand(MockTask.TASK_COMMAND)
                .build());
      }
      jobConfigBuilders.add(new JobConfig.Builder().addTaskConfigs(taskConfigs));
    }

    _driver.stop(queueName);
    for (int i = 0; i < jobConfigBuilders.size(); ++i) {
      _driver.enqueueJob(queueName, "job_" + (i + 1), jobConfigBuilders.get(i));
    }
    _driver.resume(queueName);
    Thread.sleep(2000);
    Assert.assertTrue(TaskTestUtil.pollForWorkflowParallelState(_driver, queueName));
  }
}
