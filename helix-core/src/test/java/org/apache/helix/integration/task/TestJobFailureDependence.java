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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestJobFailureDependence extends TaskTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestJobFailureDependence.class);

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 5;
    super.beforeClass();
  }

  @Test
  public void testJobDependantFailure() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName, 0, 100);
    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < _numDbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(_testDbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE"));
      String jobName = "job" + _testDbs.get(i);
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }

    _driver.start(queueBuilder.build());
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, _testDbs.get(2));

    // all jobs after failed job should fail too.
    for (int i = 2; i < _numDbs; i++) {
      String namedSpaceJob = String.format("%s_%s", queueName, currentJobNames.get(i));
      _driver.pollForJobState(queueName, namedSpaceJob, TaskState.FAILED);
    }
  }

  @Test
  public void testJobDependantWorkflowFailure() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName);
    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < _numDbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(_testDbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE"));
      String jobName = "job" + _testDbs.get(i);
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }

    _driver.start(queueBuilder.build());
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, _testDbs.get(2));

    String namedSpaceJob1 = String.format("%s_%s", queueName, currentJobNames.get(2));
    _driver.pollForJobState(queueName, namedSpaceJob1, TaskState.FAILED);
  }

  @Test
  public void testIgnoreJobDependantFailure() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName, 0, 100);
    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < _numDbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(_testDbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE")).setIgnoreDependentJobFailure(true);
      String jobName = "job" + _testDbs.get(i);
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }

    _driver.start(queueBuilder.build());
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, _testDbs.get(2));
    String namedSpaceJob2 = String.format("%s_%s", queueName, currentJobNames.get(2));
    _driver.pollForJobState(queueName, namedSpaceJob2, TaskState.FAILED);

    // all jobs after failed job should complete.
    for (int i = 3; i < _numDbs; i++) {
      String namedSpaceJob = String.format("%s_%s", queueName, currentJobNames.get(i));
      _driver.pollForJobState(queueName, namedSpaceJob, TaskState.COMPLETED);
    }
  }

  @Test
  public void testWorkflowFailureJobThreshold() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName, 0, 3);
    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < _numDbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(_testDbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE")).setIgnoreDependentJobFailure(true);
      String jobName = "job" + _testDbs.get(i);
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }

    _driver.start(queueBuilder.build());
    _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, _testDbs.get(1));

    String namedSpaceJob1 = String.format("%s_%s", queueName, currentJobNames.get(1));
    _driver.pollForJobState(queueName, namedSpaceJob1, TaskState.FAILED);
    String lastJob =
        String.format("%s_%s", queueName, currentJobNames.get(currentJobNames.size() - 1));
    _driver.pollForJobState(queueName, lastJob, TaskState.COMPLETED);

    _driver.flushQueue(queueName);

    WorkflowConfig currentWorkflowConfig = _driver.getWorkflowConfig(queueName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(currentWorkflowConfig);

    configBuilder.setFailureThreshold(0);
    _driver.updateWorkflow(queueName, configBuilder.build());
    _driver.stop(queueName);

    for (int i = 0; i < _numDbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(_testDbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE")).setIgnoreDependentJobFailure(true);
      String jobName = "job" + _testDbs.get(i);
      queueBuilder.enqueueJob(jobName, jobConfig);
      _driver.enqueueJob(queueName, jobName, jobConfig);
    }

    _driver.resume(queueName);

    namedSpaceJob1 = String.format("%s_%s", queueName, currentJobNames.get(1));
    _driver.pollForJobState(queueName, namedSpaceJob1, TaskState.FAILED);
  }
}

