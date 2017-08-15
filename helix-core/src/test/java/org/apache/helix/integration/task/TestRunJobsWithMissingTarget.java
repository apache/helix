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
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestRunJobsWithMissingTarget extends TaskTestBase {
  private static final Logger LOG = Logger.getLogger(TestRunJobsWithMissingTarget.class);

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 5;
    super.beforeClass();
  }

  @Test
  public void testJobFailsWithMissingTarget() throws Exception {
    String workflowName = TestHelper.getTestMethodName();

    // Create a workflow
    LOG.info("Starting job-queue: " + workflowName);
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < _numDbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(
              _testDbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE"));
      String jobName = "job" + _testDbs.get(i);
      builder.addJob(jobName, jobConfig);
      if (i > 0) {
        builder.addParentChildDependency("job" + _testDbs.get(i - 1), "job" + _testDbs.get(i));
      }
      currentJobNames.add(jobName);
    }

    _setupTool.dropResourceFromCluster(CLUSTER_NAME, _testDbs.get(1));
    _driver.start(builder.build());

    String namedSpaceJob = String.format("%s_%s", workflowName, currentJobNames.get(1));
    _driver.pollForJobState(workflowName, namedSpaceJob, TaskState.FAILED);
    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);

    _driver.delete(workflowName);
  }

  @Test(dependsOnMethods = "testJobFailsWithMissingTarget")
  public void testJobContinueUponParentJobFailure() throws Exception {
    String workflowName = TestHelper.getTestMethodName();

    // Create a workflow
    LOG.info("Starting job-queue: " + workflowName);
    Workflow.Builder builder = new Workflow.Builder(workflowName).setWorkflowConfig(
        new WorkflowConfig.Builder(workflowName).setFailureThreshold(10).build());
    // Create and add jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < _numDbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(_testDbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE")).setIgnoreDependentJobFailure(true);
      String jobName = "job" + _testDbs.get(i);
      builder.addJob(jobName, jobConfig);
      if (i > 0) {
        builder.addParentChildDependency("job" + _testDbs.get(i - 1), "job" + _testDbs.get(i));
      }
      currentJobNames.add(jobName);
    }

    _driver.start(builder.build());

    String namedSpaceJob1 = String.format("%s_%s", workflowName, currentJobNames.get(1));
    _driver.pollForJobState(workflowName, namedSpaceJob1, TaskState.FAILED);
    String lastJob =
        String.format("%s_%s", workflowName, currentJobNames.get(currentJobNames.size() - 1));
    _driver.pollForJobState(workflowName, lastJob, TaskState.COMPLETED);

    _driver.delete(workflowName);
  }

  @Test(dependsOnMethods = "testJobContinueUponParentJobFailure")
  public void testJobFailsWithMissingTargetInRunning() throws Exception {
    String workflowName = TestHelper.getTestMethodName();

    // Create a workflow
    LOG.info("Starting job-queue: " + workflowName);
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    // Create and add jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < _numDbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(_testDbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE"));
      String jobName = "job" + _testDbs.get(i);
      builder.addJob(jobName, jobConfig);
      if (i > 0) {
        builder.addParentChildDependency("job" + _testDbs.get(i - 1), "job" + _testDbs.get(i));
      }      currentJobNames.add(jobName);
    }

    _driver.start(builder.build());
    _setupTool.dropResourceFromCluster(CLUSTER_NAME, _testDbs.get(0));

    String namedSpaceJob1 = String.format("%s_%s", workflowName, currentJobNames.get(0));
    _driver.pollForJobState(workflowName, namedSpaceJob1, TaskState.FAILED);
    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);

    _driver.delete(workflowName);
  }
}

