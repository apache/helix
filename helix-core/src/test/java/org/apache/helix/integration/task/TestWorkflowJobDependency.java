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
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestWorkflowJobDependency extends TaskTestBase {
  private static final Logger LOG = Logger.getLogger(TestWorkflowJobDependency.class);

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 5;
    _numParitions = 1;
    _partitionVary = false;
    super.beforeClass();
  }

  @Test (enabled = false)
  public void testWorkflowWithOutDependencies() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();

    // Workflow setup
    LOG.info("Start setup for workflow: " + workflowName);
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    for (int i = 0; i < _numDbs; i++) {
      // Let each job delay for 2 secs.
      JobConfig.Builder jobConfig = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
          .setTargetResource(_testDbs.get(i)).setTargetPartitionStates(Sets.newHashSet("SLAVE","MASTER"))
          .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG);
      String jobName = "job" + _testDbs.get(i);
      builder.addJob(jobName, jobConfig);
    }

    // Start workflow
    Workflow workflow = builder.build();
    _driver.start(workflow);

    // Wait until the workflow completes
    TaskTestUtil.pollForWorkflowState(_driver, workflowName, TaskState.COMPLETED);
    WorkflowContext workflowContext = _driver.getWorkflowContext(workflowName);
    long startTime = workflowContext.getStartTime();
    long finishTime = workflowContext.getFinishTime();

    // Update the start time range.
    for (String jobName : workflow.getJobConfigs().keySet()) {
      JobContext context = _driver.getJobContext(jobName);
      LOG.info(String
          .format("JOB: %s starts from %s finishes at %s.", jobName, context.getStartTime(),
              context.getFinishTime()));

      // Find job start time range.
      startTime = Math.max(context.getStartTime(), startTime);
      finishTime = Math.min(context.getFinishTime(), finishTime);
    }

    // All jobs have a valid overlap time range.
    Assert.assertTrue(startTime <= finishTime);
  }
}
