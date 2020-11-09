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
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test checks of the workflow and job context does not get updated if there no change is
 * applied to them.
 */
public class TestContextUpdateIfNoChange extends TaskTestBase {
  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numPartitions = 1;
    super.beforeClass();
  }

  @Test
  public void testContextUpdateIfNoChange() throws Exception {
    // First workflow has one long running job
    String workflowName1 = TestHelper.getTestMethodName() + "_1";
    String jobName = "JOB0";
    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setWorkflow(workflowName1).setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100)
        .setTimeoutPerTask(Long.MAX_VALUE).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));
    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName1).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    _driver.pollForJobState(workflowName1, TaskUtil.getNamespacedJobName(workflowName1, jobName),
        TaskState.IN_PROGRESS);
    _driver.pollForWorkflowState(workflowName1, TaskState.IN_PROGRESS);
    Assert.assertTrue(TestHelper.verify(() -> (TaskPartitionState.RUNNING
            .equals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName1, jobName))
                .getPartitionState(0))),
        TestHelper.WAIT_DURATION));

    int initialWorkflowContextVersion = _manager.getHelixDataAccessor()
        .getProperty(
            _manager.getHelixDataAccessor().keyBuilder().workflowContextZNode(workflowName1))
        .getRecord().getVersion();

    int initialJobContextVersion = _manager.getHelixDataAccessor()
        .getProperty(
            _manager.getHelixDataAccessor().keyBuilder().jobContextZNode(workflowName1, jobName))
        .getRecord().getVersion();

    // Second workflow has one short running job
    String workflowName2 = TestHelper.getTestMethodName() + "_2";
    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setWorkflow(workflowName2).setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100)
        .setTimeoutPerTask(Long.MAX_VALUE).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "5000"));
    Workflow.Builder workflowBuilder2 =
        new Workflow.Builder(workflowName2).addJob(jobName, jobBuilder2);
    // Start new workflow and make sure it gets completed. This would help us to make sure pipeline
    // has been run several times
    _driver.start(workflowBuilder2.build());
    _driver.pollForWorkflowState(workflowName2, TaskState.COMPLETED);

    int finalWorkflowContextVersion = _manager.getHelixDataAccessor()
        .getProperty(
            _manager.getHelixDataAccessor().keyBuilder().workflowContextZNode(workflowName1))
        .getRecord().getVersion();

    int finalJobContextVersion = _manager.getHelixDataAccessor()
        .getProperty(
            _manager.getHelixDataAccessor().keyBuilder().jobContextZNode(workflowName1, jobName))
        .getRecord().getVersion();

    Assert.assertEquals(initialJobContextVersion,finalJobContextVersion);
    Assert.assertEquals(initialWorkflowContextVersion, finalWorkflowContextVersion);
  }
}
