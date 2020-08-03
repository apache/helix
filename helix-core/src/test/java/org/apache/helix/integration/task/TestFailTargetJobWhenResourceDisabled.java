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
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestFailTargetJobWhenResourceDisabled extends TaskTestBase {
  private JobConfig.Builder _jobCfg;
  private String _jobName;

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
    _jobName = "TestJob";
    _jobCfg = new JobConfig.Builder().setJobId(_jobName).setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB);
  }

  @Test
  public void testJobScheduleAfterResourceDisabled() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    _gSetupTool.getClusterManagementTool()
        .enableResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, false);
    Workflow.Builder workflow = new Workflow.Builder(workflowName);
    workflow.addJob(_jobName, _jobCfg);
    _driver.start(workflow.build());

    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);
  }

  @Test
  public void testJobScheduleBeforeResourceDisabled() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflow = new Workflow.Builder(workflowName);
    _jobCfg.setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000000"));
    workflow.addJob(_jobName, _jobCfg);
    _driver.start(workflow.build());
    Thread.sleep(1000);
    _gSetupTool.getClusterManagementTool()
        .enableResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, false);
    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);
  }


}
