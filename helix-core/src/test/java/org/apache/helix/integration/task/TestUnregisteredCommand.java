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
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestUnregisteredCommand extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  @Test public void testUnregisteredCommand() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand("OtherCommand").setTimeoutPerTask(10000L).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG);

    builder.addJob("JOB1", jobBuilder);

    _driver.start(builder.build());

    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);
    Assert.assertEquals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "JOB1"))
        .getPartitionState(0), TaskPartitionState.ERROR);
    Assert.assertEquals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "JOB1"))
        .getPartitionNumAttempts(0), 1);
  }
}
