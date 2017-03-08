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
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskWithInstanceDisabled extends TaskTestBase {
  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    _numNodes = 2;
    _numReplicas = 2;
    _partitionVary = false;
    super.beforeClass();
  }
  @Test
  public void testTaskWithInstanceDisabled() throws InterruptedException {
    _setupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_" + (_startPort + 0), false);
    String jobResource = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB);
    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();
    _driver.start(flow);

    _driver.pollForWorkflowState(jobResource, TaskState.COMPLETED);
    JobContext ctx = _driver.getJobContext(TaskUtil.getNamespacedJobName(jobResource));
    Assert.assertEquals(ctx.getAssignedParticipant(0), PARTICIPANT_PREFIX + "_" + (_startPort + 1));
  }
}
