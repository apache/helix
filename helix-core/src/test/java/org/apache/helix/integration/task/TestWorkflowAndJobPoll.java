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
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestWorkflowAndJobPoll extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 1;
    _numParitions = 1;
    _numReplicas = 1;
    super.beforeClass();
  }

  @Test public void testWorkflowPoll() throws InterruptedException {
    String jobResource = TestHelper.getTestMethodName();
    Workflow.Builder builder =
        WorkflowGenerator.generateDefaultSingleJobWorkflowBuilder(jobResource);
    _driver.start(builder.build());

    TaskState polledState =
        _driver.pollForWorkflowState(jobResource, 4000L, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(TaskState.COMPLETED, polledState);
  }

  @Test public void testJobPoll() throws InterruptedException {
    String jobResource = TestHelper.getTestMethodName();
    Workflow.Builder builder =
        WorkflowGenerator.generateDefaultSingleJobWorkflowBuilder(jobResource);
    _driver.start(builder.build());

    TaskState polledState = _driver
        .pollForJobState(jobResource, String.format("%s_%s", jobResource, jobResource), 4000L,
            TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(TaskState.COMPLETED, polledState);
  }
}
