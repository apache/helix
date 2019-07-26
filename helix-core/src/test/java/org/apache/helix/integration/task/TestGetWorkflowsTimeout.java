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

package org.apache.helix.integration.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGetWorkflowsTimeout extends TaskTestBase {
  @Test
  public void testGetWorkflowsTimeout() throws Exception {
    List<Workflow> workflowList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Workflow workflow = WorkflowGenerator
          .generateDefaultRepeatedJobWorkflowBuilder(TestHelper.getTestMethodName() + i).build();
      _driver.start(workflow);
      workflowList.add(workflow);
    }

    for (Workflow workflow : workflowList) {
      _driver.pollForWorkflowState(workflow.getName(), TaskState.COMPLETED);
    }

    _driver.startPool();

    // Disconnect ZkManager to make getWorkflows timeout.
    _manager.disconnect();

    try {
      Map<String, WorkflowConfig> workflowConfigMap = _driver.getWorkflows(5_000L);
    } catch (Exception e) {
      // Exepected timeout.
    }

    _driver.shutdownPool();
  }
}
