package org.apache.helix.task;

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

import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestWorkflowCreation extends TaskSynchronizedTestBase {

  /**
   * Test that submitting workflows of the same name throws an exception.
   */
  @Test(expectedExceptions = HelixException.class)
  public void testWorkflowCreationNoDuplicates() {
    String queue = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(queue);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG).setExpiry(1L);
    for (int i = 0; i < 8; i++) {
      builder.enqueueJob("JOB" + i, jobBuilder);
    }

    // First try
    _driver.createQueue(builder.build());
    Assert.assertNotNull(_driver.getWorkflowConfig(queue));

    // Second try (this should throw an exception)
    _driver.createQueue(builder.build());
  }
}
