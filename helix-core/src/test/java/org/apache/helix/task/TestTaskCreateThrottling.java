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

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.HelixException;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskCreateThrottling extends TaskTestBase {
  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
    _driver._configsLimitation = 10;
  }

  @Test
  public void testTaskCreatingThrottle() {
    Workflow flow = WorkflowGenerator.generateDefaultRepeatedJobWorkflowBuilder("hugeWorkflow",
        (int) _driver._configsLimitation + 1).build();
    try {
      _driver.start(flow);
      Assert.fail("Creating a huge workflow contains more jobs than expected should fail.");
    } catch (HelixException e) {
      // expected
    }
  }

  @Test(dependsOnMethods = {
      "testTaskCreatingThrottle"
  })
  public void testEnqueueJobsThrottle() throws InterruptedException {
    List<String> jobs = new ArrayList<>();
    // Use a short name for testing
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue("Q");
    builder.setCapacity(Integer.MAX_VALUE);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG).setExpiry(1L);
    for (int i = 0; i < _driver._configsLimitation - 5; i++) {
      builder.enqueueJob("J" + i, jobBuilder);
      jobs.add("J" + i);
    }
    JobQueue jobQueue = builder.build();
    // check if large number of jobs smaller than the threshold is OK.
    _driver.start(jobQueue);
    _driver.stop(jobQueue.getName());
    _driver.pollForWorkflowState(jobQueue.getName(), TaskState.STOPPED);
    try {
      for (int i = 0; i < _driver._configsLimitation; i++) {
        _driver.enqueueJob(jobQueue.getName(), "EJ" + i, jobBuilder);
        jobs.add("EJ" + i);
      }
      Assert.fail("Enqueuing a huge number of jobs should fail.");
    } catch (HelixException e) {
      // expected
    }

    for (String job : jobs) {
      try {
        _driver.deleteJob(jobQueue.getName(), job);
      } catch (Exception e) {
        // OK
      }
    }
    _driver.delete(jobQueue.getName());
  }
}
