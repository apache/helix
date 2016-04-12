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
import java.util.Collections;
import java.util.List;

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.WorkflowConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskRebalancerParallel extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
   _numDbs = 4;
    super.beforeClass();
  }

  @Test public void test() throws Exception {
    final int PARALLEL_COUNT = 2;

    String queueName = TestHelper.getTestMethodName();

    WorkflowConfig.Builder cfgBuilder = new WorkflowConfig.Builder();
    cfgBuilder.setParallelJobs(PARALLEL_COUNT);

    JobQueue.Builder queueBuild =
        new JobQueue.Builder(queueName).setWorkflowConfig(cfgBuilder.build());
    JobQueue queue = queueBuild.build();
    _driver.createQueue(queue);

    List<JobConfig.Builder> jobConfigBuilders = new ArrayList<JobConfig.Builder>();
    for (String testDbName : _testDbs) {
      jobConfigBuilders.add(
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(testDbName)
              .setTargetPartitionStates(Collections.singleton("SLAVE")));
    }

    for (int i = 0; i < jobConfigBuilders.size(); ++i) {
      _driver.enqueueJob(queueName, "job_" + (i + 1), jobConfigBuilders.get(i));
    }

    Assert.assertTrue(TaskTestUtil.pollForWorkflowParallelState(_driver, queueName));
  }
}
