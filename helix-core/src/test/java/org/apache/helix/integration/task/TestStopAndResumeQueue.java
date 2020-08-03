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
import com.google.common.collect.Sets;
import org.apache.helix.TestHelper;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test to check if context of the queue gets properly updated even when there is no job queued in
 * it.
 */
public class TestStopAndResumeQueue extends TaskTestBase {
  private static final String DATABASE = WorkflowGenerator.DEFAULT_TGT_DB;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 3;
    super.beforeClass();
  }

  @Test
  public void testStopAndResumeQueue() throws Exception {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder0 =
        new JobConfig.Builder().setWorkflow(jobQueueName).setTargetResource(DATABASE)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("JOB0", jobBuilder0);
    _driver.start(jobQueue.build());
    _driver.pollForWorkflowState(jobQueueName, TaskState.IN_PROGRESS);
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"),
        TaskState.COMPLETED);
    _driver.waitToStop(jobQueueName, 50000L);
    _driver.resume(jobQueueName);
    // Resume should change the workflow context's state to IN_PROGRESS even when there is no job
    // running
    _driver.pollForWorkflowState(jobQueueName, TaskState.IN_PROGRESS);
  }
}
