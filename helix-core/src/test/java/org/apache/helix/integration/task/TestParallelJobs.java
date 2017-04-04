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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashSet;

import java.util.Set;
import org.apache.helix.TestHelper;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestParallelJobs extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
   _numDbs = 4;
    super.beforeClass();
  }

  /**
   * This test starts 4 jobs in job queue, the job all stuck, and verify that
   * (1) the number of running job does not exceed configured max allowed parallel jobs
   * (2) one instance can only be assigned to one job in the workflow
   */
  @Test
  public void testParallelJobsMeetRequirements() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    WorkflowConfig.Builder cfgBuilder = new WorkflowConfig.Builder();
    cfgBuilder.setParallelJobs(2);

    JobQueue.Builder queueBuild =
        new JobQueue.Builder(queueName).setWorkflowConfig(cfgBuilder.build());
    JobQueue queue = queueBuild.build();
    _driver.createQueue(queue);
    _driver.stop(queueName);

    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder()
        .setCommand(MockTask.TASK_COMMAND)
        .setTargetPartitionStates(Collections.singleton(MasterSlaveSMD.States.SLAVE.name()))
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.TIMEOUT_CONFIG, "99999999"));

    for (String testDbName : _testDbs) {
      jobConfigBuilder.setTargetResource(testDbName);
      _driver.enqueueJob(queueName, "job_" + testDbName, jobConfigBuilder);
    }

    _driver.resume(queueName);

    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(queueName);

    Assert.assertTrue(getNumOfRunningJobs(workflowConfig) <= workflowConfig.getParallelJobs());
    Assert.assertTrue(noInstanceAssignedToMultipleJobs(workflowConfig));

    _driver.stop(queueName);
    _driver.pollForWorkflowState(queueName, TaskState.STOPPED);
  }

  private int getNumOfRunningJobs(WorkflowConfig workflowConfig) throws InterruptedException {
    WorkflowContext workflowContext = TaskTestUtil.pollForWorkflowContext(_driver, workflowConfig.getWorkflowId());
    int runningCount = 0;
    for (String jobName : workflowConfig.getJobDag().getAllNodes()) {
      TaskState jobState = workflowContext.getJobState(jobName);
      if (jobState == TaskState.IN_PROGRESS) {
        runningCount++;
      }
    }
    return runningCount;
  }

  private boolean noInstanceAssignedToMultipleJobs(WorkflowConfig workflowConfig) {
    Set<String> takenInstances = new HashSet<String>();
    for (String jobName : workflowConfig.getJobDag().getAllNodes()) {
      JobContext jobContext = _driver.getJobContext(jobName);
      if (jobContext == null) {
        continue;
      }
      for (int partition : jobContext.getPartitionSet()) {
        TaskPartitionState taskState = jobContext.getPartitionState(partition);
        if (taskState == TaskPartitionState.INIT || taskState == TaskPartitionState.RUNNING) {
          String instance = jobContext.getAssignedParticipant(partition);
          if (takenInstances.contains(instance)) {
            return false;
          }
          takenInstances.add(instance);
        }
      }
    }
    return true;
  }
}
