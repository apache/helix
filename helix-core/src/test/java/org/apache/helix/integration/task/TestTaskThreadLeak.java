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

import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskThreadLeak extends TaskTestBase {
  private int _threadCountBefore = 0;

  @BeforeClass
  public void beforeClass() throws Exception {
    _threadCountBefore = getThreadCount("TaskStateModelFactory");
    setSingleTestEnvironment();
    _numNodes = 1;
    super.beforeClass();
  }

  @Test
  public void testTaskThreadCount() throws InterruptedException {
    String queueName = "myTestJobQueue";
    JobQueue.Builder queueBuilder = new JobQueue.Builder(queueName);
    String lastJob = null;
    for (int i = 0; i < 5; i++) {
      String db = TestHelper.getTestMethodName() + "_" + i;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, 20, MASTER_SLAVE_STATE_MODEL,
          IdealState.RebalanceMode.FULL_AUTO.name());
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, 1);
      JobConfig.Builder jobBuilder =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(db)
              .setNumConcurrentTasksPerInstance(100);
      queueBuilder.addJob(db + "_job", jobBuilder);
      lastJob = db + "_job";
    }

    queueBuilder
        .setWorkflowConfig(new WorkflowConfig.Builder(queueName).setParallelJobs(10).build());

    _driver.start(queueBuilder.build());

    String nameSpacedJob = TaskUtil.getNamespacedJobName(queueName, lastJob);
    _driver.pollForJobState(queueName, nameSpacedJob, TaskState.COMPLETED);


    int threadCountAfter = getThreadCount("TaskStateModelFactory");

    Assert.assertTrue(
        (threadCountAfter - _threadCountBefore) <= TaskStateModelFactory.TASK_THREADPOOL_SIZE + 1);
  }


  private int getThreadCount(String threadPrefix) {
    int count = 0;
    Set<Thread> allThreads = Thread.getAllStackTraces().keySet();
    for (Thread t : allThreads) {
      if (t.getName().contains(threadPrefix)) {
        count ++;
      }
    }

    return count;
  }
}
