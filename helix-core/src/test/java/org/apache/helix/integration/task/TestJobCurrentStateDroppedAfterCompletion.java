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

import org.apache.helix.AccessOption;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestJobCurrentStateDroppedAfterCompletion extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 1;
    super.beforeClass();
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
  }

  @Test
  public void testJobCurrentStateDroppedAfterCompletion() throws Exception {
    String jobQueueName = TestHelper.getTestMethodName();

    JobConfig.Builder jobBuilderCompleted =
        JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG).setMaxAttemptsPerTask(1)
            .setWorkflow(jobQueueName)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10"));

    // Job will timeout in 10 seconds because the the default value it 10 second for
    // DEFAULT_JOB_CONFIG
    JobConfig.Builder jobBuilderTimedOut =
        JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG).setMaxAttemptsPerTask(1)
            .setWorkflow(jobQueueName)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName, 0, 100);

    for (int i = 0; i < 20; i++) {
      jobQueue.enqueueJob("job" + i, jobBuilderCompleted);
    }

    jobQueue.enqueueJob("job" + 20, jobBuilderTimedOut);

    _driver.start(jobQueue.build());

    for (int i = 0; i < 20; i++) {
      _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "job" + i),
          TaskState.COMPLETED);
    }
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "job" + 20),
        TaskState.FAILED);

    String instanceP0 = PARTICIPANT_PREFIX + "_" + (_startPort + 0);
    ZkClient clientP0 = (ZkClient) _participants[0].getZkClient();
    String sessionIdP0 = ZkTestHelper.getSessionId(clientP0);

    for (int i = 0; i < 21; i++) {
      String currentStatePathP0 =
          "/" + CLUSTER_NAME + "/INSTANCES/" + instanceP0 + "/CURRENTSTATES/" + sessionIdP0 + "/"
              + TaskUtil.getNamespacedJobName(jobQueueName, "job" + i);
      boolean isCurrentStateRemoved = TestHelper.verify(() -> {
        ZNRecord record = _manager.getHelixDataAccessor().getBaseDataAccessor()
            .get(currentStatePathP0, new Stat(), AccessOption.PERSISTENT);
        return record == null;
      }, TestHelper.WAIT_DURATION);
      Assert.assertTrue(isCurrentStateRemoved);
    }
  }
}
