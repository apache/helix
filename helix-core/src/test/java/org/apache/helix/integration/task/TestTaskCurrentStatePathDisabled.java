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
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test makes sure that the Current State of the task are being removed after participant
 * handles new session.
 */
public class TestTaskCurrentStatePathDisabled extends TaskTestBase {
  private static final String DATABASE = WorkflowGenerator.DEFAULT_TGT_DB;
  protected HelixDataAccessor _accessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 1;
    super.beforeClass();
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
    System.setProperty(SystemPropertyKeys.TASK_CURRENT_STATE_PATH_DISABLED, "false");
  }

  @Test
  public void testTaskCurrentStatePathDisabled() throws Exception {
    String jobQueueName0 = TestHelper.getTestMethodName() + "_0";
    JobConfig.Builder jobBuilder0 =
        new JobConfig.Builder().setWorkflow(jobQueueName0).setTargetResource(DATABASE)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));
    JobQueue.Builder jobQueue0 = TaskTestUtil.buildJobQueue(jobQueueName0);
    jobQueue0.enqueueJob("JOB0", jobBuilder0);

    _driver.start(jobQueue0.build());
    String namespacedJobName0 = TaskUtil.getNamespacedJobName(jobQueueName0, "JOB0");
    _driver.pollForJobState(jobQueueName0, namespacedJobName0, TaskState.IN_PROGRESS);

    // Get the current states of Participant0
    String instanceP0 = PARTICIPANT_PREFIX + "_" + (_startPort + 0);
    ZkClient clientP0 = (ZkClient) _participants[0].getZkClient();
    String sessionIdP0 = ZkTestHelper.getSessionId(clientP0);
    PropertyKey.Builder keyBuilder = _manager.getHelixDataAccessor().keyBuilder();
    Assert.assertTrue(TestHelper.verify(() -> _manager.getHelixDataAccessor()
        .getProperty(keyBuilder.taskCurrentState(instanceP0, sessionIdP0, namespacedJobName0))
        != null, TestHelper.WAIT_DURATION));
    Assert.assertNull(_manager.getHelixDataAccessor()
        .getProperty(keyBuilder.currentState(instanceP0, sessionIdP0, namespacedJobName0)));

    // Test the case when the task current state path is disabled
    String jobQueueName1 = TestHelper.getTestMethodName() + "_1";
    JobConfig.Builder jobBuilder1 =
        new JobConfig.Builder().setWorkflow(jobQueueName1).setTargetResource(DATABASE)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));
    JobQueue.Builder jobQueue1 = TaskTestUtil.buildJobQueue(jobQueueName1);
    jobQueue1.enqueueJob("JOB1", jobBuilder1);

    System.setProperty(SystemPropertyKeys.TASK_CURRENT_STATE_PATH_DISABLED, "true");
    _driver.start(jobQueue1.build());
    String namespacedJobName1 = TaskUtil.getNamespacedJobName(jobQueueName1, "JOB1");
    _driver.pollForJobState(jobQueueName1, namespacedJobName1, TaskState.IN_PROGRESS);
    Assert.assertTrue(TestHelper.verify(() -> _manager.getHelixDataAccessor()
        .getProperty(keyBuilder.currentState(instanceP0, sessionIdP0, namespacedJobName1))
        != null, TestHelper.WAIT_DURATION));
    Assert.assertNull(_manager.getHelixDataAccessor()
        .getProperty(keyBuilder.taskCurrentState(instanceP0, sessionIdP0, namespacedJobName1)));
  }
}
