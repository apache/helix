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
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskPartitionState;
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
import com.google.common.collect.Sets;

/**
 * This test makes sure that the Current State of the task are being removed after participant
 * handles new session.
 */
public class TestTaskCurrentStateDrop extends TaskTestBase {
  private static final String DATABASE = WorkflowGenerator.DEFAULT_TGT_DB;
  protected HelixDataAccessor _accessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 1;
    super.beforeClass();
  }

  @AfterClass()
  public void afterClass() throws Exception {
    super.afterClass();
  }

  @Test
  public void testCurrentStateDropAfterReconnecting() throws Exception {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder0 =
        new JobConfig.Builder().setWorkflow(jobQueueName).setTargetResource(DATABASE)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("JOB0", jobBuilder0);

    _driver.start(jobQueue.build());

    String namespacedJobName = TaskUtil.getNamespacedJobName(jobQueueName, "JOB0");

    _driver.pollForJobState(jobQueueName, namespacedJobName, TaskState.IN_PROGRESS);

    // Make sure task is in running state
    Assert.assertTrue(TestHelper.verify(
        () -> (TaskPartitionState.RUNNING
            .equals(_driver.getJobContext(namespacedJobName).getPartitionState(0))),
        TestHelper.WAIT_DURATION));

    // Get the current states of Participant0
    String instanceP0 = PARTICIPANT_PREFIX + "_" + (_startPort + 0);
    ZkClient clientP0 = (ZkClient) _participants[0].getZkClient();
    String sessionIdP0 = ZkTestHelper.getSessionId(clientP0);
    String taskCurrentStatePathP0 = _manager.getHelixDataAccessor().keyBuilder()
        .taskCurrentState(instanceP0, sessionIdP0, namespacedJobName).toString();
    String dataBaseCurrentStatePathP0 = _manager.getHelixDataAccessor().keyBuilder()
        .currentState(instanceP0, sessionIdP0, DATABASE).toString();

    // Read the current states of Participant0 and make sure they been created
    boolean isCurrentStateCreated = TestHelper.verify(() -> {
      ZNRecord recordTask = _manager.getHelixDataAccessor().getBaseDataAccessor()
          .get(taskCurrentStatePathP0, new Stat(), AccessOption.PERSISTENT);
      ZNRecord recordDataBase = _manager.getHelixDataAccessor().getBaseDataAccessor()
          .get(dataBaseCurrentStatePathP0, new Stat(), AccessOption.PERSISTENT);
      return (recordTask != null && recordDataBase != null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isCurrentStateCreated);

    // Stop the controller to make sure controller does not sent any message to participants inorder
    // to drop the current states
    _controller.syncStop();

    // restart the participant0 and make sure task related current state has not been carried over
    stopParticipant(0);
    startParticipant(0);

    clientP0 = (ZkClient) _participants[0].getZkClient();
    String newSessionIdP0 = ZkTestHelper.getSessionId(clientP0);
    String newTaskCurrentStatePathP0 = _manager.getHelixDataAccessor().keyBuilder()
        .taskCurrentState(instanceP0, newSessionIdP0, namespacedJobName).toString();
    String newDataBaseCurrentStatePathP0 = _manager.getHelixDataAccessor().keyBuilder()
        .currentState(instanceP0, newSessionIdP0, DATABASE).toString();

    boolean isCurrentStateExpected = TestHelper.verify(() -> {
      ZNRecord taskRecord = _manager.getHelixDataAccessor().getBaseDataAccessor()
          .get(newTaskCurrentStatePathP0, new Stat(), AccessOption.PERSISTENT);
      ZNRecord dataBase = _manager.getHelixDataAccessor().getBaseDataAccessor()
          .get(newDataBaseCurrentStatePathP0, new Stat(), AccessOption.PERSISTENT);
      return (taskRecord == null && dataBase != null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isCurrentStateExpected);
  }
}
