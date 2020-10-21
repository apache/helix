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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.CurrentState;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestDropCurrentStateRunningTask extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 3;
    _numPartitions = 1;
    super.beforeClass();

    // Stop participants that have been started in super class
    for (int i = 0; i < _numNodes; i++) {
      super.stopParticipant(i);
      Assert.assertFalse(_participants[i].isConnected());
    }
    _participants = new MockParticipantManager[_numNodes];
    startParticipant(2);
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
  }

  @Test
  public void testDropCurrentStateRunningTask() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";
    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);

    _driver.start(workflowBuilder1.build());

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    // Task should be assigned to the only available instance which is participant2
    Assert.assertTrue(TestHelper.verify(
        () -> (TaskPartitionState.RUNNING
            .equals(_driver.getJobContext(namespacedJobName).getPartitionState(0))
            && (PARTICIPANT_PREFIX + "_" + (_startPort + 2))
                .equals(_driver.getJobContext(namespacedJobName).getAssignedParticipant(0))),
        TestHelper.WAIT_DURATION));

    // Now start two other participants
    startParticipant(0);
    startParticipant(1);

    // Get current state of participant 2 and make sure it is created
    String instanceP2 = PARTICIPANT_PREFIX + "_" + (_startPort + 2);
    ZkClient clientP2 = (ZkClient) _participants[2].getZkClient();
    String sessionIdP2 = ZkTestHelper.getSessionId(clientP2);
    String currentStatePathP2 = "/" + CLUSTER_NAME + "/INSTANCES/" + instanceP2 + "/CURRENTSTATES/"
        + sessionIdP2 + "/" + namespacedJobName;

    Assert
        .assertTrue(
            TestHelper
                .verify(
                    () -> (_manager.getHelixDataAccessor().getBaseDataAccessor()
                        .get(currentStatePathP2, new Stat(), AccessOption.PERSISTENT) != null),
                    TestHelper.WAIT_DURATION));

    // Set the current state of participant0 and participant1 with requested state equals DROPPED
    String instanceP0 = PARTICIPANT_PREFIX + "_" + (_startPort + 0);
    ZkClient clientP0 = (ZkClient) _participants[0].getZkClient();
    String sessionIdP0 = ZkTestHelper.getSessionId(clientP0);
    String currentStatePathP0 = "/" + CLUSTER_NAME + "/INSTANCES/" + instanceP0 + "/CURRENTSTATES/"
        + sessionIdP0 + "/" + namespacedJobName;

    String instanceP1 = PARTICIPANT_PREFIX + "_" + (_startPort + 1);
    ZkClient clientP1 = (ZkClient) _participants[1].getZkClient();
    String sessionIdP1 = ZkTestHelper.getSessionId(clientP1);
    String currentStatePathP1 = "/" + CLUSTER_NAME + "/INSTANCES/" + instanceP1 + "/CURRENTSTATES/"
        + sessionIdP1 + "/" + namespacedJobName;

    ZNRecord record = _manager.getHelixDataAccessor().getBaseDataAccessor().get(currentStatePathP2,
        new Stat(), AccessOption.PERSISTENT);
    String partitionName = namespacedJobName + "_0";
    Map<String, String> newCurrentState = new HashMap<>();
    newCurrentState.put(CurrentState.CurrentStateProperty.CURRENT_STATE.name(),
        TaskPartitionState.RUNNING.name());
    newCurrentState.put(CurrentState.CurrentStateProperty.REQUESTED_STATE.name(),
        TaskPartitionState.DROPPED.name());
    record.setSimpleField(CurrentState.CurrentStateProperty.SESSION_ID.name(), sessionIdP0);
    record.setMapField(partitionName, newCurrentState);
    _manager.getHelixDataAccessor().getBaseDataAccessor().set(currentStatePathP0, record,
        AccessOption.PERSISTENT);
    record.setSimpleField(CurrentState.CurrentStateProperty.SESSION_ID.name(), sessionIdP1);
    _manager.getHelixDataAccessor().getBaseDataAccessor().set(currentStatePathP1, record,
        AccessOption.PERSISTENT);

    // Make sure that the current states on participant0 and participant1 have been deleted
    Assert
        .assertTrue(
            TestHelper
                .verify(
                    () -> (_manager.getHelixDataAccessor().getBaseDataAccessor()
                        .get(currentStatePathP0, new Stat(), AccessOption.PERSISTENT) == null
                        && _manager.getHelixDataAccessor().getBaseDataAccessor()
                            .get(currentStatePathP1, new Stat(), AccessOption.PERSISTENT) == null),
                    TestHelper.WAIT_DURATION));

    _driver.stop(workflowName);
  }

  @Test(dependsOnMethods = "testDropCurrentStateRunningTask")
  public void testJobCurrentStateDroppedAfterCompletion() throws Exception {
    // Stop participants that have been started in previous test and start one of them
    for (int i = 0; i < _numNodes; i++) {
      super.stopParticipant(i);
      Assert.assertFalse(_participants[i].isConnected());
    }
    _participants = new MockParticipantManager[_numNodes];
    startParticipant(0);

    String jobQueueName = TestHelper.getTestMethodName();

    JobConfig.Builder jobBuilderCompleted =
        JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG).setMaxAttemptsPerTask(1)
            .setWorkflow(jobQueueName)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10"));

    // Task gets timed out in 10 seconds because the the default value is 10 seconds in
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
