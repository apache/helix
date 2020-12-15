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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableMap;

/**
 * Test to check if targeted tasks correctly get assigned and also if cancel messages are not being
 * sent when there are two CurrentStates.
 */
public class TestTaskSchedulingTwoCurrentStates extends TaskTestBase {
  private static final String DATABASE = "TestDB_" + TestHelper.getTestClassName();
  protected HelixDataAccessor _accessor;
  private PropertyKey.Builder _keyBuilder;
  private static final AtomicInteger CANCEL_COUNT = new AtomicInteger(0);

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 3;
    super.beforeClass();

    // Stop participants that have been started in super class
    for (int i = 0; i < _numNodes; i++) {
      super.stopParticipant(i);
      Assert.assertFalse(_participants[i].isConnected());
    }

    // Start new participants that have new TaskStateModel (NewMockTask) information
    _participants = new MockParticipantManager[_numNodes];
    for (int i = 0; i < _numNodes; i++) {
      Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
      taskFactoryReg.put(NewMockTask.TASK_COMMAND, NewMockTask::new);
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }
  }

  @AfterClass()
  public void afterClass() throws Exception {
    super.afterClass();
  }

  @Test
  public void testTargetedTaskTwoCurrentStates() throws Exception {
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, DATABASE, _numPartitions,
        MASTER_SLAVE_STATE_MODEL, IdealState.RebalanceMode.SEMI_AUTO.name());
    _gSetupTool.rebalanceResource(CLUSTER_NAME, DATABASE, 3);
    List<String> preferenceList = new ArrayList<>();
    preferenceList.add(PARTICIPANT_PREFIX + "_" + (_startPort + 1));
    preferenceList.add(PARTICIPANT_PREFIX + "_" + (_startPort + 0));
    preferenceList.add(PARTICIPANT_PREFIX + "_" + (_startPort + 2));
    IdealState idealState = new IdealState(DATABASE);
    idealState.setPreferenceList(DATABASE + "_0", preferenceList);
    _gSetupTool.getClusterManagementTool().updateIdealState(CLUSTER_NAME, DATABASE, idealState);

    // [Participant0: localhost_12918, Participant1: localhost_12919, Participant2: localhost_12920]
    // Preference list [localhost_12919, localhost_12918, localhost_12920]
    // Status: [Participant1: Master, Participant0: Slave, Participant2: Slave]
    // Based on the above preference list and since is is SEMI_AUTO, localhost_12919 will be Master.
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder0 =
        new JobConfig.Builder().setWorkflow(jobQueueName).setTargetResource(DATABASE)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("JOB0", jobBuilder0);

    // Make sure master has been correctly switched to Participant1
    boolean isMasterSwitchedToCorrectInstance = TestHelper.verify(() -> {
      ExternalView externalView =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, DATABASE);
      if (externalView == null) {
        return false;
      }
      Map<String, String> stateMap = externalView.getStateMap(DATABASE + "_0");
      if (stateMap == null) {
        return false;
      }
      return "MASTER".equals(stateMap.get(PARTICIPANT_PREFIX + "_" + (_startPort + 1)));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isMasterSwitchedToCorrectInstance);

    _driver.start(jobQueue.build());

    String namespacedJobName = TaskUtil.getNamespacedJobName(jobQueueName, "JOB0");

    _driver.pollForJobState(jobQueueName, namespacedJobName, TaskState.IN_PROGRESS);

    // Task should be assigned to Master -> Participant0
    boolean isTaskAssignedToMasterNode = TestHelper.verify(() -> {
      JobContext ctx = _driver.getJobContext(namespacedJobName);
      String participant = ctx.getAssignedParticipant(0);
      if (participant == null) {
        return false;
      }
      return (participant.equals(PARTICIPANT_PREFIX + "_" + (_startPort + 1)));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isTaskAssignedToMasterNode);

    String instanceP0 = PARTICIPANT_PREFIX + "_" + (_startPort + 0);
    ZkClient clientP0 = (ZkClient) _participants[0].getZkClient();
    String sessionIdP0 = ZkTestHelper.getSessionId(clientP0);
    String currentStatePathP0 = _manager.getHelixDataAccessor().keyBuilder()
        .taskCurrentState(instanceP0, sessionIdP0, namespacedJobName).toString();

    // Get the current state of Participant1
    String instanceP1 = PARTICIPANT_PREFIX + "_" + (_startPort + 1);
    ZkClient clientP1 = (ZkClient) _participants[1].getZkClient();
    String sessionIdP1 = ZkTestHelper.getSessionId(clientP1);
    String currentStatePathP1 = _manager.getHelixDataAccessor().keyBuilder()
        .taskCurrentState(instanceP1, sessionIdP1, namespacedJobName).toString();

    boolean isCurrentStateCreated = TestHelper.verify(() -> {
      ZNRecord record = _manager.getHelixDataAccessor().getBaseDataAccessor()
          .get(currentStatePathP1, new Stat(), AccessOption.PERSISTENT);
      if (record != null) {
        record.setSimpleField(CurrentState.CurrentStateProperty.SESSION_ID.name(), sessionIdP0);
        _manager.getHelixDataAccessor().getBaseDataAccessor().set(currentStatePathP0, record,
            AccessOption.PERSISTENT);
        return true;
      } else {
        return false;
      }
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isCurrentStateCreated);

    // Wait until the job is finished.
    _driver.pollForJobState(jobQueueName, namespacedJobName, TaskState.COMPLETED);
    Assert.assertEquals(CANCEL_COUNT.get(), 0);
  }

  /**
   * A mock task that extents MockTask class to count the number of cancel messages.
   */
  private class NewMockTask extends MockTask {

    NewMockTask(TaskCallbackContext context) {
      super(context);
    }

    @Override
    public void cancel() {
      // Increment the cancel count so we know cancel() has been called
      CANCEL_COUNT.incrementAndGet();
      super.cancel();
    }
  }
}
