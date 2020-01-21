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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableMap;

/**
 * Test To check if targeted tasks correctly assigned to the instances when participants disconnect
 * and reconnect.
 */
public class TestTargetedTaskParticipantDisconnect extends TaskTestBase {
  private final String DATABASE = WorkflowGenerator.DEFAULT_TGT_DB;
  protected HelixDataAccessor _accessor;
  private PropertyKey.Builder _keyBuilder;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 3;
    super.beforeClass();
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    _driver = new TaskDriver(_manager);
  }

  @Test
  public void testTargetedTaskParticipantDisconnect() throws Exception {
    String jobQueueName = TestHelper.getTestMethodName();

    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    _keyBuilder = _accessor.keyBuilder();
    ClusterConfig clusterConfig = _accessor.getProperty(_keyBuilder.clusterConfig());
    clusterConfig.setPersistIntermediateAssignment(true);
    clusterConfig.setRebalanceTimePeriod(10000L);
    _accessor.setProperty(_keyBuilder.clusterConfig(), clusterConfig);

    List<String> preferenceList = new ArrayList<>();
    for (int i = 0; i < _numNodes; i++) {
      preferenceList.add(PARTICIPANT_PREFIX + "_" + (_startPort + i));
    }

    // Change the Rebalance Mode to SEMI_AUTO
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DATABASE);
    idealState.setPreferenceList(DATABASE + "_0", preferenceList);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, DATABASE,
        idealState);

    // [Participant0: localhost_12918, Participant1: localhost_12919, Participant2: localhost_12920]
    // Preference list [localhost_12918, localhost_12919, localhost_12920]
    // Status: [Participant0: Master, Participant1: Slave, Participant2: Slave]
    // Based on the above preference list and since is is SEMI_AUTO, localhost_12918 will be Master.
    JobConfig.Builder jobBuilder0 = new JobConfig.Builder().setWorkflow(jobQueueName)
        .setTargetResource(DATABASE)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "20000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("JOB0", jobBuilder0);

    _driver.start(jobQueue.build());

    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"),
        TaskState.IN_PROGRESS);


    // Task should be assigned to Master -> Participant0
    boolean isTaskAssignedToMasterNode = TestHelper.verify(() -> {
      JobContext ctx =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"));
      String participant = ctx.getAssignedParticipant(0);
      return (participant.equals(PARTICIPANT_PREFIX + "_" + (_startPort + 0)));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isTaskAssignedToMasterNode);

    _participants[0].syncStop();

    // Participant0 has been disconnected
    // Status: [Participant0: Offline, Participant1: Master, Participant2: Slave]
    // Task should be assigned to new Master -> Participant1
    isTaskAssignedToMasterNode = TestHelper.verify(() -> {
      JobContext ctx =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"));
      String participant = ctx.getAssignedParticipant(0);
      return (participant.equals(PARTICIPANT_PREFIX + "_" + (_startPort + 1)));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isTaskAssignedToMasterNode);

    _participants[1].syncStop();

    // Participant1 has been disconnected
    // Status: [Participant0: Offline, Participant1: Offline, Participant2: Master]
    // Task should be assigned to new Master -> Participant2
    isTaskAssignedToMasterNode = TestHelper.verify(() -> {
      JobContext ctx =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"));
      String participant = ctx.getAssignedParticipant(0);
      return (participant.equals(PARTICIPANT_PREFIX + "_" + (_startPort + 2)));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isTaskAssignedToMasterNode);

    // Participant1 is now connected again
    // Status: [Participant0: Offline, Participant1: Master, Participant2: Slave]
    // Task should be assigned to new Master -> Participant1
    String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + 1);
    _participants[0] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    startParticipant(1);

    isTaskAssignedToMasterNode = TestHelper.verify(() -> {
      JobContext ctx =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"));
      String participant = ctx.getAssignedParticipant(0);
      return (participant.equals(PARTICIPANT_PREFIX + "_" + (_startPort + 1)));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isTaskAssignedToMasterNode);


    // Participant0 is now connected again
    // Status: [Participant0: Master, Participant1: Slave, Participant2: Slave]
    // Task should be assigned to new Master -> Participant0
    instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + 0);
    _participants[0] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    startParticipant(0);

    isTaskAssignedToMasterNode = TestHelper.verify(() -> {
      JobContext ctx =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"));
      String participant = ctx.getAssignedParticipant(0);
      return (participant.equals(PARTICIPANT_PREFIX + "_" + (_startPort + 0)));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isTaskAssignedToMasterNode);


    // Wait until the job is finished.
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"),
        TaskState.COMPLETED);


    // NUmber of attempts should be 5 (There won't be multiple assignment and reassignment).
    // Here is the assignments
    // 1- Participant0
    // 2- Participant1
    // 3- Participant2
    // 4- Participant1
    // 5- Participant0
    JobContext ctx =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"));
    Assert.assertEquals(ctx.getPartitionNumAttempts(0), 5);
  }
}
