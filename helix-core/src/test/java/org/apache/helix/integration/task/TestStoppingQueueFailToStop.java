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
import java.util.concurrent.CountDownLatch;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * Test to check if waitToStop method correctly throws an Exception if Queue stuck in STOPPING
 * state.
 */
public class TestStoppingQueueFailToStop extends TaskTestBase {
  private static final String DATABASE = WorkflowGenerator.DEFAULT_TGT_DB;
  private CountDownLatch latch = new CountDownLatch(1);

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 3;
    super.beforeClass();
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);

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

    _manager.connect();
    _driver = new TaskDriver(_manager);
  }

  @Test
  public void testStoppingQueueFailToStop() throws Exception {
    String jobQueueName = TestHelper.getTestMethodName();
    JobConfig.Builder jobBuilder0 =
        new JobConfig.Builder().setWorkflow(jobQueueName).setTargetResource(DATABASE)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("JOB0", jobBuilder0);
    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"),
        TaskState.IN_PROGRESS);
    boolean exceptionHappened = false;
    try {
      _driver.waitToStop(jobQueueName, 5000L);
    } catch (HelixException e) {
      exceptionHappened = true;
    }
    _driver.pollForWorkflowState(jobQueueName, TaskState.STOPPING);
    Assert.assertTrue(exceptionHappened);
    latch.countDown();
  }

  /**
   * A mock task that extents MockTask class and stuck in running when cancel is called.
   */
  private class NewMockTask extends MockTask {

    NewMockTask(TaskCallbackContext context) {
      super(context);
    }

    @Override
    public void cancel() {
      try {
        latch.await();
      } catch (Exception e) {
        // Pass
      }
      super.cancel();
    }
  }
}
