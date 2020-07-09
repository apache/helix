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

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * Test to check is maximum number of attempts being respected while target partition is switching
 * continuously.
 */
public class TestMaxNumberOfAttemptsMasterSwitch extends TaskTestBase {
  private static final String DATABASE = WorkflowGenerator.DEFAULT_TGT_DB;
  protected HelixDataAccessor _accessor;
  private PropertyKey.Builder _keyBuilder;
  private List<String> _preferenceList;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 3;
    super.beforeClass();
    _driver = new TaskDriver(_manager);
    _preferenceList = new ArrayList<>();
    _preferenceList.add(PARTICIPANT_PREFIX + "_" + (_startPort + 0));
    _preferenceList.add(PARTICIPANT_PREFIX + "_" + (_startPort + 1));
    _preferenceList.add(PARTICIPANT_PREFIX + "_" + (_startPort + 2));
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
  }

  @Test
  public void testMaxNumberOfAttemptsMasterSwitch() throws Exception {
    String jobQueueName = TestHelper.getTestMethodName();
    int maxNumberOfAttempts = 5;

    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    _keyBuilder = _accessor.keyBuilder();
    ClusterConfig clusterConfig = _accessor.getProperty(_keyBuilder.clusterConfig());
    _accessor.setProperty(_keyBuilder.clusterConfig(), clusterConfig);

    // Change the Rebalance Mode to SEMI_AUTO
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DATABASE);
    idealState.setPreferenceList(DATABASE + "_0", _preferenceList);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, DATABASE,
        idealState);

    JobConfig.Builder jobBuilder0 =
        new JobConfig.Builder().setWorkflow(jobQueueName).setTargetResource(DATABASE)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(maxNumberOfAttempts)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("JOB0", jobBuilder0);
    String nameSpacedJobName = TaskUtil.getNamespacedJobName(jobQueueName, "JOB0");

    _driver.start(jobQueue.build());
    _driver.pollForJobState(jobQueueName, nameSpacedJobName, TaskState.IN_PROGRESS);
    boolean isInstanceAlive = true;

    // Turn on and off the instance (10 times) and make sure task gets retried and number of
    // attempts gets
    // incremented every time.
    // Also make sure that the task won't be retried more than maxNumberOfAttempts
    for (int i = 1; i <= 2 * maxNumberOfAttempts; i++) {
      int expectedRetryNumber = Math.min(i, maxNumberOfAttempts);
      Assert
          .assertTrue(
              TestHelper.verify(
                  () -> (_driver.getJobContext(nameSpacedJobName)
                      .getPartitionNumAttempts(0) == expectedRetryNumber),
                  TestHelper.WAIT_DURATION));
      if (isInstanceAlive) {
        stopParticipant(0);
        isInstanceAlive = false;
      } else {
        startParticipant(0);
        isInstanceAlive = true;
      }
    }

    // Since the task reaches max number of attempts, ths job will fails.
    _driver.pollForJobState(jobQueueName, nameSpacedJobName, TaskState.FAILED);
    Assert.assertEquals(_driver.getJobContext(nameSpacedJobName).getPartitionNumAttempts(0),
        maxNumberOfAttempts);
  }
}
