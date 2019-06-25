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
import com.google.common.collect.Sets;
import org.apache.helix.HelixException;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskSynchronizedTestBase;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestJobFailureHighThreshold extends TaskSynchronizedTestBase {

  private static final String DB_NAME = WorkflowGenerator.DEFAULT_TGT_DB;

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants = new MockParticipantManager[_numNodes];
    _numDbs = 1;
    _numNodes = 1;
    _numPartitions = 5;
    _numReplicas = 1;

    _gSetupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    startParticipants();
    createManagers();
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX);
    _controller.syncStart();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling(10000, 100));
  }

  /**
   * Number of instance is equal to number of failure threshold, thus job failure mechanism needs to
   * consider given up
   * tasks that no longer exist on the instance, not only given up tasks currently reported on
   * CurrentState.
   */
  @Test
  public void testHighThreshold() throws InterruptedException {
    final String WORKFLOW_NAME = "testWorkflow";
    final String JOB_NAME = "testJob";

    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setWorkflow(WORKFLOW_NAME).setTargetResource(DB_NAME)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND)
            .setJobCommandConfigMap(
                ImmutableMap.of(MockTask.TASK_RESULT_STATUS, TaskResult.Status.FATAL_FAILED.name()))
            .setFailureThreshold(1);
    Workflow.Builder workflowBuilder =
        new Workflow.Builder(WORKFLOW_NAME).addJob(JOB_NAME, jobBuilder);
    _driver.start(workflowBuilder.build());

    _driver.pollForJobState(WORKFLOW_NAME, TaskUtil.getNamespacedJobName(WORKFLOW_NAME, JOB_NAME),
        TaskState.FAILED);
    _driver.pollForWorkflowState(WORKFLOW_NAME, TaskState.FAILED);

    JobContext jobContext =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(WORKFLOW_NAME, JOB_NAME));
    int countAborted = 0;
    int countNoState = 0;
    for (int pId : jobContext.getPartitionSet()) {
      TaskPartitionState state = jobContext.getPartitionState(pId);
      if (state == TaskPartitionState.TASK_ABORTED) {
        countAborted++;
      } else if (state == null) {
        countNoState++;
      } else {
        throw new HelixException(String.format("State %s is not expected.", state));
      }
    }
    Assert.assertEquals(countAborted, 2); // Failure threshold is 1, so 2 tasks aborted.
    Assert.assertEquals(countNoState, 3); // Other 3 tasks are not scheduled at all.
  }
}
