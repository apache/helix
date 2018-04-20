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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskSynchronizedTestBase;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.TestInputLoader;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public final class TestJobFailure extends TaskSynchronizedTestBase {

  private ClusterControllerManager _controller;
  private final String DB_NAME = WorkflowGenerator.DEFAULT_TGT_DB;

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants = new MockParticipantManager[_numNodes];
    _numNodes = 2;
    _numParitions = 2;
    _numReplicas = 1; // only Master, no Slave
    _numDbs = 1;

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursively(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    startParticipants();
    createManagers();
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX);
    _controller.syncStart();

    Thread.sleep(1000); // Wait for cluster to setup.
  }

  private static final String EXPECTED_ENDING_STATE = "ExpectedEndingState";
  private static int testNum = 0;

  @Test(dataProvider = "testJobFailureInput")
  public void testNormalJobFailure(String comment, List<String> taskStates, List<String> expectedTaskEndingStates,
      String expectedJobEndingStates, String expectedWorkflowEndingStates) throws InterruptedException {
    final String JOB_NAME = "test_job";
    final String WORKFLOW_NAME = TestHelper.getTestMethodName() + testNum++;
    System.out.println("Test case comment: " + comment);

    Map<String, Map<String, String>> targetPartitionConfigs =
        createPartitionConfig(taskStates, expectedTaskEndingStates);

    JobConfig.Builder firstJobBuilder = new JobConfig.Builder()
        .setWorkflow(WORKFLOW_NAME)
        .setTargetResource(DB_NAME)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(
            MockTask.TARGET_PARTITION_CONFIG, MockTask.serializeTargetPartitionConfig(targetPartitionConfigs)));

    Workflow.Builder workflowBuilder = new Workflow.Builder(WORKFLOW_NAME)
        .addJob(JOB_NAME, firstJobBuilder);

    _driver.start(workflowBuilder.build());

    _driver.pollForJobState(WORKFLOW_NAME, TaskUtil.getNamespacedJobName(WORKFLOW_NAME, JOB_NAME),
        TaskState.valueOf(expectedJobEndingStates));
    _driver.pollForWorkflowState(WORKFLOW_NAME, TaskState.valueOf(expectedWorkflowEndingStates));

    JobContext jobContext = _driver.getJobContext(TaskUtil.getNamespacedJobName(WORKFLOW_NAME, JOB_NAME));
    for (int pId : jobContext.getPartitionSet()) {
      Map<String, String> targetPartitionConfig = targetPartitionConfigs.get(jobContext.getTargetForPartition(pId));
      Assert.assertEquals(jobContext.getPartitionState(pId).name(), targetPartitionConfig.get(EXPECTED_ENDING_STATE));
    }
  }

  @DataProvider(name = "testJobFailureInput")
  public Object[][] loadtestJobFailureInput() {
    String[] params = {"comment", "taskStates", "expectedTaskEndingStates", "expectedJobEndingStates",
        "expectedWorkflowEndingStates"};
    return TestInputLoader.loadTestInputs("TestJobFailure.json", params);
  }

  private Map<String, Map<String, String>> createPartitionConfig(List<String> taskStates,
      List<String> expectedTaskEndingStates) {
    Map<String, Map<String, String>> targetPartitionConfigs = new HashMap<String, Map<String, String>>();
    ExternalView externalView = _manager.getClusterManagmentTool().getResourceExternalView(CLUSTER_NAME, DB_NAME);
    Set<String> partitionSet = externalView.getPartitionSet();
    if (taskStates.size() != partitionSet.size()) {
      throw new IllegalArgumentException(
          "Input size does not match number of partitions for target resource: " + DB_NAME);
    }
    int i = 0;
    // Set job command configs for target partitions(order doesn't matter) according to specified task states.
    for (String partition : partitionSet) {
      Map<String, String> config = new HashMap<String, String>();
      if (taskStates.get(i).equals(TaskPartitionState.COMPLETED.name())) {
        config.put(MockTask.TASK_RESULT_STATUS, TaskResult.Status.COMPLETED.name());
      } else if (taskStates.get(i).equals(TaskPartitionState.TASK_ERROR.name())) {
        config.put(MockTask.TASK_RESULT_STATUS, TaskResult.Status.FAILED.name());
      } else if (taskStates.get(i).equals(TaskPartitionState.TASK_ABORTED.name())) {
        config.put(MockTask.TASK_RESULT_STATUS, TaskResult.Status.FATAL_FAILED.name());
      } else if (taskStates.get(i).equals(TaskPartitionState.RUNNING.name())) {
        config.put(MockTask.JOB_DELAY, "99999999");
      } else {
        throw new IllegalArgumentException("Invalid taskStates input: " + taskStates.get(i));
      }
      config.put(EXPECTED_ENDING_STATE, expectedTaskEndingStates.get(i));
      targetPartitionConfigs.put(partition, config);
      i++;
    }
    return targetPartitionConfigs;
  }
}
