package org.apache.helix.integration.manager;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZkHelixAdmin extends TaskTestBase {

  private HelixAdmin _admin;
  private ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 2;
    _numPartitions = 3;
    _numReplicas = 2;
    _partitionVary = false;
    _admin = new ZKHelixAdmin(_gZkClient);
    _configAccessor = new ConfigAccessor(_gZkClient);
    super.beforeClass();
  }

  @Test
  public void testEnableDisablePartitions() throws InterruptedException {
    _admin.enablePartition(false, CLUSTER_NAME, (PARTICIPANT_PREFIX + "_" + _startPort),
        WorkflowGenerator.DEFAULT_TGT_DB, Arrays.asList(new String[] { "TestDB_0", "TestDB_2" }));
    _admin.enablePartition(false, CLUSTER_NAME, (PARTICIPANT_PREFIX + "_" + (_startPort + 1)),
        WorkflowGenerator.DEFAULT_TGT_DB, Arrays.asList(new String[] { "TestDB_0", "TestDB_2" }));

    IdealState idealState =
        _admin.getResourceIdealState(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    List<String> preferenceList =
        Arrays.asList(new String[] { "localhost_12919", "localhost_12918" });
    for (String partitionName : idealState.getPartitionSet()) {
      idealState.setPreferenceList(partitionName, preferenceList);
    }
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    _admin.setResourceIdealState(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, idealState);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setWorkflow(workflowName).setCommand(MockTask.TASK_COMMAND)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setTargetPartitionStates(Collections.singleton("SLAVE"));
    builder.addJob("JOB", jobBuilder);
    _driver.start(builder.build());
    Thread.sleep(2000L);
    JobContext jobContext =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "JOB"));
    int n = idealState.getNumPartitions();
    for ( int i = 0; i < n; i++) {
      String targetPartition = jobContext.getTargetForPartition(i);
      if (targetPartition.equals("TestDB_0") || targetPartition.equals("TestDB_2")) {
        Assert.assertEquals(jobContext.getPartitionState(i), null);
      } else {
        Assert.assertEquals(jobContext.getPartitionState(i), TaskPartitionState.COMPLETED);
      }
    }
  }
}
