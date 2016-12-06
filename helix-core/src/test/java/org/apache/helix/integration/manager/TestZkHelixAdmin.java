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

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZkHelixAdmin extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 2;
    _numParitions = 3;
    _numReplicas = 2;
    _partitionVary = false;
    super.beforeClass();
  }

  @Test public void testEnableDisablePartitions() throws InterruptedException {
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.enablePartition(false, CLUSTER_NAME, (PARTICIPANT_PREFIX + "_" + _startPort),
        WorkflowGenerator.DEFAULT_TGT_DB, Arrays.asList(new String[] { "TestDB_0", "TestDB_2" }));

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
    Assert.assertEquals(jobContext.getPartitionState(0), null);
    Assert.assertEquals(jobContext.getPartitionState(1), TaskPartitionState.COMPLETED);
    Assert.assertEquals(jobContext.getPartitionState(2), null);
  }
}
