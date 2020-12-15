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

import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.model.CurrentState;
import org.apache.helix.task.JobConfig;
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

/**
 * This test makes sure that controller will not be blocked if there exists null current states.
 */
public class TestTaskCurrentStateNull extends TaskTestBase {
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
  public void testCurrentStateNull() throws Exception {
    String workflowName1 = TestHelper.getTestMethodName() + "_1";
    String workflowName2 = TestHelper.getTestMethodName() + "_2";

    Workflow.Builder builder1 = new Workflow.Builder(workflowName1);
    Workflow.Builder builder2 = new Workflow.Builder(workflowName2);

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName1)
        .setNumberOfTasks(5).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    JobConfig.Builder jobBuilder2 = new JobConfig.Builder().setWorkflow(workflowName2)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    builder1.addJob("JOB0", jobBuilder1);
    builder2.addJob("JOB0", jobBuilder2);

    _driver.start(builder1.build());
    _driver.start(builder2.build());

    String namespacedJobName1 = TaskUtil.getNamespacedJobName(workflowName1, "JOB0");
    String namespacedJobName2 = TaskUtil.getNamespacedJobName(workflowName2, "JOB0");

    _driver.pollForJobState(workflowName1, namespacedJobName1, TaskState.IN_PROGRESS);
    _driver.pollForJobState(workflowName2, namespacedJobName2, TaskState.IN_PROGRESS);

    // Get the current states of Participant0
    String instanceP0 = PARTICIPANT_PREFIX + "_" + (_startPort + 0);
    ZkClient clientP0 = (ZkClient) _participants[0].getZkClient();
    String sessionIdP0 = ZkTestHelper.getSessionId(clientP0);
    String jobCurrentStatePath1 = _manager.getHelixDataAccessor().keyBuilder()
        .taskCurrentState(instanceP0, sessionIdP0, namespacedJobName1).toString();
    String jobCurrentStatePath2 = _manager.getHelixDataAccessor().keyBuilder()
        .taskCurrentState(instanceP0, sessionIdP0, namespacedJobName2).toString();

    // Read the current states of Participant0 and make sure they have been created
    boolean isCurrentStateCreated = TestHelper.verify(() -> {
      ZNRecord recordJob1 = _manager.getHelixDataAccessor().getBaseDataAccessor()
          .get(jobCurrentStatePath1, new Stat(), AccessOption.PERSISTENT);
      ZNRecord recordJob2 = _manager.getHelixDataAccessor().getBaseDataAccessor()
          .get(jobCurrentStatePath2, new Stat(), AccessOption.PERSISTENT);
      Map<String, String> taskCurrentState = null;
      if (recordJob2 != null){
        taskCurrentState = recordJob2.getMapField(namespacedJobName2 + "_0");
      }
      return (recordJob1 != null && recordJob2 != null && taskCurrentState != null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(isCurrentStateCreated);

    ZNRecord recordTask = _manager.getHelixDataAccessor().getBaseDataAccessor()
        .get(jobCurrentStatePath2, new Stat(), AccessOption.PERSISTENT);
    Map<String, String> taskCurrentState = recordTask.getMapField(namespacedJobName2 + "_0");
    taskCurrentState.put(CurrentState.CurrentStateProperty.CURRENT_STATE.name(), null);
    recordTask.setMapField(namespacedJobName2 + "_0", taskCurrentState);

    _manager.getHelixDataAccessor().getBaseDataAccessor().set(jobCurrentStatePath2, recordTask,
        AccessOption.PERSISTENT);
    _driver.pollForWorkflowState(workflowName1, TestHelper.WAIT_DURATION, TaskState.COMPLETED);
  }
}
