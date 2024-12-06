package org.apache.helix.task;

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

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestJobTagRemoval extends TaskTestBase {

  @Test
  public void testJobTagRemoval() throws InterruptedException {
    String TEST_TAG = "testTag";

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    InstanceConfig cfg =
        configAccessor.getInstanceConfig(CLUSTER_NAME, _participants[0].getInstanceName());
    cfg.addTag(TEST_TAG);
    configAccessor.setInstanceConfig(CLUSTER_NAME, _participants[0].getInstanceName(), cfg);

    Workflow.Builder builder = new Workflow.Builder("testWorkflow");
    List<TaskConfig> taskConfigs = new ArrayList<>();
    taskConfigs.add(
        new TaskConfig.Builder().setTaskId("tagged_task").setCommand(MockTask.TASK_COMMAND)
            .build());
    JobConfig.Builder jobBuilder = new JobConfig.Builder().addTaskConfigs(taskConfigs)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "5000"))
        .setInstanceGroupTag(TEST_TAG);
    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().addTaskConfigs(taskConfigs)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"))
        .setInstanceGroupTag(TEST_TAG);
    builder.addJob("JOB0", jobBuilder);
    builder.addJob("JOB1", jobBuilder1);
    builder.addParentChildDependency("JOB0", "JOB1");
    _driver.start(builder.build());

    // Wait for the job to be created
    _driver.pollForJobState("testWorkflow", "testWorkflow_JOB0", TaskState.IN_PROGRESS);

    // Remove the tag
    cfg = configAccessor.getInstanceConfig(CLUSTER_NAME, _participants[0].getInstanceName());
    cfg.removeTag(TEST_TAG);
    configAccessor.setInstanceConfig(CLUSTER_NAME, _participants[0].getInstanceName(), cfg);

    // Wait for the job to complete
    _driver.pollForJobState("testWorkflow", "testWorkflow_JOB0", TaskState.COMPLETED);
    _driver.pollForJobState("testWorkflow", "testWorkflow_JOB1", TaskState.IN_PROGRESS);
    JobContext ctx = _driver.getJobContext("testWorkflow_JOB1");
    Assert.assertEquals(ctx.getPartitionState(0), null);
  }
}
