package org.apache.helix.controller.dataproviders;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.apache.helix.AccessOption;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.RuntimeJobDag;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestWorkflowControllerDataProvider extends TaskTestBase {

  @Test
  public void testResourceConfigRefresh() throws Exception {
    Workflow.Builder builder = new Workflow.Builder("TEST");
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);

    builder.addJob(WorkflowGenerator.JOB_NAME_1, jobBuilder);

    _driver.start(builder.build());

    WorkflowControllerDataProvider cache =
        new WorkflowControllerDataProvider("CLUSTER_" + TestHelper.getTestClassName());

    boolean expectedValuesAchieved = TestHelper.verify(() -> {
      cache.requireFullRefresh();
      cache.refresh(_manager.getHelixDataAccessor());
      int configMapSize = cache.getJobConfigMap().size();
      int workflowConfigMapSize = cache.getWorkflowConfigMap().size();
      int contextsSize = cache.getContexts().size();
      return (configMapSize == 1 && workflowConfigMapSize == 1 && contextsSize == 2);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(expectedValuesAchieved);

    builder = new Workflow.Builder("TEST1");
    builder.addParentChildDependency(WorkflowGenerator.JOB_NAME_1, WorkflowGenerator.JOB_NAME_2);

    builder.addJob(WorkflowGenerator.JOB_NAME_1, jobBuilder);
    builder.addJob(WorkflowGenerator.JOB_NAME_2, jobBuilder);

    _driver.start(builder.build());

    expectedValuesAchieved = TestHelper.verify(() -> {
      cache.requireFullRefresh();
      cache.refresh(_manager.getHelixDataAccessor());
      int configMapSize = cache.getJobConfigMap().size();
      int workflowConfigMapSize = cache.getWorkflowConfigMap().size();
      int contextsSize = cache.getContexts().size();
      return (configMapSize == 3 && workflowConfigMapSize == 2 && contextsSize == 5);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(expectedValuesAchieved);

  }

  @Test (dependsOnMethods = "testResourceConfigRefresh")
  public void testRuntimeDagRefresh() throws Exception {
    String jobQueueName = TestHelper.getTestMethodName();
    JobQueue.Builder builder = TaskTestUtil.buildJobQueue(jobQueueName);
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    builder.enqueueJob(WorkflowGenerator.JOB_NAME_1, jobBuilder);
    String jobName1 = TaskUtil.getNamespacedJobName(jobQueueName, WorkflowGenerator.JOB_NAME_1);
    _driver.start(builder.build());

    WorkflowControllerDataProvider cache =
        new WorkflowControllerDataProvider("CLUSTER_" + TestHelper.getTestClassName());

    Assert.assertTrue(TestHelper.verify(() -> {
      cache.requireFullRefresh();
      cache.refresh(_manager.getHelixDataAccessor());
      return cache.getTaskDataCache().getJobConfig(jobName1) != null;
    }, TestHelper.WAIT_DURATION));
    RuntimeJobDag runtimeJobDag = cache.getTaskDataCache().getRuntimeJobDag(jobQueueName);
    Assert.assertEquals(Collections.singleton(jobName1), runtimeJobDag.getAllNodes());

    // Mimic job running
    runtimeJobDag.getNextJob();

    // Add job config without adding it to the dag
    String danglingJobName = TaskUtil.getNamespacedJobName(jobQueueName, "DanglingJob");
    JobConfig danglingJobConfig = new JobConfig(danglingJobName, jobBuilder.build());
    PropertyKey.Builder keyBuilder = _manager.getHelixDataAccessor().keyBuilder();
    _baseAccessor
        .create(keyBuilder.resourceConfig(danglingJobName).getPath(), danglingJobConfig.getRecord(),
            AccessOption.PERSISTENT);

    // There shouldn't be a refresh to runtime dag. The dag should only contain one job and the job is inflight.
    Assert.assertTrue(TestHelper.verify(() -> {
      cache.requireFullRefresh();
      cache.refresh(_manager.getHelixDataAccessor());
      return cache.getTaskDataCache().getJobConfig(danglingJobName) != null;
    }, TestHelper.WAIT_DURATION));
    runtimeJobDag = cache.getTaskDataCache().getRuntimeJobDag(jobQueueName);
    Assert.assertEquals(Collections.singleton(jobName1), runtimeJobDag.getAllNodes());
    Assert.assertEquals(Collections.singleton(jobName1), runtimeJobDag.getInflightJobList());

    _driver.enqueueJob(jobQueueName, WorkflowGenerator.JOB_NAME_2, jobBuilder);
    String jobName2 = TaskUtil.getNamespacedJobName(jobQueueName, WorkflowGenerator.JOB_NAME_2);

    // There should be a refresh to runtime dag.
    Assert.assertTrue(TestHelper.verify(() -> {
      cache.requireFullRefresh();
      cache.refresh(_manager.getHelixDataAccessor());
      return cache.getTaskDataCache().getJobConfig(jobName2) != null;
    }, TestHelper.WAIT_DURATION));
    runtimeJobDag = cache.getTaskDataCache().getRuntimeJobDag(jobQueueName);
    Assert.assertEquals(new HashSet<>(Arrays.asList(jobName1, jobName2)),
        runtimeJobDag.getAllNodes());
    Assert.assertEquals(Collections.emptyList(), runtimeJobDag.getInflightJobList());
  }
}
