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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.TestHelper;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestWorkflowControllerDataProvider extends TaskTestBase {

  @Test
  public void testResourceConfigRefresh() throws InterruptedException {
    Workflow.Builder builder = new Workflow.Builder("TEST");
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);

    builder.addJob(WorkflowGenerator.JOB_NAME_1, jobBuilder);

    _driver.start(builder.build());
    Thread.sleep(4000);
    WorkflowControllerDataProvider cache =
        new WorkflowControllerDataProvider("CLUSTER_" + TestHelper.getTestClassName());
    cache.requireFullRefresh();
    cache.refresh(_manager.getHelixDataAccessor());
    Assert.assertEquals(cache.getJobConfigMap().size(), 1);
    Assert.assertEquals(cache.getWorkflowConfigMap().size(), 1);
    Assert.assertEquals(cache.getContexts().size(), 2);


    builder = new Workflow.Builder("TEST1");
    builder.addParentChildDependency(WorkflowGenerator.JOB_NAME_1, WorkflowGenerator.JOB_NAME_2);

    builder.addJob(WorkflowGenerator.JOB_NAME_1, jobBuilder);
    builder.addJob(WorkflowGenerator.JOB_NAME_2, jobBuilder);

    _driver.start(builder.build());
    Thread.sleep(4000);
    cache.requireFullRefresh();
    cache.refresh(_manager.getHelixDataAccessor());
    Assert.assertEquals(cache.getJobConfigMap().size(), 3);
    Assert.assertEquals(cache.getWorkflowConfigMap().size(), 2);
    Assert.assertEquals(cache.getContexts().size(), 5);
  }
}
