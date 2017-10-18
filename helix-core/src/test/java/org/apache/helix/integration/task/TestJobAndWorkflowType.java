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

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestJobAndWorkflowType extends TaskTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestJobAndWorkflowType.class);

  @Test
  public void testJobAndWorkflowType() throws InterruptedException {
    LOG.info("Start testing job and workflow type");
    String jobName = TestHelper.getTestMethodName();
    JobConfig.Builder jobConfig = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG).setJobType("JobTestType");

    Map<String, String> tmp = new HashMap<String, String>();
    tmp.put("WorkflowType", "WorkflowTestType");
    Workflow.Builder builder =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobName, jobConfig).fromMap(tmp);

    // Start workflow
    _driver.start(builder.build());

    _driver.pollForWorkflowState(jobName, TaskState.COMPLETED);
    String fetchedJobType =
        _driver.getJobConfig(String.format("%s_%s", jobName, jobName)).getJobType();
    String fetchedWorkflowType =
        _driver.getWorkflowConfig(jobName).getWorkflowType();

    Assert.assertEquals(fetchedJobType, "JobTestType");
    Assert.assertEquals(fetchedWorkflowType, "WorkflowTestType");
  }
}
