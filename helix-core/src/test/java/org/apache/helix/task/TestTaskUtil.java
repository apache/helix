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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.TaskTestUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestTaskUtil extends TaskTestBase {

  @Test
  public void testGetExpiredJobsFromCache() {
    String workflowName = "TEST_WORKFLOW";
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(workflowName);

    JobConfig.Builder jobBuilder_0 =
        new JobConfig.Builder().setJobId("Job_0").setTargetResource("1").setCommand("1")
            .setExpiry(1L);
    JobConfig.Builder jobBuilder_1 =
        new JobConfig.Builder().setJobId("Job_1").setTargetResource("1").setCommand("1")
            .setExpiry(1L);
    JobConfig.Builder jobBuilder_2 =
        new JobConfig.Builder().setJobId("Job_2").setTargetResource("1").setCommand("1")
            .setExpiry(1L);
    JobConfig.Builder jobBuilder_3 =
        new JobConfig.Builder().setJobId("Job_3").setTargetResource("1").setCommand("1")
            .setExpiry(1L);
    Workflow jobQueue =
        queueBuilder.enqueueJob("Job_0", jobBuilder_0).enqueueJob("Job_1", jobBuilder_1)
            .enqueueJob("Job_2", jobBuilder_2).enqueueJob("Job_3", jobBuilder_3).build();

    WorkflowContext workflowContext = mock(WorkflowContext.class);
    Map<String, TaskState> jobStates = new HashMap<>();
    jobStates.put(workflowName + "_Job_0", TaskState.COMPLETED);
    jobStates.put(workflowName + "_Job_1", TaskState.COMPLETED);
    jobStates.put(workflowName + "_Job_2", TaskState.FAILED);
    jobStates.put(workflowName + "_Job_3", TaskState.COMPLETED);
    when(workflowContext.getJobStates()).thenReturn(jobStates);

    JobConfig jobConfig = mock(JobConfig.class);
    WorkflowControllerDataProvider workflowControllerDataProvider =
        mock(WorkflowControllerDataProvider.class);
    when(workflowControllerDataProvider.getJobConfig(workflowName + "_Job_1")).thenReturn(null);
    when(workflowControllerDataProvider.getJobConfig(workflowName + "_Job_1"))
        .thenReturn(jobConfig);
    when(workflowControllerDataProvider.getJobConfig(workflowName + "_Job_2"))
        .thenReturn(jobConfig);
    when(workflowControllerDataProvider.getJobConfig(workflowName + "_Job_3"))
        .thenReturn(jobConfig);

    JobContext jobContext = mock(JobContext.class);
    when(jobContext.getFinishTime()).thenReturn(System.currentTimeMillis());

    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_1")).thenReturn(null);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_2"))
        .thenReturn(jobContext);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_3"))
        .thenReturn(jobContext);

    Set<String> expectedJobs = new HashSet<>();
    expectedJobs.add(workflowName + "_Job_0");
    expectedJobs.add(workflowName + "_Job_3");
    Assert.assertEquals(TaskUtil
        .getExpiredJobsFromCache(workflowControllerDataProvider, jobQueue.getWorkflowConfig(),
            workflowContext), expectedJobs);
  }
}
