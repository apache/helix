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

  @Test
  public void testGetExpiredJobsFromCacheFailPropagation() {
    String workflowName = "TEST_WORKFLOW_COMPLEX_DAG";
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    // Workflow Schematic:
    //                 0
    //               / | \
    //              /  |  \
    //             1   2   3
    //            /     \ /
    //          /｜\    /|\
    //         4 5  6  7 8 9

    for (int i = 0; i < 10; i++) {
      workflowBuilder.addJob("Job_" + i,
          new JobConfig.Builder().setJobId("Job_" + i).setTargetResource("1").setCommand("1"));
    }

    workflowBuilder.addParentChildDependency("Job_0", "Job_1");
    workflowBuilder.addParentChildDependency("Job_0", "Job_2");
    workflowBuilder.addParentChildDependency("Job_0", "Job_3");
    workflowBuilder.addParentChildDependency("Job_1", "Job_4");
    workflowBuilder.addParentChildDependency("Job_1", "Job_5");
    workflowBuilder.addParentChildDependency("Job_1", "Job_6");
    workflowBuilder.addParentChildDependency("Job_2", "Job_7");
    workflowBuilder.addParentChildDependency("Job_2", "Job_8");
    workflowBuilder.addParentChildDependency("Job_2", "Job_9");
    workflowBuilder.addParentChildDependency("Job_3", "Job_7");
    workflowBuilder.addParentChildDependency("Job_4", "Job_8");
    workflowBuilder.addParentChildDependency("Job_5", "Job_9");
    Workflow workflow = workflowBuilder.build();

    WorkflowContext workflowContext = mock(WorkflowContext.class);
    Map<String, TaskState> jobStates = new HashMap<>();
    jobStates.put(workflowName + "_Job_0", TaskState.FAILED);
    jobStates.put(workflowName + "_Job_1", TaskState.FAILED);
    jobStates.put(workflowName + "_Job_2", TaskState.TIMED_OUT);
    jobStates.put(workflowName + "_Job_3", TaskState.IN_PROGRESS);
    jobStates.put(workflowName + "_Job_4", TaskState.FAILED);
    jobStates.put(workflowName + "_Job_5", TaskState.FAILED);
    jobStates.put(workflowName + "_Job_6", TaskState.IN_PROGRESS);
    jobStates.put(workflowName + "_Job_7", TaskState.FAILED);
    jobStates.put(workflowName + "_Job_8", TaskState.FAILED);
    jobStates.put(workflowName + "_Job_9", TaskState.IN_PROGRESS);
    when(workflowContext.getJobStates()).thenReturn(jobStates);

    JobConfig jobConfig = mock(JobConfig.class);
    when(jobConfig.getTerminalStateExpiry()).thenReturn(1L);
    WorkflowControllerDataProvider workflowControllerDataProvider =
        mock(WorkflowControllerDataProvider.class);
    for (int i = 0; i < 10; i++) {
      when(workflowControllerDataProvider.getJobConfig(workflowName + "_Job_" + i))
          .thenReturn(jobConfig);
    }

    Long currentTime = System.currentTimeMillis();
    JobContext inProgressJobContext = mock(JobContext.class);
    JobContext failedJobContext = mock(JobContext.class);
    when(failedJobContext.getFinishTime()).thenReturn(currentTime);

    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_0"))
        .thenReturn(failedJobContext);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_1")).thenReturn(null);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_2"))
        .thenReturn(failedJobContext);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_3"))
        .thenReturn(inProgressJobContext);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_4"))
        .thenReturn(failedJobContext);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_5")).thenReturn(null);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_6"))
        .thenReturn(inProgressJobContext);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_7")).thenReturn(null);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_8")).thenReturn(null);
    when(workflowControllerDataProvider.getJobContext(workflowName + "_Job_9"))
        .thenReturn(inProgressJobContext);

    Set<String> expectedJobs = new HashSet<>();
    expectedJobs.add(workflowName + "_Job_0");
    expectedJobs.add(workflowName + "_Job_1");
    expectedJobs.add(workflowName + "_Job_2");
    expectedJobs.add(workflowName + "_Job_4");
    expectedJobs.add(workflowName + "_Job_5");
    expectedJobs.add(workflowName + "_Job_7");
    expectedJobs.add(workflowName + "_Job_8");
    Assert.assertEquals(TaskUtil
        .getExpiredJobsFromCache(workflowControllerDataProvider, workflow.getWorkflowConfig(),
            workflowContext), expectedJobs);
  }
}
