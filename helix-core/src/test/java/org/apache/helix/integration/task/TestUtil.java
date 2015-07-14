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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;

/**
 * Static test utility methods.
 */
public class TestUtil {
  private final static int _default_timeout = 2 * 60 * 1000; /* 2 mins */

  /**
   * Polls {@link org.apache.helix.task.JobContext} for given task resource until a timeout is
   * reached.
   * If the task has not reached target state by then, an error is thrown
   * @param workflowResource Resource to poll for completeness
   * @throws InterruptedException
   */
  public static void pollForWorkflowState(HelixManager manager, String workflowResource,
      TaskState state) throws InterruptedException {
    // Wait for completion.
    long st = System.currentTimeMillis();
    WorkflowContext ctx;
    do {
      Thread.sleep(100);
      ctx = TaskUtil.getWorkflowContext(manager, workflowResource);
    } while ((ctx == null || ctx.getWorkflowState() == null || ctx.getWorkflowState() != state)
        && System.currentTimeMillis() < st + _default_timeout);

    Assert.assertNotNull(ctx);
    Assert.assertEquals(ctx.getWorkflowState(), state);
  }

  /**
   * poll for job until it is at either state in targetStates.
   * @param manager
   * @param workflowResource
   * @param jobName
   * @param targetStates
   * @throws InterruptedException
   */
  public static void pollForJobState(HelixManager manager, String workflowResource, String jobName,
      TaskState... targetStates) throws InterruptedException {
    // Get workflow config
    WorkflowConfig wfCfg = TaskUtil.getWorkflowCfg(manager, workflowResource);
    Assert.assertNotNull(wfCfg);
    WorkflowContext ctx;
    if (wfCfg.isRecurring()) {
      // if it's recurring, need to reconstruct workflow and job name
      do {
        Thread.sleep(100);
        ctx = TaskUtil.getWorkflowContext(manager, workflowResource);
      } while ((ctx == null || ctx.getLastScheduledSingleWorkflow() == null));
      Assert.assertNotNull(ctx);
      Assert.assertNotNull(ctx.getLastScheduledSingleWorkflow());
      jobName = jobName.substring(workflowResource.length() + 1);
      workflowResource = ctx.getLastScheduledSingleWorkflow();
      jobName = String.format("%s_%s", workflowResource, jobName);
    }

    Set<TaskState> allowedStates = new HashSet<TaskState>(Arrays.asList(targetStates));
    // Wait for state
    long st = System.currentTimeMillis();
    do {
      Thread.sleep(100);
      ctx = TaskUtil.getWorkflowContext(manager, workflowResource);
    }
    while ((ctx == null || ctx.getJobState(jobName) == null || !allowedStates.contains(ctx.getJobState(jobName)))
        && System.currentTimeMillis() < st + _default_timeout);
    Assert.assertNotNull(ctx);
    Assert.assertTrue(allowedStates.contains(ctx.getJobState(jobName)));
  }

  public static void pollForEmptyJobState(final HelixManager manager, final String workflowName,
      final String jobName) throws Exception {
    final String namespacedJobName = String.format("%s_%s", workflowName, jobName);
    boolean succeed = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        WorkflowContext ctx = TaskUtil.getWorkflowContext(manager, workflowName);
        return ctx == null || ctx.getJobState(namespacedJobName) == null;
      }
    }, _default_timeout);
    Assert.assertTrue(succeed);
  }

  public static WorkflowContext pollForWorkflowContext(HelixManager manager, String workflowResource)
      throws InterruptedException {
    // Wait for completion.
    long st = System.currentTimeMillis();
    WorkflowContext ctx;
    do {
      Thread.sleep(100);
      ctx = TaskUtil.getWorkflowContext(manager, workflowResource);
    } while (ctx == null && System.currentTimeMillis() < st + _default_timeout);
    Assert.assertNotNull(ctx);
    return ctx;
  }

  // 1. Different jobs in a same work flow is in RUNNING at the same time
  // 2. No two jobs in the same work flow is in RUNNING at the same instance
  public static boolean pollForWorkflowParallelState(HelixManager manager, String workflowName)
      throws InterruptedException {

    WorkflowConfig workflowConfig = TaskUtil.getWorkflowCfg(manager, workflowName);
    Assert.assertNotNull(workflowConfig);

    WorkflowContext workflowContext = null;
    while (workflowContext == null) {
      workflowContext = TaskUtil.getWorkflowContext(manager, workflowName);
      Thread.sleep(100);
    }

    int maxRunningCount = 0;
    boolean finished = false;

    while (!finished) {
      finished = true;
      int runningCount = 0;

      workflowContext = TaskUtil.getWorkflowContext(manager, workflowName);
      for (String jobName : workflowConfig.getJobDag().getAllNodes()) {
        TaskState jobState = workflowContext.getJobState(jobName);
        if (jobState == TaskState.IN_PROGRESS) {
          ++runningCount;
          finished = false;
        }
      }

      if (runningCount > maxRunningCount ) {
        maxRunningCount = runningCount;
      }

      List<JobContext> jobContextList = new ArrayList<JobContext>();
      for (String jobName : workflowConfig.getJobDag().getAllNodes()) {
        JobContext jobContext = TaskUtil.getJobContext(manager, jobName);
        if (jobContext != null) {
          jobContextList.add(TaskUtil.getJobContext(manager, jobName));
        }
      }

      Set<String> instances = new HashSet<String>();
      for (JobContext jobContext : jobContextList) {
        for (int partition : jobContext.getPartitionSet()) {
          String instance = jobContext.getAssignedParticipant(partition);
          TaskPartitionState taskPartitionState = jobContext.getPartitionState(partition);

          if (instance == null) {
            continue;
          }
          if (taskPartitionState != TaskPartitionState.INIT &&
              taskPartitionState != TaskPartitionState.RUNNING) {
            continue;
          }
          if (instances.contains(instance)) {
            return false;
          }

          TaskPartitionState state = jobContext.getPartitionState(partition);
          if (state != TaskPartitionState.COMPLETED) {
            instances.add(instance);
          }
        }
      }

      Thread.sleep(100);
    }

    return maxRunningCount > 1 && maxRunningCount <= workflowConfig.getParallelJobs();
  }

  public static boolean pollForParticipantParallelState() {
    return false;
  }
}
