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

import org.apache.helix.HelixManager;
import org.apache.helix.TestHelper;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
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

  public static void pollForJobState(HelixManager manager, String workflowResource, String jobName,
      TaskState state) throws InterruptedException {
    // Wait for completion.
    long st = System.currentTimeMillis();
    WorkflowContext ctx;
    do {
      Thread.sleep(100);
      ctx = TaskUtil.getWorkflowContext(manager, workflowResource);
    } while ((ctx == null || ctx.getJobState(jobName) == null || ctx.getJobState(jobName) != state)
        && System.currentTimeMillis() < st + _default_timeout);
    Assert.assertNotNull(ctx);
    Assert.assertEquals(ctx.getJobState(jobName), state);
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
}
