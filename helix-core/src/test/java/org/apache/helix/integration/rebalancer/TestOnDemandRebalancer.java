package org.apache.helix.integration.rebalancer;

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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * This test check the funcionality of OnDemandRebalancer
 */
public class TestOnDemandRebalancer extends TaskTestBase {
  @Test
  public void testOndemandRebalancer() throws Exception {
    String jobName = "JOB0";
    long expiry = 1000;
    String delay = "1000";
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, delay));

    // Generate simple workflow with single job
    Workflow workflow = WorkflowGenerator.generateSingleJobWorkflowBuilder(jobName, jobBuilder)
        .setExpiry(expiry).build();

    // Start the workflow and wait for it to go to IN_PROGRESS state
    _driver.start(workflow);
    _driver.pollForWorkflowState(jobName, TaskState.IN_PROGRESS);

    // Running workflow should have config and context viewable through accessor
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey workflowCfgKey = accessor.keyBuilder().resourceConfig(jobName);
    String workflowPropStoreKey =
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobName);

    // Ensure context and config exist
    Assert.assertTrue(
        _manager.getHelixPropertyStore().exists(workflowPropStoreKey, AccessOption.PERSISTENT));
    Assert.assertNotSame(accessor.getProperty(workflowCfgKey), null);

    // Wait for job to finish
    _driver.pollForWorkflowState(jobName, TaskState.COMPLETED);

    // Wait a little while after expiration/expiry time to check config and context clean up
    Thread.sleep(expiry + 100);

    // Ensure workflow config and context were cleaned up by now
    // This specifically tests TaskUtil's purgeExpiredJobs functionality which includes
    // scheduleOnDemandPipeline
    Assert.assertFalse(
        _manager.getHelixPropertyStore().exists(workflowPropStoreKey, AccessOption.PERSISTENT));
    Assert.assertNull(accessor.getProperty(workflowCfgKey));
  }
}
