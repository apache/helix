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

import com.google.common.collect.Sets;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.collect.ImmutableMap;

/**
 * Test to make sure that a job which is running will be able to continue its progress even when its
 * IdealState gets deleted.
 */
public class TestJobQueueDeleteIdealState extends TaskTestBase {
  private static final String DATABASE = WorkflowGenerator.DEFAULT_TGT_DB;
  protected HelixDataAccessor _accessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numPartitions = 1;
    _numNodes = 3;
    super.beforeClass();
  }

  @Test
  public void testJobQueueDeleteIdealState() throws Exception {
    String jobQueueName = TestHelper.getTestMethodName();

    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);

    JobConfig.Builder jobBuilder0 =
        new JobConfig.Builder().setWorkflow(jobQueueName).setTargetResource(DATABASE)
            .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
            .setCommand(MockTask.TASK_COMMAND).setExpiry(5000L)
            .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10000"));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("JOB0", jobBuilder0);
    jobQueue.enqueueJob("JOB1", jobBuilder0);

    WorkflowConfig.Builder cfgBuilder = new WorkflowConfig.Builder(jobQueue.getWorkflowConfig());
    cfgBuilder.setJobPurgeInterval(1000L);
    jobQueue.setWorkflowConfig(cfgBuilder.build());

    _driver.start(jobQueue.build());

    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"),
        TaskState.COMPLETED);

    // Wait until JOB1 goes to IN_PROGRESS
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB1"),
        TaskState.IN_PROGRESS);

    // Remove IdealState of JOB1
    PropertyKey isKey =
        _accessor.keyBuilder().idealStates(TaskUtil.getNamespacedJobName(jobQueueName, "JOB1"));
    if (_accessor.getPropertyStat(isKey) != null) {
      _accessor.removeProperty(isKey);
    }

    // Make sure IdealState has been successfully deleted
    Assert.assertNull(_accessor.getPropertyStat(isKey));

    // JOB1 should reach completed state even without IS
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB1"),
        TaskState.COMPLETED);
  }
}
