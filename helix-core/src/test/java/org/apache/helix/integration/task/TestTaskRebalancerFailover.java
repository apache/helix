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

import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.model.ExternalView;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestTaskRebalancerFailover extends TaskTestBase {
  private static final Logger LOG = Logger.getLogger(TestTaskRebalancerFailover.class);

  @Test
  public void test() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue queue = new JobQueue.Builder(queueName).build();
    _driver.createQueue(queue);

    // Enqueue jobs
    Set<String> master = Sets.newHashSet("MASTER");
    JobConfig.Builder job =
        new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(master);
    String job1Name = "masterJob";
    LOG.info("Enqueuing job: " + job1Name);
    _driver.enqueueJob(queueName, job1Name, job);

    // check all tasks completed on MASTER
    String namespacedJob1 = String.format("%s_%s", queueName, job1Name);
    TaskTestUtil.pollForJobState(_driver, queueName, namespacedJob1, TaskState.COMPLETED);

    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    ExternalView ev =
        accessor.getProperty(keyBuilder.externalView(WorkflowGenerator.DEFAULT_TGT_DB));
    JobContext ctx = _driver.getJobContext(namespacedJob1);
    Set<String> failOverPartitions = Sets.newHashSet();
    for (int p = 0; p < _numParitions; p++) {
      String instanceName = ctx.getAssignedParticipant(p);
      Assert.assertNotNull(instanceName);
      String partitionName = ctx.getTargetForPartition(p);
      Assert.assertNotNull(partitionName);
      String state = ev.getStateMap(partitionName).get(instanceName);
      Assert.assertNotNull(state);
      Assert.assertEquals(state, "MASTER");
      if (instanceName.equals("localhost_12918")) {
        failOverPartitions.add(partitionName);
      }
    }

    // enqueue another master job and fail localhost_12918
    String job2Name = "masterJob2";
    String namespacedJob2 = String.format("%s_%s", queueName, job2Name);
    LOG.info("Enqueuing job: " + job2Name);
    _driver.enqueueJob(queueName, job2Name, job);

    TaskTestUtil.pollForJobState(_driver, queueName, namespacedJob2, TaskState.IN_PROGRESS);
    _participants[0].syncStop();
    TaskTestUtil.pollForJobState(_driver, queueName, namespacedJob2, TaskState.COMPLETED);

    // tasks previously assigned to localhost_12918 should be re-scheduled on new master
    ctx = _driver.getJobContext(namespacedJob2);
    ev = accessor.getProperty(keyBuilder.externalView(WorkflowGenerator.DEFAULT_TGT_DB));
    for (int p = 0; p < _numParitions; p++) {
      String partitionName = ctx.getTargetForPartition(p);
      Assert.assertNotNull(partitionName);
      if (failOverPartitions.contains(partitionName)) {
        String instanceName = ctx.getAssignedParticipant(p);
        Assert.assertNotNull(instanceName);
        Assert.assertNotSame(instanceName, "localhost_12918");
        String state = ev.getStateMap(partitionName).get(instanceName);
        Assert.assertNotNull(state);
        Assert.assertEquals(state, "MASTER");
      }
    }

    // Flush queue and check cleanup
    _driver.flushQueue(queueName);
    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(namespacedJob1)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(namespacedJob1)));
    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(namespacedJob2)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(namespacedJob2)));
    WorkflowConfig workflowCfg = _driver.getWorkflowConfig(queueName);
    JobDag dag = workflowCfg.getJobDag();
    Assert.assertFalse(dag.getAllNodes().contains(namespacedJob1));
    Assert.assertFalse(dag.getAllNodes().contains(namespacedJob2));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namespacedJob1));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namespacedJob2));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namespacedJob1));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namespacedJob2));
  }
}
