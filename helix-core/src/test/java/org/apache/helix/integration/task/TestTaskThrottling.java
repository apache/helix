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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskThrottling extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    _numNodes = 2;
    super.beforeClass();
  }

  /**
   * This test has been disabled/deprecated because Task Framework 2.0 uses quotas that are meant to
   * throttle tasks.
   * @throws InterruptedException
   */
  @Test
  public void testTaskThrottle() throws InterruptedException {
    int numTasks = 30 * _numNodes; // 60 tasks
    int perNodeTaskLimitation = 5;

    JobConfig.Builder jobConfig = generateLongRunJobConfig(numTasks);

    // 1. Job executed in the participants with no limitation
    String jobName1 = "Job1";
    Workflow flow = WorkflowGenerator.generateSingleJobWorkflowBuilder(jobName1, jobConfig).build();
    _driver.start(flow);
    _driver.pollForJobState(flow.getName(), TaskUtil.getNamespacedJobName(flow.getName(), jobName1),
        TaskState.IN_PROGRESS);
    // Wait for tasks to be picked up
    Thread.sleep(1000L);

    Assert.assertEquals(countRunningPartition(flow, jobName1), numTasks);

    _driver.stop(flow.getName());
    _driver.pollForWorkflowState(flow.getName(), TaskState.STOPPED);

    // 2. Job executed in the participants with max task limitation

    // Configuring cluster
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
            .forCluster(CLUSTER_NAME).build();
    Map<String, String> properties = new HashMap<>();
    properties.put(ClusterConfig.ClusterConfigProperty.MAX_CONCURRENT_TASK_PER_INSTANCE.name(),
        Integer.toString(perNodeTaskLimitation));
    _gSetupTool.getClusterManagementTool().setConfig(scope, properties);

    String jobName2 = "Job2";
    flow = WorkflowGenerator.generateSingleJobWorkflowBuilder(jobName2, jobConfig).build();
    _driver.start(flow);
    _driver.pollForJobState(flow.getName(), TaskUtil.getNamespacedJobName(flow.getName(), jobName2),
        TaskState.IN_PROGRESS);
    // Wait for tasks to be picked up
    Thread.sleep(1000L);

    // Expect 10 tasks
    Assert.assertEquals(countRunningPartition(flow, jobName2), _numNodes * perNodeTaskLimitation);

    _driver.stop(flow.getName());
    _driver.pollForWorkflowState(flow.getName(), TaskState.STOPPED);

    // 3. Ensure job can finish normally
    jobConfig.setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "10"));
    String jobName3 = "Job3";
    flow = WorkflowGenerator.generateSingleJobWorkflowBuilder(jobName3, jobConfig).build();
    _driver.start(flow);
    _driver.pollForJobState(flow.getName(), TaskUtil.getNamespacedJobName(flow.getName(), jobName3),
        TaskState.COMPLETED);
  }

  // Disable this test since helix will have priority map when integrate with JobIterator.
  @Test(dependsOnMethods = {
      "testTaskThrottle"
  }, enabled = false)
  public void testJobPriority() throws InterruptedException {
    int numTasks = 30 * _numNodes;
    int perNodeTaskLimitation = 5;

    JobConfig.Builder jobConfig = generateLongRunJobConfig(numTasks);

    // Configuring participants
    setParticipantsCapacity(perNodeTaskLimitation);

    // schedule job1
    String jobName1 = "PriorityJob1";
    Workflow flow1 =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobName1, jobConfig).build();
    _driver.start(flow1);
    _driver.pollForJobState(flow1.getName(),
        TaskUtil.getNamespacedJobName(flow1.getName(), jobName1), TaskState.IN_PROGRESS);
    // Wait for tasks to be picked up
    Thread.sleep(1000L);
    Assert.assertEquals(countRunningPartition(flow1, jobName1), _numNodes * perNodeTaskLimitation);

    // schedule job2
    String jobName2 = "PriorityJob2";
    Workflow flow2 =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobName2, jobConfig).build();
    _driver.start(flow2);
    _driver.pollForJobState(flow2.getName(),
        TaskUtil.getNamespacedJobName(flow2.getName(), jobName2), TaskState.IN_PROGRESS);
    // Wait for tasks to be picked up
    Thread.sleep(1500);
    Assert.assertEquals(countRunningPartition(flow2, jobName2), 0);

    // Increasing participants capacity
    perNodeTaskLimitation = 2 * perNodeTaskLimitation;
    setParticipantsCapacity(perNodeTaskLimitation);

    Thread.sleep(1500);
    // Additional capacity should all be used by job1
    Assert.assertEquals(countRunningPartition(flow1, jobName1), _numNodes * perNodeTaskLimitation);
    Assert.assertEquals(countRunningPartition(flow2, jobName2), 0);

    _driver.stop(flow1.getName());
    _driver.pollForWorkflowState(flow1.getName(), TaskState.STOPPED);
    _driver.stop(flow2.getName());
    _driver.pollForWorkflowState(flow2.getName(), TaskState.STOPPED);
  }

  private int countRunningPartition(Workflow flow, String jobName) {
    int runningPartition = 0;
    JobContext jobContext =
        _driver.getJobContext(TaskUtil.getNamespacedJobName(flow.getName(), jobName));
    for (int partition : jobContext.getPartitionSet()) {
      if (jobContext.getPartitionState(partition) != null
          && jobContext.getPartitionState(partition).equals(TaskPartitionState.RUNNING)) {
        runningPartition++;
      }
    }
    return runningPartition;
  }

  private JobConfig.Builder generateLongRunJobConfig(int numTasks) {
    JobConfig.Builder jobConfig = new JobConfig.Builder();
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int j = 0; j < numTasks; j++) {
      taskConfigs.add(new TaskConfig.Builder().setTaskId("task_" + j)
          .setCommand(MockTask.TASK_COMMAND).build());
    }
    jobConfig.addTaskConfigs(taskConfigs).setNumConcurrentTasksPerInstance(numTasks)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "120000"));
    return jobConfig;
  }

  private void setParticipantsCapacity(int perNodeTaskLimitation) {
    for (int i = 0; i < _numNodes; i++) {
      InstanceConfig instanceConfig = _gSetupTool.getClusterManagementTool()
          .getInstanceConfig(CLUSTER_NAME, PARTICIPANT_PREFIX + "_" + (_startPort + i));
      instanceConfig.setMaxConcurrentTask(perNodeTaskLimitation);
      _gSetupTool.getClusterManagementTool().setInstanceConfig(CLUSTER_NAME,
          PARTICIPANT_PREFIX + "_" + (_startPort + i), instanceConfig);
    }
  }
}
