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
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.task.assigner.AssignableInstance;
import org.apache.helix.task.assigner.TaskAssignResult;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAssignableInstanceManagerControllerSwitch extends TaskTestBase {
  private int numJobs = 2;
  private int numTasks = 3;

  @Test
  public void testControllerSwitch() throws InterruptedException {
    setupAndRunJobs();

    Map<String, LiveInstance> liveInstanceMap = new HashMap<>();
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();

    RoutingTableProvider routingTableProvider = new RoutingTableProvider(_manager);
    Collection<LiveInstance> liveInstances = routingTableProvider.getLiveInstances();
    for (LiveInstance liveInstance : liveInstances) {
      String instanceName = liveInstance.getInstanceName();
      liveInstanceMap.put(instanceName, liveInstance);
      instanceConfigMap.put(instanceName,
          _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instanceName));
    }

    // Get ClusterConfig
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);

    // Initialize TaskDataCache
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    TaskDataCache taskDataCache = new TaskDataCache(CLUSTER_NAME);
    Map<String, ResourceConfig> resourceConfigMap =
        accessor.getChildValuesMap(accessor.keyBuilder().resourceConfigs(), true);

    // Wait for the job pipeline
    Thread.sleep(100);
    taskDataCache.refresh(accessor, resourceConfigMap);

    // Create prev manager and build
    AssignableInstanceManager prevAssignableInstanceManager = new AssignableInstanceManager();
    prevAssignableInstanceManager.buildAssignableInstances(clusterConfig, taskDataCache,
        liveInstanceMap, instanceConfigMap);
    Map<String, AssignableInstance> prevAssignableInstanceMap =
        new HashMap<>(prevAssignableInstanceManager.getAssignableInstanceMap());
    Map<String, TaskAssignResult> prevTaskAssignResultMap =
        new HashMap<>(prevAssignableInstanceManager.getTaskAssignResultMap());

    // Stop the current controller
    _controller.syncStop();
    // Start a new controller
    String newControllerName = CONTROLLER_PREFIX + "_2";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, newControllerName);
    _controller.syncStart();

    // Generate a new AssignableInstanceManager
    taskDataCache.refresh(accessor, resourceConfigMap);
    AssignableInstanceManager newAssignableInstanceManager = new AssignableInstanceManager();
    newAssignableInstanceManager.buildAssignableInstances(clusterConfig, taskDataCache,
        liveInstanceMap, instanceConfigMap);
    Map<String, AssignableInstance> newAssignableInstanceMap =
        new HashMap<>(newAssignableInstanceManager.getAssignableInstanceMap());
    Map<String, TaskAssignResult> newTaskAssignResultMap =
        new HashMap<>(newAssignableInstanceManager.getTaskAssignResultMap());

    // Compare prev and new - they should match up exactly
    Assert.assertEquals(prevAssignableInstanceMap.size(), newAssignableInstanceMap.size());
    Assert.assertEquals(prevTaskAssignResultMap.size(), newTaskAssignResultMap.size());
    for (Map.Entry<String, AssignableInstance> assignableInstanceEntry : newAssignableInstanceMap
        .entrySet()) {
      String instance = assignableInstanceEntry.getKey();
      Assert.assertEquals(prevAssignableInstanceMap.get(instance).getCurrentAssignments(),
          assignableInstanceEntry.getValue().getCurrentAssignments());
      Assert.assertEquals(prevAssignableInstanceMap.get(instance).getTotalCapacity(),
          assignableInstanceEntry.getValue().getTotalCapacity());
      Assert.assertEquals(prevAssignableInstanceMap.get(instance).getUsedCapacity(),
          assignableInstanceEntry.getValue().getUsedCapacity());
    }
    for (Map.Entry<String, TaskAssignResult> taskAssignResultEntry : newTaskAssignResultMap
        .entrySet()) {
      String taskID = taskAssignResultEntry.getKey();
      Assert.assertEquals(prevTaskAssignResultMap.get(taskID).toString(),
          taskAssignResultEntry.getValue().toString());
    }
  }

  private void setupAndRunJobs() {
    // Create a workflow with some long-running jobs in progress
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    for (int i = 0; i < numJobs; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      for (int j = 0; j < numTasks; j++) {
        String taskID = "JOB_" + i + "_TASK_" + j;
        TaskConfig.Builder taskConfigBuilder = new TaskConfig.Builder();
        taskConfigBuilder.setTaskId(taskID).setCommand(MockTask.TASK_COMMAND)
            .addConfig(MockTask.JOB_DELAY, "120000");
        taskConfigs.add(taskConfigBuilder.build());
      }
      String jobName = "JOB_" + i;
      JobConfig.Builder jobBuilder =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(10000)
              .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG)
              .addTaskConfigs(taskConfigs).setIgnoreDependentJobFailure(true)
              .setFailureThreshold(100000)
              .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "120000")); // Long-running
      // job
      builder.addJob(jobName, jobBuilder);
    }
    // Start the workflow
    _driver.start(builder.build());
  }
}