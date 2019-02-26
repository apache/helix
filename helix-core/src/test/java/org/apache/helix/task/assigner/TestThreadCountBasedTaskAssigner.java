package org.apache.helix.task.assigner;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.TaskAssignmentCalculator;
import org.apache.helix.task.TaskConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestThreadCountBasedTaskAssigner extends AssignerTestBase {

  @Test
  public void testSuccessfulAssignment() {
    TaskAssigner assigner = new ThreadCountBasedTaskAssigner();
    int taskCountPerType = 150;
    int instanceCount = 20;
    int threadCount = 50;
    AssignableInstanceManager assignableInstanceManager =
        createAssignableInstanceManager(instanceCount, threadCount);

    for (String quotaType : testQuotaTypes) {
      // Create tasks
      List<TaskConfig> tasks = createTaskConfigs(taskCountPerType);

      // Assign
      Map<String, TaskAssignResult> results = assigner.assignTasks(assignableInstanceManager,
          assignableInstanceManager.getAssignableInstanceNames(), tasks, quotaType);

      // Check success
      assertAssignmentResults(results.values(), true);

      // Check evenness
      for (AssignableInstance instance : assignableInstanceManager.getAssignableInstanceMap()
          .values()) {
        int assignedCount = instance.getUsedCapacity()
            .get(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name()).get(quotaType);
        Assert.assertTrue(assignedCount <= taskCountPerType / instanceCount + 1
            && assignedCount >= taskCountPerType / instanceCount);
      }
    }
  }

  @Test
  public void testAssignmentFailureNoInstance() {
    TaskAssigner assigner = new ThreadCountBasedTaskAssigner();
    int taskCount = 10;
    List<TaskConfig> tasks = createTaskConfigs(taskCount);
    AssignableInstanceManager assignableInstanceManager = new AssignableInstanceManager();
    Map<String, TaskAssignResult> results = assigner.assignTasks(assignableInstanceManager,
        assignableInstanceManager.getAssignableInstanceNames(), tasks, "Dummy");
    Assert.assertEquals(results.size(), taskCount);
    for (TaskAssignResult result : results.values()) {
      Assert.assertFalse(result.isSuccessful());
      Assert.assertNull(result.getAssignableInstance());
      Assert.assertEquals(result.getFailureReason(),
          TaskAssignResult.FailureReason.INSUFFICIENT_QUOTA);
    }
  }

  @Test
  public void testAssignmentFailureNoTask() {
    TaskAssigner assigner = new ThreadCountBasedTaskAssigner();
    AssignableInstanceManager assignableInstanceManager = createAssignableInstanceManager(1, 10);
    Map<String, TaskAssignResult> results = assigner.assignTasks(assignableInstanceManager,
        assignableInstanceManager.getAssignableInstanceNames(),
        Collections.<TaskConfig> emptyList(), AssignableInstance.DEFAULT_QUOTA_TYPE);
    Assert.assertTrue(results.isEmpty());
  }

  @Test
  public void testAssignmentFailureInsufficientQuota() {
    TaskAssigner assigner = new ThreadCountBasedTaskAssigner();

    // 10 * Type1 quota
    AssignableInstanceManager assignableInstanceManager = createAssignableInstanceManager(2, 10);
    List<TaskConfig> tasks = createTaskConfigs(20);

    Map<String, TaskAssignResult> results = assigner.assignTasks(assignableInstanceManager,
        assignableInstanceManager.getAssignableInstanceNames(), tasks, testQuotaTypes[0]);
    int successCnt = 0;
    int failCnt = 0;
    for (TaskAssignResult rst : results.values()) {
      if (rst.isSuccessful()) {
        successCnt += 1;
      } else {
        failCnt += 1;
        Assert.assertEquals(rst.getFailureReason(),
            TaskAssignResult.FailureReason.INSUFFICIENT_QUOTA);
      }
    }
    Assert.assertEquals(successCnt, 10);
    Assert.assertEquals(failCnt, 10);
  }

  @Test
  public void testAssignmentFailureDuplicatedTask() {
    TaskAssigner assigner = new ThreadCountBasedTaskAssigner();
    AssignableInstanceManager assignableInstanceManager = createAssignableInstanceManager(1, 20);
    List<TaskConfig> tasks = createTaskConfigs(10, false);

    // Duplicate all tasks
    tasks.addAll(createTaskConfigs(10, false));
    Collections.shuffle(tasks);

    Map<String, TaskAssignResult> results = assigner.assignTasks(assignableInstanceManager,
        assignableInstanceManager.getAssignableInstanceNames(), tasks, testQuotaTypes[0]);
    Assert.assertEquals(results.size(), 10);
    assertAssignmentResults(results.values(), true);
  }

  @Test(enabled = false, description = "Not enabling profiling tests")
  public void testAssignerProfiling() {
    int instanceCount = 1000;
    int taskCount = 50000;
    for (int batchSize : new int[] {10000, 5000, 2000, 1000, 500, 100}) {
      System.out.println("testing batch size: " + batchSize);
      profileAssigner(batchSize, instanceCount, taskCount);
    }
  }

  @Test
  public void testAssignmentToGivenInstances() {
    int totalNumberOfInstances = 10;
    int eligibleNumberOfInstances = 5;
    String instanceNameFormat = "instance-%s";

    TaskAssigner assigner = new ThreadCountBasedTaskAssigner();
    AssignableInstanceManager assignableInstanceManager = createAssignableInstanceManager(10, 20);
    List<TaskConfig> tasks = createTaskConfigs(100, false);
    Set<String> eligibleInstances = new HashSet<>();

    // Add only eligible number of instances
    for (int i = 0; i < eligibleNumberOfInstances; i++) {
      eligibleInstances.add(String.format(instanceNameFormat, i));
    }

    Map<String, TaskAssignResult> result = assigner.assignTasks(assignableInstanceManager,
        eligibleInstances, tasks, testQuotaTypes[0]);

    for (int i = 0; i < totalNumberOfInstances; i++) {
      String instance = String.format(instanceNameFormat, i);
      Set<String> test = assignableInstanceManager.getAssignableInstance(instance)
          .getCurrentAssignments();
      boolean isAssignmentEmpty = assignableInstanceManager.getAssignableInstance(instance)
          .getCurrentAssignments().isEmpty();
      // Check that assignment only took place to eligible number of instances and that assignment
      // did not happen to non-eligible AssignableInstances
      if (i < eligibleNumberOfInstances) {
        // Must have tasks assigned to these instances
        Assert.assertFalse(isAssignmentEmpty);
      } else {
        // These instances should have no tasks assigned to them
        Assert.assertTrue(isAssignmentEmpty);
      }
    }
  }

  private void profileAssigner(int assignBatchSize, int instanceCount, int taskCount) {
    int trail = 100;
    long totalTime = 0;
    for (int i = 0; i < trail; i++) {
      TaskAssigner assigner = new ThreadCountBasedTaskAssigner();

      // 50 * instanceCount number of tasks
      AssignableInstanceManager assignableInstanceManager =
          createAssignableInstanceManager(instanceCount, 100);
      List<TaskConfig> tasks = createTaskConfigs(taskCount);
      List<Map<String, TaskAssignResult>> allResults = new ArrayList<>();

      // Assign
      long start = System.currentTimeMillis();
      for (int j = 0; j < taskCount / assignBatchSize; j++) {
        allResults.add(assigner.assignTasks(assignableInstanceManager,
            assignableInstanceManager.getAssignableInstanceNames(),
            tasks.subList(j * assignBatchSize, (j + 1) * assignBatchSize), testQuotaTypes[0]));
      }
      long duration = System.currentTimeMillis() - start;
      totalTime += duration;

      // Validate
      for (Map<String, TaskAssignResult> results : allResults) {
        for (TaskAssignResult rst : results.values()) {
          Assert.assertTrue(rst.isSuccessful());
        }
      }
    }
    System.out.println("Average time: " + totalTime / trail + "ms");
  }

  private void assertAssignmentResults(Iterable<TaskAssignResult> results, boolean expected) {
    for (TaskAssignResult rst : results) {
      Assert.assertEquals(rst.isSuccessful(), expected);
    }
  }

  private List<TaskConfig> createTaskConfigs(int count) {
    return createTaskConfigs(count, true);
  }

  private List<TaskConfig> createTaskConfigs(int count, boolean randomID) {
    List<TaskConfig> tasks = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      TaskConfig task =
          new TaskConfig(null, null, randomID ? UUID.randomUUID().toString() : "task-" + i, null);
      tasks.add(task);
    }
    return tasks;
  }

  private AssignableInstanceManager createAssignableInstanceManager(int count, int threadCount) {
    AssignableInstanceManager assignableInstanceManager = new AssignableInstanceManager();
    ClusterConfig clusterConfig = createClusterConfig(testQuotaTypes, testQuotaRatio, false);
    String instanceNameFormat = "instance-%s";
    Map<String, LiveInstance> liveInstanceMap = new HashMap<>();
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    for (int i = 0; i < count; i++) {
      String instanceName = String.format(instanceNameFormat, i);
      liveInstanceMap.put(instanceName, createLiveInstance(
          new String[] { LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name() },
          new String[] { Integer.toString(threadCount) }, instanceName));
      instanceConfigMap.put(instanceName, new InstanceConfig(instanceName));
    }

    assignableInstanceManager
        .buildAssignableInstances(clusterConfig, new TaskDataCache(testClusterName),
            liveInstanceMap, instanceConfigMap);
    return assignableInstanceManager;
  }
}
