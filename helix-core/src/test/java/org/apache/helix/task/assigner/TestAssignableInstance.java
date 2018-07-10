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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskStateModelFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;


public class TestAssignableInstance extends AssignerTestBase {

  @Test
  public void testInvalidInitialization() {
    try {
      AssignableInstance ai = new AssignableInstance(null, null, null);
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("cannot be null"));
    }

    try {
      ClusterConfig clusterConfig = new ClusterConfig("testCluster");
      InstanceConfig instanceConfig = new InstanceConfig("instance");
      LiveInstance liveInstance = new LiveInstance("another-instance");
      AssignableInstance ai = new AssignableInstance(clusterConfig, instanceConfig, liveInstance);
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("don't match"));
    }
  }

  @Test
  public void testInitializationWithQuotaUnset() {
    // Initialize AssignableInstance with neither resource capacity nor quota ratio provided
    AssignableInstance ai = new AssignableInstance(
        createClusterConfig(null, null, false),
        new InstanceConfig(testInstanceName),
        createLiveInstance(null, null)
    );
    Assert.assertEquals(ai.getUsedCapacity().size(), 1);
    Assert.assertEquals(
        (int) ai.getUsedCapacity().get(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name())
            .get(TaskConfig.DEFAULT_QUOTA_TYPE), 0);
    Assert.assertEquals(
        (int) ai.getTotalCapacity().get(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name())
            .get(TaskConfig.DEFAULT_QUOTA_TYPE), TaskStateModelFactory.TASK_THREADPOOL_SIZE);
    Assert.assertEquals(ai.getCurrentAssignments().size(), 0);
  }

  @Test
  public void testInitializationWithOnlyCapacity() {
    // Initialize AssignableInstance with only resource capacity provided
    AssignableInstance ai = new AssignableInstance(
        createClusterConfig(null, null, false),
        new InstanceConfig(testInstanceName),
        createLiveInstance(testResourceTypes, testResourceCapacity)
    );
    Assert.assertEquals(ai.getTotalCapacity().size(), testResourceTypes.length);
    Assert.assertEquals(ai.getUsedCapacity().size(), testResourceTypes.length);
    for (int i = 0; i < testResourceTypes.length; i++) {
      Assert.assertEquals(ai.getTotalCapacity().get(testResourceTypes[i]).size(), 1);
      Assert.assertEquals(ai.getUsedCapacity().get(testResourceTypes[i]).size(), 1);
      Assert.assertEquals(
          ai.getTotalCapacity().get(testResourceTypes[i]).get(TaskConfig.DEFAULT_QUOTA_TYPE),
          Integer.valueOf(testResourceCapacity[i])
      );
      Assert.assertEquals(
          ai.getUsedCapacity().get(testResourceTypes[i]).get(TaskConfig.DEFAULT_QUOTA_TYPE),
          Integer.valueOf(0)
      );
    }
  }

  @Test
  public void testInitializationWithOnlyQuotaType() {
    // Initialize AssignableInstance with only quota type provided
    AssignableInstance ai = new AssignableInstance(
        createClusterConfig(testQuotaTypes, testQuotaRatio, false),
        new InstanceConfig(testInstanceName),
        createLiveInstance(null, null)
    );

    Assert.assertEquals(ai.getTotalCapacity().size(), 1);
    Assert.assertEquals(ai.getUsedCapacity().size(), 1);
    Assert.assertEquals(
        ai.getTotalCapacity().get(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name()).size(),
        testQuotaTypes.length
    );
    Assert.assertEquals(
        ai.getUsedCapacity().get(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name()).size(),
        testQuotaTypes.length
    );
    Assert.assertEquals(
        ai.getTotalCapacity().get(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name()),
        calculateExpectedQuotaPerType(TaskStateModelFactory.TASK_THREADPOOL_SIZE, testQuotaTypes,
            testQuotaRatio));
    Assert.assertEquals(ai.getCurrentAssignments().size(), 0);
  }

  @Test
  public void testInitializationWithQuotaAndCapacity() {
    // Initialize AssignableInstance with both capacity and quota type provided
    AssignableInstance ai = new AssignableInstance(
        createClusterConfig(testQuotaTypes, testQuotaRatio, false),
        new InstanceConfig(testInstanceName),
        createLiveInstance(testResourceTypes, testResourceCapacity)
    );

    Map<String, Integer> usedResourcePerType =
        createResourceQuotaPerTypeMap(testQuotaTypes, new int[] { 0, 0, 0 });
    for (int i = 0; i < testResourceTypes.length; i++) {
      Assert.assertEquals(ai.getTotalCapacity().get(testResourceTypes[i]),
          calculateExpectedQuotaPerType(Integer.valueOf(testResourceCapacity[i]), testQuotaTypes,
              testQuotaRatio));
      Assert.assertEquals(ai.getUsedCapacity().get(testResourceTypes[i]), usedResourcePerType);
    }
  }

  @Test
  public void testAssignableInstanceUpdateConfigs() {
    AssignableInstance ai = new AssignableInstance(
        createClusterConfig(testQuotaTypes, testQuotaRatio, false),
        new InstanceConfig(testInstanceName),
        createLiveInstance(testResourceTypes, testResourceCapacity)
    );

    String[] newResources = new String[] {"Resource2", "Resource3", "Resource4"};
    String[] newResourceCapacities = new String[] {"100", "150", "50"};

    String[] newTypes = new String[] {"Type3", "Type4", "Type5", "Type6"};
    String[] newTypeRatio = new String[] {"20", "40", "25", "25"};

    LiveInstance newLiveInstance = createLiveInstance(newResources, newResourceCapacities);
    ClusterConfig newClusterConfig = createClusterConfig(newTypes, newTypeRatio, false);
    ai.updateConfigs(newClusterConfig, null, newLiveInstance);

    Assert.assertEquals(ai.getUsedCapacity().size(), newResourceCapacities.length);
    Assert.assertEquals(ai.getTotalCapacity().size(), newResourceCapacities.length);

    for (int i = 0; i < newResources.length; i++) {
      Assert.assertEquals(ai.getTotalCapacity().get(newResources[i]),
          calculateExpectedQuotaPerType(Integer.valueOf(newResourceCapacities[i]), newTypes,
              newTypeRatio));
      Assert.assertEquals(ai.getUsedCapacity().get(newResources[i]),
          createResourceQuotaPerTypeMap(newTypes, new int[] { 0, 0, 0, 0 }));
    }
  }

  @Test
  public void testNormalTryAssign() {
    AssignableInstance ai = new AssignableInstance(
        createClusterConfig(null, null, true),
        new InstanceConfig(testInstanceName),
        createLiveInstance(null, null)
    );

    // When nothing is configured, we should use default quota to assign
    Map<String, TaskAssignResult> results = new HashMap<>();
    for (int i = 0; i < TaskStateModelFactory.TASK_THREADPOOL_SIZE; i++) {
      String taskId = Integer.toString(i);
      TaskConfig task = new TaskConfig("", null, taskId, null);
      TaskAssignResult result = ai.tryAssign(task);
      Assert.assertTrue(result.isSuccessful());
      ai.assign(result);
      results.put(taskId, result);
    }

    // We are out of quota now and we should not be able to assign
    String taskId = "TaskCannotAssign";
    TaskConfig task = new TaskConfig("", null, taskId, null);
    TaskAssignResult result = ai.tryAssign(task);
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(result.getFailureReason(), TaskAssignResult.FailureReason.INSUFFICIENT_QUOTA);
    try {
      ai.assign(result);
      Assert.fail("Expecting IllegalStateException");
    } catch (IllegalStateException e) {
      // OK
    }

    // After releasing 1 task, we should be able to schedule
    ai.release(results.get("1").getTaskConfig());
    result = ai.tryAssign(task);
    Assert.assertTrue(result.isSuccessful());

    // release all tasks, check remaining resources
    for (TaskAssignResult rst : results.values()) {
      ai.release(rst.getTaskConfig());
    }

    Assert.assertEquals(
        (int) ai.getUsedCapacity().get(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name())
            .get(TaskConfig.DEFAULT_QUOTA_TYPE), 0);
  }

  @Test
  public void testTryAssignFailure() {
    AssignableInstance ai =
        new AssignableInstance(createClusterConfig(testQuotaTypes, testQuotaRatio, false),
            new InstanceConfig(testInstanceName),
            createLiveInstance(testResourceTypes, testResourceCapacity));

    // No such resource type
    String taskId = "testTask";
    TaskConfig task = new TaskConfig("", null, taskId, "");
    TaskAssignResult result = ai.tryAssign(task);
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(result.getFailureReason(),
        TaskAssignResult.FailureReason.NO_SUCH_RESOURCE_TYPE);

    // No such quota type
    ai.updateConfigs(null, null, createLiveInstance(
        new String[] { LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name() },
        new String[] { "1" }));

    result = ai.tryAssign(task);
    Assert.assertFalse(result.isSuccessful());
    Assert
        .assertEquals(result.getFailureReason(), TaskAssignResult.FailureReason.NO_SUCH_QUOTA_TYPE);

    ai.updateConfigs(createClusterConfig(testQuotaTypes, testQuotaRatio, true), null, null);

    task.setQuotaType(TaskConfig.DEFAULT_QUOTA_TYPE);
    result = ai.tryAssign(task);
    Assert.assertTrue(result.isSuccessful());
    ai.assign(result);
    try {
      ai.assign(result);
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalStateException e) {
      // OK
    }

    // Duplicate assignment
    result = ai.tryAssign(task);
    Assert.assertFalse(result.isSuccessful());
    Assert.assertEquals(result.getFailureReason(), TaskAssignResult.FailureReason.TASK_ALREADY_ASSIGNED);

    // Insufficient quota
    ai.release(task);
    ai.updateConfigs(null, null, createLiveInstance(
        new String[] { LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name() },
        new String[] { "0" }));

    task.setQuotaType(TaskConfig.DEFAULT_QUOTA_TYPE);
    result = ai.tryAssign(task);
    Assert.assertFalse(result.isSuccessful());
    Assert
        .assertEquals(result.getFailureReason(), TaskAssignResult.FailureReason.INSUFFICIENT_QUOTA);
  }

  @Test
  public void testSetCurrentAssignment() {
    AssignableInstance ai =
        new AssignableInstance(createClusterConfig(testQuotaTypes, testQuotaRatio, true),
            new InstanceConfig(testInstanceName), createLiveInstance(
            new String[] { LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name() },
            new String[] { "40" }));

    Map<String, TaskConfig> currentAssignments = new HashMap<>();
    currentAssignments.put("supportedTask", new TaskConfig("", null, "supportedTask", ""));
    TaskConfig unsupportedTask = new TaskConfig("", null, "unsupportedTask", "");
    unsupportedTask.setQuotaType("UnsupportedQuotaType");
    currentAssignments.put("unsupportedTask", unsupportedTask);

    Map<String, TaskAssignResult> results = Maps.newHashMap();
    for (Map.Entry<String, TaskConfig> entry : currentAssignments.entrySet()) {
      String taskID = entry.getKey();
      TaskConfig taskConfig = entry.getValue();
      TaskAssignResult taskAssignResult = ai.restoreTaskAssignResult(taskID, taskConfig);
      if (taskAssignResult.isSuccessful()) {
        results.put(taskID, taskAssignResult);
      }
    }

    for (TaskAssignResult rst : results.values()) {
      Assert.assertTrue(rst.isSuccessful());
      Assert.assertEquals(rst.getAssignableInstance(), ai);
    }
    Assert.assertEquals(ai.getCurrentAssignments().size(), 2);
    Assert.assertEquals(
        (int) ai.getUsedCapacity().get(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name())
            .get(TaskConfig.DEFAULT_QUOTA_TYPE), 1);
  }

  private Map<String, Integer> createResourceQuotaPerTypeMap(String[] types, int[] quotas) {
    Map<String, Integer> ret = new HashMap<>();
    for (int i = 0; i < types.length; i++) {
      ret.put(types[i], quotas[i]);
    }
    return ret;
  }

  private Map<String, Integer> calculateExpectedQuotaPerType(int capacity, String[] quotaTypes,
      String[] quotaRatios) {
    Integer totalQuota = 0;
    Map<String, Integer> expectedQuotaPerType = new HashMap<>();

    for (String ratio : quotaRatios) {
      totalQuota += Integer.valueOf(ratio);
    }

    for (int i = 0; i < quotaRatios.length; i++) {
      expectedQuotaPerType.put(quotaTypes[i],
          Math.round((float)capacity * Integer.valueOf(quotaRatios[i])
              / totalQuota));
    }
    return expectedQuotaPerType;
  }
}