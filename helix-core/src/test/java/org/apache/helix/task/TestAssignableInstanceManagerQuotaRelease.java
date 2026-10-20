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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.helix.task.assigner.AssignableInstance;
import org.apache.helix.task.assigner.TaskAssignResult;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAssignableInstanceManagerQuotaRelease {
  private static final String INSTANCE = "Instance_0";

  private AssignableInstanceManager buildManager() {
    ClusterConfig clusterConfig = new ClusterConfig("TestCluster_0");
    Map<String, LiveInstance> liveInstances = new HashMap<>();
    Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
    LiveInstance liveInstance = new LiveInstance(INSTANCE);
    liveInstance.setCurrentTaskThreadPoolSize(10);
    liveInstances.put(INSTANCE, liveInstance);
    instanceConfigs.put(INSTANCE, new InstanceConfig(INSTANCE));

    AssignableInstanceManager manager = new AssignableInstanceManager();
    manager.buildAssignableInstances(clusterConfig, new TaskDataCache4Test("TestCluster_0"),
        liveInstances, instanceConfigs);
    return manager;
  }

  /**
   * A task with a quota type that is not configured is assigned as DEFAULT. Releasing it with the
   * original (undefined) quota type must return the global DEFAULT quota.
   */
  @Test
  public void testReleaseUndefinedQuotaTypeRestoresGlobalDefault() {
    AssignableInstanceManager manager = buildManager();
    int before = manager.getGlobalCapacityMap().get(AssignableInstance.DEFAULT_QUOTA_TYPE);

    TaskConfig taskConfig = new TaskConfig.Builder().setTaskId("Job_0_Task_0").build();
    TaskAssignResult result = manager.tryAssign(INSTANCE, taskConfig, "UndefinedType");
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(result.getQuotaType(), AssignableInstance.DEFAULT_QUOTA_TYPE);

    manager.assign(INSTANCE, result);
    Assert.assertEquals(
        (int) manager.getGlobalCapacityMap().get(AssignableInstance.DEFAULT_QUOTA_TYPE),
        before - 1);

    manager.release(INSTANCE, taskConfig, "UndefinedType");
    Assert.assertEquals(
        (int) manager.getGlobalCapacityMap().get(AssignableInstance.DEFAULT_QUOTA_TYPE), before);
  }

  /** Same as above for a null quota type. */
  @Test
  public void testReleaseNullQuotaTypeRestoresGlobalDefault() {
    AssignableInstanceManager manager = buildManager();
    int before = manager.getGlobalCapacityMap().get(AssignableInstance.DEFAULT_QUOTA_TYPE);

    TaskConfig taskConfig = new TaskConfig.Builder().setTaskId("Job_0_Task_1").build();
    TaskAssignResult result = manager.tryAssign(INSTANCE, taskConfig, null);
    Assert.assertTrue(result.isSuccessful());

    manager.assign(INSTANCE, result);
    manager.release(INSTANCE, taskConfig, null);
    Assert.assertEquals(
        (int) manager.getGlobalCapacityMap().get(AssignableInstance.DEFAULT_QUOTA_TYPE), before);
  }

  /**
   * A quota type removed from the cluster config while its task is still running must not cause
   * the global DEFAULT quota to be inflated when the task is released.
   */
  @Test
  public void testReleaseRemovedQuotaTypeDoesNotInflateDefault() {
    ClusterConfig withType = new ClusterConfig("TestCluster_0");
    withType.setTaskQuotaRatio(AssignableInstance.DEFAULT_QUOTA_TYPE, 1);
    withType.setTaskQuotaRatio("TypeX", 1);

    Map<String, LiveInstance> liveInstances = new HashMap<>();
    Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
    LiveInstance liveInstance = new LiveInstance(INSTANCE);
    liveInstance.setCurrentTaskThreadPoolSize(10);
    liveInstances.put(INSTANCE, liveInstance);
    instanceConfigs.put(INSTANCE, new InstanceConfig(INSTANCE));

    AssignableInstanceManager manager = new AssignableInstanceManager();
    manager.buildAssignableInstances(withType, new TaskDataCache4Test("TestCluster_0"),
        liveInstances, instanceConfigs);

    TaskConfig taskConfig = new TaskConfig.Builder().setTaskId("Job_0_Task_2").build();
    TaskAssignResult result = manager.tryAssign(INSTANCE, taskConfig, "TypeX");
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(result.getQuotaType(), "TypeX");
    manager.assign(INSTANCE, result);

    // Remove TypeX from the cluster config while the task is still running
    ClusterConfig withoutType = new ClusterConfig("TestCluster_0");
    withoutType.setTaskQuotaRatio(AssignableInstance.DEFAULT_QUOTA_TYPE, 1);
    manager.updateAssignableInstances(withoutType, liveInstances, instanceConfigs);
    int defaultBefore = manager.getGlobalCapacityMap().get(AssignableInstance.DEFAULT_QUOTA_TYPE);

    manager.release(INSTANCE, taskConfig, "TypeX");
    Assert.assertEquals(
        (int) manager.getGlobalCapacityMap().get(AssignableInstance.DEFAULT_QUOTA_TYPE),
        defaultBefore);
  }

  private static class TaskDataCache4Test extends org.apache.helix.common.caches.TaskDataCache {
    TaskDataCache4Test(String clusterName) {
      super(clusterName);
    }

    @Override
    public Map<String, JobConfig> getJobConfigMap() {
      return new HashMap<>();
    }

    @Override
    public Map<String, WorkflowConfig> getWorkflowConfigMap() {
      return new HashMap<>();
    }
  }
}
