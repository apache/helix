package org.apache.helix.task;

import java.util.Collections;

import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTaskStateModelFactory extends TaskTestBase {
  private static final int TEST_TARGET_TASK_THREAD_POOL_SIZE = 50;

  @Test
  public void testGetTaskThreadPoolSizeWithInstanceConfig() {
    MockParticipantManager anyParticipantManager = _participants[0];

    InstanceConfig instanceConfig =
        InstanceConfig.toInstanceConfig(anyParticipantManager.getInstanceName());
    instanceConfig.setTargetTaskThreadPoolSize(TEST_TARGET_TASK_THREAD_POOL_SIZE);
    anyParticipantManager.getConfigAccessor()
        .setInstanceConfig(anyParticipantManager.getClusterName(),
            anyParticipantManager.getInstanceName(), instanceConfig);

    ClusterConfig clusterConfig = new ClusterConfig(anyParticipantManager.getClusterName());
    clusterConfig.setDefaultTargetTaskThreadPoolSize(TEST_TARGET_TASK_THREAD_POOL_SIZE + 1);
    anyParticipantManager.getConfigAccessor()
        .setClusterConfig(anyParticipantManager.getClusterName(), clusterConfig);

    TaskStateModelFactory taskStateModelFactory =
        new TaskStateModelFactory(anyParticipantManager, Collections.emptyMap());
    Assert.assertEquals(taskStateModelFactory.getTaskThreadPoolSize(),
        TEST_TARGET_TASK_THREAD_POOL_SIZE);
  }

  @Test(dependsOnMethods = "testGetTaskThreadPoolSizeWithInstanceConfig")
  public void testGetTaskThreadPoolSizeInstanceConfigUndefined() {
    MockParticipantManager anyParticipantManager = _participants[0];

    InstanceConfig instanceConfig =
        InstanceConfig.toInstanceConfig(anyParticipantManager.getInstanceName());
    anyParticipantManager.getConfigAccessor()
        .setInstanceConfig(anyParticipantManager.getClusterName(),
            anyParticipantManager.getInstanceName(), instanceConfig);

    ClusterConfig clusterConfig = new ClusterConfig(anyParticipantManager.getClusterName());
    clusterConfig.setDefaultTargetTaskThreadPoolSize(TEST_TARGET_TASK_THREAD_POOL_SIZE);
    anyParticipantManager.getConfigAccessor()
        .setClusterConfig(anyParticipantManager.getClusterName(), clusterConfig);

    TaskStateModelFactory taskStateModelFactory =
        new TaskStateModelFactory(anyParticipantManager, Collections.emptyMap());
    Assert.assertEquals(taskStateModelFactory.getTaskThreadPoolSize(),
        TEST_TARGET_TASK_THREAD_POOL_SIZE);
  }

  @Test(dependsOnMethods = "testGetTaskThreadPoolSizeInstanceConfigUndefined")
  public void testGetTaskThreadPoolSizeClusterConfigUndefined() {
    MockParticipantManager anyParticipantManager = _participants[0];

    InstanceConfig instanceConfig =
        InstanceConfig.toInstanceConfig(anyParticipantManager.getInstanceName());
    anyParticipantManager.getConfigAccessor()
        .setInstanceConfig(anyParticipantManager.getClusterName(),
            anyParticipantManager.getInstanceName(), instanceConfig);

    ClusterConfig clusterConfig = new ClusterConfig(anyParticipantManager.getClusterName());
    anyParticipantManager.getConfigAccessor()
        .setClusterConfig(anyParticipantManager.getClusterName(), clusterConfig);

    TaskStateModelFactory taskStateModelFactory =
        new TaskStateModelFactory(anyParticipantManager, Collections.emptyMap());
    Assert.assertEquals(taskStateModelFactory.getTaskThreadPoolSize(),
        TaskConstants.DEFAULT_TASK_THREAD_POOL_SIZE);
  }
}
