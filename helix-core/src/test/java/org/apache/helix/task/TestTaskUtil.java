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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTaskUtil extends TaskTestBase {
  // This value has to be different from the default value to verify correctness
  private static final int TEST_TARGET_TASK_THREAD_POOL_SIZE =
      TaskConstants.DEFAULT_TASK_THREAD_POOL_SIZE + 1;

  @Test
  public void testGetTaskThreadPoolSize() {
    MockParticipantManager anyParticipantManager = _participants[0];

    InstanceConfig instanceConfig =
        InstanceConfig.toInstanceConfig(anyParticipantManager.getInstanceName());
    instanceConfig.setTargetTaskThreadPoolSize(TEST_TARGET_TASK_THREAD_POOL_SIZE);
    anyParticipantManager.getConfigAccessor()
        .setInstanceConfig(anyParticipantManager.getClusterName(),
            anyParticipantManager.getInstanceName(), instanceConfig);

    ClusterConfig clusterConfig = new ClusterConfig(anyParticipantManager.getClusterName());
    clusterConfig.setGlobalTargetTaskThreadPoolSize(TEST_TARGET_TASK_THREAD_POOL_SIZE + 1);
    anyParticipantManager.getConfigAccessor()
        .setClusterConfig(anyParticipantManager.getClusterName(), clusterConfig);

    Assert.assertEquals(TaskUtil.getTargetThreadPoolSize(anyParticipantManager.getZkClient(),
        anyParticipantManager.getClusterName(), anyParticipantManager.getInstanceName()),
        TEST_TARGET_TASK_THREAD_POOL_SIZE);
  }

  @Test(dependsOnMethods = "testGetTaskThreadPoolSize")
  public void testGetTaskThreadPoolSizeInstanceConfigUndefined() {
    MockParticipantManager anyParticipantManager = _participants[0];

    InstanceConfig instanceConfig =
        InstanceConfig.toInstanceConfig(anyParticipantManager.getInstanceName());
    anyParticipantManager.getConfigAccessor()
        .setInstanceConfig(anyParticipantManager.getClusterName(),
            anyParticipantManager.getInstanceName(), instanceConfig);

    ClusterConfig clusterConfig = new ClusterConfig(anyParticipantManager.getClusterName());
    clusterConfig.setGlobalTargetTaskThreadPoolSize(TEST_TARGET_TASK_THREAD_POOL_SIZE);
    anyParticipantManager.getConfigAccessor()
        .setClusterConfig(anyParticipantManager.getClusterName(), clusterConfig);

    Assert.assertEquals(TaskUtil.getTargetThreadPoolSize(anyParticipantManager.getZkClient(),
        anyParticipantManager.getClusterName(), anyParticipantManager.getInstanceName()),
        TEST_TARGET_TASK_THREAD_POOL_SIZE);
  }

  @Test(dependsOnMethods = "testGetTaskThreadPoolSizeInstanceConfigUndefined")
  public void testGetTaskThreadPoolSizeInstanceConfigDoesNotExist() {
    MockParticipantManager anyParticipantManager = _participants[0];

    HelixDataAccessor helixDataAccessor = anyParticipantManager.getHelixDataAccessor();
    helixDataAccessor.removeProperty(
        helixDataAccessor.keyBuilder().instanceConfig(anyParticipantManager.getInstanceName()));

    ClusterConfig clusterConfig = new ClusterConfig(anyParticipantManager.getClusterName());
    clusterConfig.setGlobalTargetTaskThreadPoolSize(TEST_TARGET_TASK_THREAD_POOL_SIZE);
    anyParticipantManager.getConfigAccessor()
        .setClusterConfig(anyParticipantManager.getClusterName(), clusterConfig);

    Assert.assertEquals(TaskUtil.getTargetThreadPoolSize(anyParticipantManager.getZkClient(),
        anyParticipantManager.getClusterName(), anyParticipantManager.getInstanceName()),
        TEST_TARGET_TASK_THREAD_POOL_SIZE);
  }

  @Test(dependsOnMethods = "testGetTaskThreadPoolSizeInstanceConfigDoesNotExist")
  public void testGetTaskThreadPoolSizeClusterConfigUndefined() {
    MockParticipantManager anyParticipantManager = _participants[0];

    ClusterConfig clusterConfig = new ClusterConfig(anyParticipantManager.getClusterName());
    anyParticipantManager.getConfigAccessor()
        .setClusterConfig(anyParticipantManager.getClusterName(), clusterConfig);

    Assert.assertEquals(TaskUtil.getTargetThreadPoolSize(anyParticipantManager.getZkClient(),
        anyParticipantManager.getClusterName(), anyParticipantManager.getInstanceName()),
        TaskConstants.DEFAULT_TASK_THREAD_POOL_SIZE);
  }

  @Test(dependsOnMethods = "testGetTaskThreadPoolSizeClusterConfigUndefined", expectedExceptions = HelixException.class)
  public void testGetTaskThreadPoolSizeClusterConfigDoesNotExist() {
    MockParticipantManager anyParticipantManager = _participants[0];

    HelixDataAccessor helixDataAccessor = anyParticipantManager.getHelixDataAccessor();
    helixDataAccessor.removeProperty(helixDataAccessor.keyBuilder().clusterConfig());
    TaskUtil.getTargetThreadPoolSize(anyParticipantManager.getZkClient(),
        anyParticipantManager.getClusterName(), anyParticipantManager.getInstanceName());
  }
}
