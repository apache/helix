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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTaskStateModelFactory extends TaskTestBase {
  // This value has to be different from the default value to verify correctness
  private static final int TEST_TARGET_TASK_THREAD_POOL_SIZE =
      TaskConstants.DEFAULT_TASK_THREAD_POOL_SIZE + 1;

  @Test
  public void testConfigAccessorCreationMultiZk() throws Exception {
    MockParticipantManager anyParticipantManager = _participants[0];

    InstanceConfig instanceConfig =
        InstanceConfig.toInstanceConfig(anyParticipantManager.getInstanceName());
    instanceConfig.setTargetTaskThreadPoolSize(TEST_TARGET_TASK_THREAD_POOL_SIZE);
    anyParticipantManager.getConfigAccessor()
        .setInstanceConfig(anyParticipantManager.getClusterName(),
            anyParticipantManager.getInstanceName(), instanceConfig);

    // Start a msds server
    final String msdsHostName = "localhost";
    final int msdsPort = 11117;
    final String msdsNamespace = "testTaskStateModelFactory";
    Map<String, Collection<String>> routingData = new HashMap<>();
    routingData
        .put(ZK_ADDR, Collections.singletonList("/" + anyParticipantManager.getClusterName()));
    MockMetadataStoreDirectoryServer msds =
        new MockMetadataStoreDirectoryServer(msdsHostName, msdsPort, msdsNamespace, routingData);
    msds.startServer();

    // Save previously-set system configs
    String prevMultiZkEnabled = System.getProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
    String prevMsdsServerEndpoint =
        System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
    // Turn on multiZk mode in System config
    System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, "true");
    // MSDS endpoint: http://localhost:11117/admin/v2/namespaces/testTaskStateModelFactory
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY,
        "http://" + msdsHostName + ":" + msdsPort + "/admin/v2/namespaces/" + msdsNamespace);

    TaskStateModelFactory taskStateModelFactory =
        new TaskStateModelFactory(anyParticipantManager, Collections.emptyMap());
    ConfigAccessor configAccessor = taskStateModelFactory.createConfigAccessor();
    Assert.assertEquals(TaskUtil
        .getTargetThreadPoolSize(configAccessor, anyParticipantManager.getClusterName(),
            anyParticipantManager.getInstanceName()), TEST_TARGET_TASK_THREAD_POOL_SIZE);

    // Restore system properties
    if (prevMultiZkEnabled == null) {
      System.clearProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
    } else {
      System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, prevMultiZkEnabled);
    }
    if (prevMsdsServerEndpoint == null) {
      System.clearProperty(SystemPropertyKeys.MSDS_SERVER_ENDPOINT_KEY);
    } else {
      System.setProperty(SystemPropertyKeys.MSDS_SERVER_ENDPOINT_KEY, prevMsdsServerEndpoint);
    }
    msds.stopServer();
  }

  @Test(dependsOnMethods = "testConfigAccessorCreationMultiZk")
  public void testConfigAccessorCreationSingleZk() {
    MockParticipantManager anyParticipantManager = _participants[0];

    // Save previously-set system configs
    String prevMultiZkEnabled = System.getProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
    // Turn on multiZk mode in System config
    System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, "false");

    TaskStateModelFactory taskStateModelFactory =
        new TaskStateModelFactory(anyParticipantManager, Collections.emptyMap());
    ConfigAccessor configAccessor = taskStateModelFactory.createConfigAccessor();
    Assert.assertEquals(TaskUtil
        .getTargetThreadPoolSize(configAccessor, anyParticipantManager.getClusterName(),
            anyParticipantManager.getInstanceName()), TEST_TARGET_TASK_THREAD_POOL_SIZE);

    // Restore system properties
    if (prevMultiZkEnabled == null) {
      System.clearProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
    } else {
      System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, prevMultiZkEnabled);
    }
  }
}
