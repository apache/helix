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

import org.apache.helix.HelixManager;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.mock.MockManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class TestTaskStateModelFactory extends TaskTestBase {
  // This value has to be different from the default value to verify correctness
  private static final int TEST_TARGET_TASK_THREAD_POOL_SIZE =
      TaskConstants.DEFAULT_TASK_THREAD_POOL_SIZE + 1;

  @Test
  public void testZkClientCreationMultiZk() throws Exception {
    MockParticipantManager anyParticipantManager = _participants[0];

    InstanceConfig instanceConfig =
        InstanceConfig.toInstanceConfig(anyParticipantManager.getInstanceName());
    instanceConfig.setTargetTaskThreadPoolSize(TEST_TARGET_TASK_THREAD_POOL_SIZE);
    anyParticipantManager.getConfigAccessor()
        .setInstanceConfig(anyParticipantManager.getClusterName(),
            anyParticipantManager.getInstanceName(), instanceConfig);

    // Start a msds server
    // TODO: Refactor all MSDS_SERVER_ENDPOINT creation in system property to one place.
    // Any test that modifies MSDS_SERVER_ENDPOINT system property and accesses
    // HttpRoutingDataReader (ex. TestMultiZkHelixJavaApis and this test) will cause the
    // MSDS_SERVER_ENDPOINT system property to be recorded as final in HttpRoutingDataReader; that
    // means any test class that satisfies the aforementioned condition and is executed first gets
    // to "decide" the default msds endpoint. The only workaround is for all these test classes to
    // use the same default msds endpoint.
    final String msdsHostName = "localhost";
    final int msdsPort = 11117;
    final String msdsNamespace = "multiZkTest";
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
    String testMSDSServerEndpointKey =
        "http://" + msdsHostName + ":" + msdsPort + "/admin/v2/namespaces/" + msdsNamespace;
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY,
        testMSDSServerEndpointKey);

    RoutingDataManager.getInstance().reset();
    verifyThreadPoolSizeAndZkClientClass(anyParticipantManager, TEST_TARGET_TASK_THREAD_POOL_SIZE,
        FederatedZkClient.class);

    // Turn off multiZk mode in System config, and remove zkAddress
    System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, "false");
    ZKHelixManager participantManager = Mockito.spy(anyParticipantManager);
    when(participantManager.getMetadataStoreConnectionString()).thenReturn(null);
    verifyThreadPoolSizeAndZkClientClass(participantManager, TEST_TARGET_TASK_THREAD_POOL_SIZE,
        FederatedZkClient.class);

    // Test no connection config case
    when(participantManager.getRealmAwareZkConnectionConfig()).thenReturn(null);
    verifyThreadPoolSizeAndZkClientClass(participantManager, TEST_TARGET_TASK_THREAD_POOL_SIZE,
        FederatedZkClient.class);

    // Remove server endpoint key and use connection config to specify endpoint
    System.clearProperty(SystemPropertyKeys.MSDS_SERVER_ENDPOINT_KEY);
    RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRealmMode(RealmAwareZkClient.RealmMode.MULTI_REALM)
            .setRoutingDataSourceEndpoint(testMSDSServerEndpointKey)
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP.name()).build();
    when(participantManager.getRealmAwareZkConnectionConfig()).thenReturn(connectionConfig);
    verifyThreadPoolSizeAndZkClientClass(participantManager, TEST_TARGET_TASK_THREAD_POOL_SIZE,
        FederatedZkClient.class);

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

  @Test(dependsOnMethods = "testZkClientCreationMultiZk")
  public void testZkClientCreationSingleZk() {
    MockParticipantManager anyParticipantManager = _participants[0];

    // Save previously-set system configs
    String prevMultiZkEnabled = System.getProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
    // Turn off multiZk mode in System config
    System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, "false");

    verifyThreadPoolSizeAndZkClientClass(anyParticipantManager, TEST_TARGET_TASK_THREAD_POOL_SIZE,
        SharedZkClientFactory.InnerSharedZkClient.class);

    // Restore system properties
    if (prevMultiZkEnabled == null) {
      System.clearProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
    } else {
      System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, prevMultiZkEnabled);
    }
  }

  @Test(dependsOnMethods = "testZkClientCreationSingleZk",
      expectedExceptions = UnsupportedOperationException.class)
  public void testZkClientCreationNonZKManager() {
    TaskStateModelFactory.createZkClient(new MockManager());
  }

  private void verifyThreadPoolSizeAndZkClientClass(HelixManager helixManager, int threadPoolSize,
      Class<?> zkClientClass) {
    RealmAwareZkClient zkClient = TaskStateModelFactory.createZkClient(helixManager);
    try {
      Assert.assertEquals(TaskUtil.getTargetThreadPoolSize(zkClient, helixManager.getClusterName(),
          helixManager.getInstanceName()), threadPoolSize);
      Assert.assertEquals(zkClient.getClass(), zkClientClass);
    } finally {
      zkClient.close();
    }
  }
}
