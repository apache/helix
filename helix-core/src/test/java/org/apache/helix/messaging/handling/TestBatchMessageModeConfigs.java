package org.apache.helix.messaging.handling;

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

import java.util.concurrent.ThreadPoolExecutor;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestBatchMessageModeConfigs extends ZkStandAloneCMTestBase {
  static final String TEST_DB_PREFIX = "TestDBABatch";
  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    _participants[0].getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new TestResourceThreadpoolSize.TestOnlineOfflineStateModelFactory(5, 2000), "TestFactory");
    // Use one node for testing
    for (int i = 1; i < _participants.length; i++) {
      _participants[i].syncStop();
    }
    Thread.sleep(2000L);
  }

  @Test
  public void testEnableBatchModeForCluster() throws InterruptedException {
    _gSetupTool.getClusterManagementTool().enableBatchMessageMode(CLUSTER_NAME, true);
    String dbName = TEST_DB_PREFIX + "Cluster";
    setupResource(dbName);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, 1);
    Thread.sleep(2000L);
    verify();
    _gSetupTool.getClusterManagementTool().enableBatchMessageMode(CLUSTER_NAME, false);
  }

  @Test
  public void testEnableBatchModeForResource() throws InterruptedException {
    String dbName = TEST_DB_PREFIX + "Resource";
    setupResource(dbName);
    _gSetupTool.getClusterManagementTool().enableBatchMessageMode(CLUSTER_NAME, dbName, true);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, 1);
    Thread.sleep(2000L);
    verify();
    _gSetupTool.getClusterManagementTool().enableBatchMessageMode(CLUSTER_NAME, dbName, false);
  }

  private void setupResource(String dbName) throws InterruptedException {
    IdealState idealState = new FullAutoModeISBuilder(dbName).setStateModel("OnlineOffline")
        .setStateModelFactoryName("TestFactory").setNumPartitions(10).setNumReplica(1).build();
    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, dbName, idealState);
  }

  private void verify() {
    DefaultMessagingService svc =
        (DefaultMessagingService) (_participants[0].getMessagingService());
    HelixTaskExecutor helixExecutor = svc.getExecutor();
    ThreadPoolExecutor executor = (ThreadPoolExecutor) (helixExecutor._batchMessageExecutorService);
    Assert.assertNotNull(executor);
    Assert.assertTrue(executor.getPoolSize() > 0);
  }
}
