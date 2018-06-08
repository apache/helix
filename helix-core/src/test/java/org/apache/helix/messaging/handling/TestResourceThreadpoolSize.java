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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.mock.participant.DummyProcess;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResourceThreadpoolSize extends ZkStandAloneCMTestBase {
  public static final String TEST_FACTORY = "TestFactory";
  public static final String ONLINE_OFFLINE = "OnlineOffline";
  public static final String OFFLINE_TO_SLAVE = "OFFLINE.SLAVE";
  public static final String SLAVE_TO_MASTER = "SLAVE.MASTER";

  @Test
  public void TestThreadPoolSizeConfig() {
    setResourceThreadPoolSize("NextDB", 12);

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "NextDB", 64, STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "NextDB", 3);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    long taskcount = 0;
    for (int i = 0; i < NODE_NR; i++) {
      DefaultMessagingService svc =
          (DefaultMessagingService) (_participants[i].getMessagingService());
      HelixTaskExecutor helixExecutor = svc.getExecutor();
      ThreadPoolExecutor executor =
          (ThreadPoolExecutor) (helixExecutor._executorMap.get(MessageType.STATE_TRANSITION + "."
              + "NextDB"));
      Assert.assertEquals(12, executor.getMaximumPoolSize());
      taskcount += executor.getCompletedTaskCount();
      Assert.assertTrue(executor.getCompletedTaskCount() > 0);
    }
    Assert.assertEquals(taskcount, 64 * 4);
  }

  @Test public void TestCustomizedResourceThreadPool() {
    int customizedPoolSize = 7;
    int configuredPoolSize = 9;
    for (MockParticipantManager participant : _participants) {
      participant.getStateMachineEngine().registerStateModelFactory(ONLINE_OFFLINE,
          new TestOnlineOfflineStateModelFactory(customizedPoolSize, 0), TEST_FACTORY);
    }

    // add db with default thread pool
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "1", 64,
        STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "1", 3);

    // add db with customized thread pool
    IdealState idealState = new FullAutoModeISBuilder(WorkflowGenerator.DEFAULT_TGT_DB + "2")
        .setStateModel(ONLINE_OFFLINE).setStateModelFactoryName(TEST_FACTORY).setNumPartitions(10)
        .setNumReplica(1).build();
    _gSetupTool.getClusterManagementTool()
        .addResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "2", idealState);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "2", 1);

    // add db with configured pool size
    idealState = new FullAutoModeISBuilder(WorkflowGenerator.DEFAULT_TGT_DB + "3")
        .setStateModel(ONLINE_OFFLINE).setStateModelFactoryName(TEST_FACTORY).setNumPartitions(10)
        .setNumReplica(1).build();
    _gSetupTool.getClusterManagementTool()
        .addResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "3", idealState);
    setResourceThreadPoolSize(WorkflowGenerator.DEFAULT_TGT_DB + "3", configuredPoolSize);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "3", 1);

    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    for (int i = 0; i < NODE_NR; i++) {
      DefaultMessagingService svc =
          (DefaultMessagingService) (_participants[i].getMessagingService());
      HelixTaskExecutor helixExecutor = svc.getExecutor();
      ThreadPoolExecutor executor = (ThreadPoolExecutor) (helixExecutor._executorMap
          .get(MessageType.STATE_TRANSITION + "." + WorkflowGenerator.DEFAULT_TGT_DB + "1"));
      Assert.assertNull(executor);

      executor = (ThreadPoolExecutor) (helixExecutor._executorMap
          .get(MessageType.STATE_TRANSITION + "." + WorkflowGenerator.DEFAULT_TGT_DB + "2"));
      Assert.assertEquals(customizedPoolSize, executor.getMaximumPoolSize());

      executor = (ThreadPoolExecutor) (helixExecutor._executorMap
          .get(MessageType.STATE_TRANSITION + "." + WorkflowGenerator.DEFAULT_TGT_DB + "3"));
      Assert.assertEquals(configuredPoolSize, executor.getMaximumPoolSize());
    }
  }

  @Test
  public void TestPerStateTransitionTypeThreadPool() throws InterruptedException {
    String MASTER_SLAVE = "MasterSlave";

    int customizedPoolSize = 22;
    for (MockParticipantManager participant : _participants) {
      participant.getStateMachineEngine().registerStateModelFactory(MASTER_SLAVE,
          new TestMasterSlaveStateModelFactory(customizedPoolSize), TEST_FACTORY);
    }

    // add db with customized thread pool
    IdealState idealState = new FullAutoModeISBuilder(WorkflowGenerator.DEFAULT_TGT_DB + "4")
        .setStateModel(MASTER_SLAVE).setStateModelFactoryName(TEST_FACTORY).setNumPartitions(10)
        .setNumReplica(1).build();
    _gSetupTool.getClusterManagementTool()
        .addResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "4", idealState);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + "4", 1);

    Thread.sleep(2000);

    // Verify OFFLINE -> SLAVE and SLAVE -> MASTER have different threadpool size.
    for (int i = 0; i < NODE_NR; i++) {
      DefaultMessagingService svc =
          (DefaultMessagingService) (_participants[i].getMessagingService());
      HelixTaskExecutor helixExecutor = svc.getExecutor();
      ThreadPoolExecutor executorOfflineToSlave = (ThreadPoolExecutor) (helixExecutor._executorMap
          .get(MessageType.STATE_TRANSITION + "." + WorkflowGenerator.DEFAULT_TGT_DB + "4" + "."
              + OFFLINE_TO_SLAVE));
      Assert.assertEquals(customizedPoolSize, executorOfflineToSlave.getMaximumPoolSize());

      ThreadPoolExecutor executorSlaveToMaster = (ThreadPoolExecutor) (helixExecutor._executorMap
          .get(MessageType.STATE_TRANSITION + "." + WorkflowGenerator.DEFAULT_TGT_DB + "4" + "."
              + SLAVE_TO_MASTER));
      Assert.assertEquals(customizedPoolSize + 5, executorSlaveToMaster.getMaximumPoolSize());
    }
  }

  @Test
  public void testBatchMessageThreadPoolSize() throws InterruptedException {
    int customizedPoolSize = 5;
    _participants[0].getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new TestOnlineOfflineStateModelFactory(customizedPoolSize, 2000), "TestFactory");
    for (int i = 1; i < _participants.length; i++) {
      _participants[i].syncStop();
    }
    Thread.sleep(2000L);

    // Add 10 dbs with batch message enabled. Each db has 10 partitions.
    // So it will have 10 batch messages and each batch message has 10 sub messages.
    int numberOfDbs = 10;
    for (int i = 0; i < numberOfDbs; i++) {
      String dbName = "TestDBABatch" + i;
      IdealState idealState = new FullAutoModeISBuilder(dbName).setStateModel("OnlineOffline")
          .setStateModelFactoryName("TestFactory").setNumPartitions(10).setNumReplica(1).build();
      idealState.setBatchMessageMode(true);
      _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, dbName, idealState);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, 1);
    }
    Thread.sleep(2000L);

    DefaultMessagingService svc =
        (DefaultMessagingService) (_participants[0].getMessagingService());
    HelixTaskExecutor helixExecutor = svc.getExecutor();
    ThreadPoolExecutor executor = (ThreadPoolExecutor) (helixExecutor._batchMessageExecutorService);
    Assert.assertNotNull(executor);
    Assert.assertTrue(executor.getPoolSize() >= numberOfDbs);

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verify());
  }

  private void setResourceThreadPoolSize(String resourceName, int threadPoolSize) {
    HelixManager manager = _participants[0];
    ConfigAccessor accessor = manager.getConfigAccessor();
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
            .forCluster(manager.getClusterName()).forResource(resourceName).build();
    accessor.set(scope, HelixTaskExecutor.MAX_THREADS, "" + threadPoolSize);
  }

  public static class TestOnlineOfflineStateModelFactory
      extends DummyProcess.DummyOnlineOfflineStateModelFactory {
    int _threadPoolSize;
    ExecutorService _threadPoolExecutor;

    public TestOnlineOfflineStateModelFactory(int threadPoolSize, int delay) {
      super(0);
      if (threadPoolSize > 0) {
        _threadPoolExecutor = Executors.newFixedThreadPool(threadPoolSize);
      }
    }

    @Override public ExecutorService getExecutorService(String resourceName) {
      return _threadPoolExecutor;
    }
  }

  public static class TestMasterSlaveStateModelFactory
      extends DummyProcess.DummyMasterSlaveStateModelFactory {
    int _startThreadPoolSize;
    Map<String, ExecutorService> _threadPoolExecutorMap;

    public TestMasterSlaveStateModelFactory(int startThreadPoolSize) {
      super(0);
      _startThreadPoolSize = startThreadPoolSize;
      _threadPoolExecutorMap = new HashMap<String, ExecutorService>();
      if (_startThreadPoolSize > 0) {
        _threadPoolExecutorMap
            .put(OFFLINE_TO_SLAVE, Executors.newFixedThreadPool(_startThreadPoolSize));
        _threadPoolExecutorMap
            .put(SLAVE_TO_MASTER, Executors.newFixedThreadPool(_startThreadPoolSize + 5));
      }
    }

    @Override
    public ExecutorService getExecutorService(String resourceName, String fromState,
        String toState) {
      return _threadPoolExecutorMap.get(fromState + "." + toState);
    }
  }
}
