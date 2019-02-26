package org.apache.helix.integration.messaging;

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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.DelayedTransitionBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.messaging.handling.MockHelixTaskExecutor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.mbeans.MessageQueueMonitor;
import org.apache.helix.monitoring.mbeans.ParticipantStatusMonitor;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestP2PNoDuplicatedMessage extends ZkTestBase {
  private static Logger logger = LoggerFactory.getLogger(TestP2PNoDuplicatedMessage.class);

  final String CLASS_NAME = getShortClassName();
  final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  static final int PARTICIPANT_NUMBER = 6;
  static final int PARTICIPANT_START_PORT = 12918;

  static final int DB_COUNT = 2;

  static final int PARTITION_NUMBER = 50;
  static final int REPLICA_NUMBER = 3;

  final String _controllerName = CONTROLLER_PREFIX + "_0";

  List<MockParticipantManager> _participants = new ArrayList<>();
  List<String> _instances = new ArrayList<>();
  ClusterControllerManager _controller;

  ZkHelixClusterVerifier _clusterVerifier;
  ConfigAccessor _configAccessor;
  HelixDataAccessor _accessor;

  @BeforeClass
  public void beforeClass() {
    System.out.println(
        "START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      String instance = PARTICIPANT_PREFIX + "_" + (PARTICIPANT_START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance);
      _instances.add(instance);
    }

    // start dummy participants
    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      MockParticipantManager participant =
          new TestParticipantManager(ZK_ADDR, CLUSTER_NAME, _instances.get(i));
      participant.setTransition(new DelayedTransitionBase(100));
      participant.syncStart();
      _participants.add(participant);
    }

    enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, true);
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    for (int i = 0; i < DB_COUNT; i++) {
      createResourceWithDelayedRebalance(CLUSTER_NAME, "TestDB_" + i,
          BuiltInStateModelDefinitions.MasterSlave.name(), PARTITION_NUMBER, REPLICA_NUMBER,
          REPLICA_NUMBER - 1, 1000000L, CrushEdRebalanceStrategy.class.getName());
    }

    // start controller
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, _controllerName);
    _controller.syncStart();

    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    _configAccessor = new ConfigAccessor(_gZkClient);
    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (MockParticipantManager p : _participants) {
      if (p.isConnected()) {
        p.syncStop();
      }
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testP2PStateTransitionDisabled() {
    enableP2PInCluster(CLUSTER_NAME, _configAccessor, false);

    MockHelixTaskExecutor.resetStats();
    // rolling upgrade the cluster
    for (String ins : _instances) {
      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, ins, false);
      Assert.assertTrue(_clusterVerifier.verifyByPolling());
      verifyP2PDisabled();

      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, ins, true);
      Assert.assertTrue(_clusterVerifier.verifyByPolling());
      verifyP2PDisabled();
    }

    Assert.assertEquals(MockHelixTaskExecutor.duplicatedMessagesInProgress, 0,
        "There are duplicated transition messages sent while participant is handling the state-transition!");
    Assert.assertEquals(MockHelixTaskExecutor.duplicatedMessages, 0,
        "There are duplicated transition messages sent at same time!");
  }

  @Test (dependsOnMethods = {"testP2PStateTransitionDisabled"})
  public void testP2PStateTransitionEnabled() {
    enableP2PInCluster(CLUSTER_NAME, _configAccessor, true);
    long startTime = System.currentTimeMillis();
    MockHelixTaskExecutor.resetStats();
    // rolling upgrade the cluster
    for (String ins : _instances) {
      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, ins, false);
      Assert.assertTrue(_clusterVerifier.verifyByPolling());
      verifyP2PEnabled(startTime);

      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, ins, true);
      Assert.assertTrue(_clusterVerifier.verifyByPolling());
      verifyP2PEnabled(startTime);
    }

    double ratio = ((double) p2pTrigged) / ((double) total);
    Assert.assertTrue(ratio > 0.6, String
       .format("Only %d out of %d percent transitions to Master were triggered by expected host!",
           p2pTrigged, total));

    Assert.assertEquals(MockHelixTaskExecutor.duplicatedMessagesInProgress, 0,
        "There are duplicated transition messages sent while participant is handling the state-transition!");
    Assert.assertEquals(MockHelixTaskExecutor.duplicatedMessages, 0,
        "There are duplicated transition messages sent at same time!");
  }

  private void verifyP2PDisabled() {
    ResourceControllerDataProvider dataCache = new ResourceControllerDataProvider(CLUSTER_NAME);
    dataCache.refresh(_accessor);
    Map<String, LiveInstance> liveInstanceMap = dataCache.getLiveInstances();

    for (LiveInstance instance : liveInstanceMap.values()) {
      Map<String, CurrentState> currentStateMap =
          dataCache.getCurrentState(instance.getInstanceName(), instance.getSessionId());
      Assert.assertNotNull(currentStateMap);
      for (CurrentState currentState : currentStateMap.values()) {
        for (String partition : currentState.getPartitionStateMap().keySet()) {
          String state = currentState.getState(partition);
          if (state.equalsIgnoreCase("MASTER")) {
            String triggerHost = currentState.getTriggerHost(partition);
            Assert.assertEquals(triggerHost, _controllerName,
                state + " of " + partition + " on " + instance.getInstanceName()
                    + " was triggered by " + triggerHost);
          }
        }
      }
    }
  }

  static int total = 0;
  static int p2pTrigged = 0;

  private void verifyP2PEnabled(long startTime) {
    ResourceControllerDataProvider dataCache = new ResourceControllerDataProvider(CLUSTER_NAME);
    dataCache.refresh(_accessor);
    Map<String, LiveInstance> liveInstanceMap = dataCache.getLiveInstances();

    for (LiveInstance instance : liveInstanceMap.values()) {
      Map<String, CurrentState> currentStateMap =
          dataCache.getCurrentState(instance.getInstanceName(), instance.getSessionId());
      Assert.assertNotNull(currentStateMap);
      for (CurrentState currentState : currentStateMap.values()) {
        for (String partition : currentState.getPartitionStateMap().keySet()) {
          String state = currentState.getState(partition);
          long start = currentState.getStartTime(partition);
          if (state.equalsIgnoreCase("MASTER") && start > startTime) {
            String triggerHost = currentState.getTriggerHost(partition);
            if (!triggerHost.equals(_controllerName)) {
              p2pTrigged ++;
            }
            total ++;
          }
        }
      }
    }
  }


  static class TestParticipantManager extends MockParticipantManager {
    private final DefaultMessagingService _messagingService;

    public TestParticipantManager(String zkAddr, String clusterName, String instanceName) {
      super(zkAddr, clusterName, instanceName);
      _messagingService = new MockMessagingService(this);
    }

    @Override
    public ClusterMessagingService getMessagingService() {
      // The caller can register message handler factories on messaging service before the
      // helix manager is connected. Thus we do not do connected check here.
      return _messagingService;
    }
  }

  static class MockMessagingService extends DefaultMessagingService {
    private final HelixTaskExecutor _taskExecutor;
    ConcurrentHashMap<String, MessageHandlerFactory> _messageHandlerFactoriestobeAdded =
        new ConcurrentHashMap<>();
    private final HelixManager _manager;

    public MockMessagingService(HelixManager manager) {
      super(manager);
      _manager = manager;

      boolean isParticipant = false;
      if (manager.getInstanceType() == InstanceType.PARTICIPANT
          || manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        isParticipant = true;
      }

      _taskExecutor = new MockHelixTaskExecutor(
          new ParticipantStatusMonitor(isParticipant, manager.getInstanceName()),
          new MessageQueueMonitor(manager.getClusterName(), manager.getInstanceName()));
    }

    @Override
    public synchronized void registerMessageHandlerFactory(String type,
        MessageHandlerFactory factory) {
      registerMessageHandlerFactory(Collections.singletonList(type), factory);
    }

    @Override
    public synchronized void registerMessageHandlerFactory(List<String> types,
        MessageHandlerFactory factory) {
      if (_manager.isConnected()) {
        for (String type : types) {
          registerMessageHandlerFactoryExtended(type, factory);
        }
      } else {
        for (String type : types) {
          _messageHandlerFactoriestobeAdded.put(type, factory);
        }
      }
    }

    public synchronized void onConnected() {
      for (String type : _messageHandlerFactoriestobeAdded.keySet()) {
        registerMessageHandlerFactoryExtended(type, _messageHandlerFactoriestobeAdded.get(type));
      }
      _messageHandlerFactoriestobeAdded.clear();
    }

    public HelixTaskExecutor getExecutor() {
      return _taskExecutor;
    }


    void registerMessageHandlerFactoryExtended(String type, MessageHandlerFactory factory) {
      int threadpoolSize = HelixTaskExecutor.DEFAULT_PARALLEL_TASKS;
      _taskExecutor.registerMessageHandlerFactory(type, factory, threadpoolSize);
      super.sendNopMessage();
    }
  }
}

