package org.apache.helix.integration;

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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.helix.HelixAdmin;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.ZkTestManager;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.mock.participant.DummyProcess;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestResourceGroupEndtoEnd extends ZkIntegrationTestBase {

  protected static final int GROUP_NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "OnlineOffline";
  protected static final String TEST_DB = "TestDB";
  protected static final int PARTITIONS = 20;
  protected static final int INSTANCE_GROUP_NR = 4;
  protected static final int TOTAL_NODE_NR = GROUP_NODE_NR * INSTANCE_GROUP_NR;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  protected TestParticipantManager[] _participants = new TestParticipantManager[TOTAL_NODE_NR];
  protected ClusterControllerManager _controller;
  protected RoutingTableProvider _routingTableProvider;
  private HelixAdmin _admin;
  HelixManager _spectator;

  int _replica = 3;

  @BeforeClass
  public void beforeClass() throws Exception {
    _admin = new ZKHelixAdmin(_gZkClient);

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    List<String> instanceGroupTags = new ArrayList<String>();
    for (int i = 0; i < INSTANCE_GROUP_NR; i++) {
      String groupTag = "cluster_" + i;
      addInstanceGroup(CLUSTER_NAME, groupTag, GROUP_NODE_NR);
      instanceGroupTags.add(groupTag);
    }

    for (String tag : instanceGroupTags) {
      List<String> instances = _admin.getInstancesInClusterWithTag(CLUSTER_NAME, tag);
      IdealState idealState =
          createIdealState(TEST_DB, tag, instances, PARTITIONS, _replica,
              IdealState.RebalanceMode.CUSTOMIZED.toString());
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, idealState.getResourceName(), idealState);
    }

    // start dummy participants
    int i = 0;
    for (String group : instanceGroupTags) {
      List<String> instances = _admin.getInstancesInClusterWithTag(CLUSTER_NAME, group);
      for (String instance : instances) {
        _participants[i] =
            new TestParticipantManager(ZK_ADDR, CLUSTER_NAME, TEST_DB, group, instance);
        _participants[i].syncStart();
        i++;
      }
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(
            new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                CLUSTER_NAME));
    Assert.assertTrue(result);

    // start speculator
    _routingTableProvider = new RoutingTableProvider();
    _spectator =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "spectator", InstanceType.SPECTATOR,
            ZK_ADDR);
    _spectator.connect();
    _spectator.addExternalViewChangeListener(_routingTableProvider);
    Thread.sleep(1000);
  }

  @AfterClass
  public void afterClass() {
    // stop participants
    for (int i = 0; i < TOTAL_NODE_NR; i++) {
      _participants[i].syncStop();
    }

    _controller.syncStop();
    _spectator.disconnect();
  }

  public IdealState createIdealState(String resourceGroupName, String instanceGroupTag,
      List<String> instanceNames, int numPartition, int replica, String rebalanceMode) {
    IdealState is =
        _gSetupTool.createIdealStateForResourceGroup(resourceGroupName, instanceGroupTag,
            numPartition, replica, rebalanceMode, "OnlineOffline");

    // setup initial partition->instance mapping.
    int nodeIdx = 0;
    int numNode = instanceNames.size();
    assert (numNode >= replica);
    for (int i = 0; i < numPartition; i++) {
      String partitionName = resourceGroupName + "_" + i;
      for (int j = 0; j < replica; j++) {
        is.setPartitionState(partitionName, instanceNames.get((nodeIdx + j) % numNode),
            OnlineOfflineSMD.States.ONLINE.toString());
      }
      nodeIdx++;
    }

    return is;
  }

  private void addInstanceGroup(String clusterName, String instanceTag, int numInstance) {
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < numInstance; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + instanceTag + "_" + (START_PORT + i);
      instances.add(storageNodeName);
      _gSetupTool.addInstanceToCluster(clusterName, storageNodeName);
      _gSetupTool.addInstanceTag(clusterName, storageNodeName, instanceTag);
    }
  }

  @Test
  public void testRoutingTable() throws Exception {
    // Verify routing table works
    Set<InstanceConfig> allOnlineNodes =
        _routingTableProvider.getInstancesForResourceGroup(TEST_DB, "ONLINE");
    Assert.assertEquals(allOnlineNodes.size(), TOTAL_NODE_NR);

    List<InstanceConfig> onlinePartitions =
        _routingTableProvider.getInstancesForResourceGroup(TEST_DB, TEST_DB + "_0", "ONLINE");
    Assert.assertEquals(onlinePartitions.size(), INSTANCE_GROUP_NR * _replica);

    Set<InstanceConfig> selectedNodes = _routingTableProvider
        .getInstancesForResourceGroup(TEST_DB, "ONLINE", Arrays.asList("cluster_2", "cluster_3"));
    Assert.assertEquals(selectedNodes.size(), GROUP_NODE_NR * 2);

    List<InstanceConfig> selectedPartition = _routingTableProvider
        .getInstancesForResourceGroup(TEST_DB, TEST_DB + "_0", "ONLINE",
            Arrays.asList("cluster_2", "cluster_3"));
    Assert.assertEquals(selectedPartition.size(), _replica * 2);
  }

  @Test(dependsOnMethods = { "testRoutingTable" })
  public void testEnableDisableClusters() throws InterruptedException {
    // disable a resource
    _gSetupTool.enableResource(CLUSTER_NAME, TEST_DB, "cluster_2", false);

    Thread.sleep(2000);

    Set<InstanceConfig> selectedNodes = _routingTableProvider
        .getInstancesForResourceGroup(TEST_DB, "ONLINE", Arrays.asList("cluster_2", "cluster_3"));
    Assert.assertEquals(selectedNodes.size(), GROUP_NODE_NR * 1);

    List<InstanceConfig> selectedPartition = _routingTableProvider
        .getInstancesForResourceGroup(TEST_DB, TEST_DB + "_0", "ONLINE",
            Arrays.asList("cluster_2", "cluster_3"));
    Assert.assertEquals(selectedPartition.size(), _replica * 1);

    // enable a resource
    _gSetupTool.enableResource(CLUSTER_NAME, TEST_DB, "cluster_2", true);
    Thread.sleep(2000);

    selectedNodes = _routingTableProvider
        .getInstancesForResourceGroup(TEST_DB, "ONLINE", Arrays.asList("cluster_2", "cluster_3"));
    Assert.assertEquals(selectedNodes.size(), GROUP_NODE_NR * 2);

    selectedPartition = _routingTableProvider
        .getInstancesForResourceGroup(TEST_DB, TEST_DB + "_0", "ONLINE",
            Arrays.asList("cluster_2", "cluster_3"));
    Assert.assertEquals(selectedPartition.size(), _replica * 2);
  }

  public static class MockProcess {
    private static final Logger logger = LoggerFactory.getLogger(DummyProcess.class);
    // public static final String rootNamespace = "rootNamespace";

    private final String _zkConnectString;
    private final String _clusterName;
    private final String _instanceName;
    private final String _resourceName;
    private final String _resourceTag;
    // private StateMachineEngine genericStateMachineHandler;

    private int _transDelayInMs = 0;
    private final String _clusterMangerType;

    public MockProcess(String zkConnectString, String clusterName, String resourceName,
        String instanceName, String resourceTag,
        String clusterMangerType, int delay) {
      _zkConnectString = zkConnectString;
      _clusterName = clusterName;
      _resourceName = resourceName;
      _resourceTag = resourceTag;
      _instanceName = instanceName;
      _clusterMangerType = clusterMangerType;
      _transDelayInMs = delay > 0 ? delay : 0;
    }

    static void sleep(long transDelay) {
      try {
        if (transDelay > 0) {
          Thread.sleep(transDelay);
        }
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    public HelixManager start() throws Exception {
      HelixManager manager = null;
      // zk cluster manager
      if (_clusterMangerType.equalsIgnoreCase("zk")) {
        manager =
            HelixManagerFactory.getZKHelixManager(_clusterName, _instanceName,
                InstanceType.PARTICIPANT, _zkConnectString);
      } else {
        throw new IllegalArgumentException("Unsupported cluster manager type:" + _clusterMangerType);
      }

      MockOnlineOfflineStateModelFactory stateModelFactory2 =
          new MockOnlineOfflineStateModelFactory(_transDelayInMs, _resourceName, _resourceTag,
              _instanceName);
      // genericStateMachineHandler = new StateMachineEngine();
      StateMachineEngine stateMach = manager.getStateMachineEngine();
      stateMach.registerStateModelFactory("OnlineOffline", stateModelFactory2);

      manager.connect();
      //manager.getMessagingService().registerMessageHandlerFactory(MessageType.STATE_TRANSITION.name(), genericStateMachineHandler);
      return manager;
    }

    public static class MockOnlineOfflineStateModelFactory extends
        StateModelFactory<MockOnlineOfflineStateModel> {
      int _delay;
      String _instanceName;
      String _resourceName;
      String _resourceTag;

      public MockOnlineOfflineStateModelFactory(int delay, String resourceName, String resourceTag,
          String instanceName) {
        _delay = delay;
        _instanceName = instanceName;
        _resourceName = resourceName;
        _resourceTag = resourceTag;
      }

      @Override
      public MockOnlineOfflineStateModel createNewStateModel(String resourceName, String stateUnitKey) {
        MockOnlineOfflineStateModel model = new MockOnlineOfflineStateModel();
        model.setDelay(_delay);
        model.setInstanceName(_instanceName);
        model.setResourceName(_resourceName);
        model.setResourceTag(_resourceTag);
        return model;
      }
    }

    public static class MockOnlineOfflineStateModel extends StateModel {
      int _transDelay = 0;
      String _instanceName;
      String _resourceName;
      String _resourceTag;

      public void setDelay(int delay) {
        _transDelay = delay > 0 ? delay : 0;
      }

      public void setInstanceName(String instanceName) {_instanceName = instanceName;}

      public void setResourceTag(String resourceTag) {
        _resourceTag = resourceTag;
      }

      public void setResourceName(String resourceName) {
        _resourceName = resourceName;
      }

      public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
        String db = message.getPartitionName();
        String instanceName = context.getManager().getInstanceName();
        MockProcess.sleep(_transDelay);

        logger.info("MockStateModel.onBecomeOnlineFromOffline(), instance:" + instanceName + ", db:"
            + db);

        logger.info(
            "MockStateModel.onBecomeOnlineFromOffline(), resource " + message.getResourceName()
                + ", partition"
                + message.getPartitionName());

        verifyMessage(message);
      }

      public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        MockProcess.sleep(_transDelay);

        logger.info(
            "MockStateModel.onBecomeOfflineFromOnline(), resource " + message.getResourceName()
                + ", partition"
                + message.getPartitionName() + ", targetName: " + message.getTgtName());

        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        verifyMessage(message);
      }

      public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        MockProcess.sleep(_transDelay);

        logger.info(
            "MockStateModel.onBecomeDroppedFromOffline(), resource " + message.getResourceName()
                + ", partition"
                + message.getPartitionName());

        verifyMessage(message);
      }

      private void verifyMessage(Message message) {
        assert _instanceName.equals(message.getTgtName());
        assert _resourceName.equals(message.getResourceGroupName());
        assert _resourceTag.equals(message.getResourceTag());
      }
    }
  }

  public static class TestParticipantManager extends ZKHelixManager implements Runnable, ZkTestManager {
    private static Logger LOG = LoggerFactory.getLogger(TestParticipantManager.class);

    private final CountDownLatch _startCountDown = new CountDownLatch(1);
    private final CountDownLatch _stopCountDown = new CountDownLatch(1);
    private final CountDownLatch _waitStopCompleteCountDown = new CountDownLatch(1);

    private String _instanceGroup;
    private String _resourceName;

    public TestParticipantManager(String zkAddr, String clusterName, String resourceName,
        String instanceGroup, String instanceName) {
      super(clusterName, instanceName, InstanceType.PARTICIPANT, zkAddr);
      _instanceGroup = instanceGroup;
      _resourceName = resourceName;
    }

    public void syncStop() {
      _stopCountDown.countDown();
      try {
        _waitStopCompleteCountDown.await();
      } catch (InterruptedException e) {
        LOG.error("exception in syncStop participant-manager", e);
      }
    }

    public void syncStart() {
      try {
        new Thread(this).start();
        _startCountDown.await();
      } catch (InterruptedException e) {
        LOG.error("exception in syncStart participant-manager", e);
      }
    }

    @Override
    public void run() {
      try {
        StateMachineEngine stateMach = getStateMachineEngine();
        MockProcess.MockOnlineOfflineStateModelFactory
            ofModelFactory =
            new MockProcess.MockOnlineOfflineStateModelFactory(10, _resourceName, _instanceGroup,
                getInstanceName());
        stateMach.registerStateModelFactory("OnlineOffline", ofModelFactory);

        connect();
        _startCountDown.countDown();

        _stopCountDown.await();
      } catch (InterruptedException e) {
        String msg =
            "participant: " + getInstanceName() + ", " + Thread.currentThread().getName()
                + " is interrupted";
        LOG.info(msg);
      } catch (Exception e) {
        LOG.error("exception running participant-manager", e);
      } finally {
        _startCountDown.countDown();

        disconnect();
        _waitStopCompleteCountDown.countDown();
      }
    }

    @Override
    public ZkClient getZkClient() {
      return _zkclient;
    }

    @Override
    public List<CallbackHandler> getHandlers() {
      return _handlers;
    }
  }
}
