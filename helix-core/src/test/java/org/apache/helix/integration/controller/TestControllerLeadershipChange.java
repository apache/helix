package org.apache.helix.integration.controller;

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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test on controller leadership on several phases given the test cluster:
 *  1. When a standalone controller becomes the leader
 *  2. When a standalone leader relinquishes the leadership
 *  3. When the leader node relinquishes the leadership and the other controller takes it over
 */
public class TestControllerLeadershipChange extends ZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestControllerLeadershipChange.class);
  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = "TestCluster-" + CLASS_NAME;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, "TestInstance");
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "TestResource", 10, "MasterSlave");
  }

  @AfterClass
  public void afterClass() {
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testControllerCleanUpClusterConfig() {
    ZkBaseDataAccessor baseDataAccessor = new ZkBaseDataAccessor(_gZkClient);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, "DISABLED_Instance");
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, "DISABLED_Instance", false);

    baseDataAccessor.update(PropertyPathBuilder.clusterConfig(CLUSTER_NAME),new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Cluster: " + CLUSTER_NAME + ": cluster config is null");
        }

        ClusterConfig clusterConfig = new ClusterConfig(currentData);
        Map<String, String> disabledInstances = new TreeMap<>(clusterConfig.getDisabledInstances());
        disabledInstances.put("DISABLED_Instance", "HELIX_ENABLED_DISABLE_TIMESTAMP=1652338376608");
        clusterConfig.setDisabledInstances(disabledInstances);

        return clusterConfig.getRecord();
      }
    }, AccessOption.PERSISTENT);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "TestController");
    controller.syncStart();
    verifyControllerIsLeader(controller);

    // Create cluster verifier
    ZkHelixClusterVerifier clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();

    // Wait for rebalanced
    Assert.assertTrue(clusterVerifier.verifyByPolling());
    ZKHelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, baseDataAccessor);
    ClusterConfig cls = helixDataAccessor.getProperty(helixDataAccessor.keyBuilder().clusterConfig());
    Assert.assertFalse(cls.getRecord().getMapFields()
        .containsKey(ClusterConfig.ClusterConfigProperty.DISABLED_INSTANCES.name()));

    controller.syncStop();
    verifyControllerIsNotLeader(controller);
    verifyZKDisconnected(controller);
  }

    @Test
  public void testControllerConnectThenDisconnect() {
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "TestController");
    long start = System.currentTimeMillis();
    controller.syncStart();
    verifyControllerIsLeader(controller);
    LOG.info(System.currentTimeMillis() - start + "ms spent on becoming the leader");

    start = System.currentTimeMillis();
    controller.syncStop();
    verifyControllerIsNotLeader(controller);
    verifyZKDisconnected(controller);

    LOG.info(
        System.currentTimeMillis() - start + "ms spent on becoming the standby node from leader");
  }

  @Test(description = "If the cluster has a controller, the second controller cannot take its leadership")
  public void testWhenControllerAlreadyExists() {
    // when the controller0 already takes over the leadership
    ClusterControllerManager firstController =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "FirstController");
    firstController.syncStart();
    verifyControllerIsLeader(firstController);

    ClusterControllerManager secondController =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "SecondController");
    secondController.syncStart();
    // The second controller cannot acquire the leadership from existing controller
    verifyControllerIsNotLeader(secondController);
    // but the zkClient is still connected
    Assert.assertFalse(secondController.getZkClient().isClosed());

    // stop the controllers
    firstController.syncStop();
    secondController.syncStop();
  }

  @Test
  public void testWhenLeadershipSwitch() {
    ClusterControllerManager firstController =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "FirstController");
    ClusterControllerManager secondController =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "SecondController");
    firstController.syncStart();
    verifyControllerIsLeader(firstController);
    firstController.syncStop();
    verifyControllerIsNotLeader(firstController);
    long start = System.currentTimeMillis();

    // the second controller is started after the first controller is stopped
    secondController.syncStart();
    verifyControllerIsLeader(secondController);
    verifyZKDisconnected(firstController);
    long end = System.currentTimeMillis();
    LOG.info(end - start + "ms spent on the leadership switch");
    secondController.syncStop();
  }

  /**
   * If the controller is not the leader of a cluster,
   * 1. The LEADER node in ZK reflects the leadership of the controller
   * 2. All the callback handlers are ready (successfully registered)
   * 3. Controller Timer tasks are scheduled
   */
  private void verifyControllerIsLeader(ClusterControllerManager controller) {
    // check against the leader node
    Assert.assertTrue(controller.isLeader());

    // check the callback handlers are correctly registered
    List<CallbackHandler> callbackHandlers = controller.getHandlers();
    Assert.assertTrue(callbackHandlers.size() > 0);
    callbackHandlers.forEach(callbackHandler -> Assert.assertTrue(callbackHandler.isReady()));

    // check the zk connection is open
    RealmAwareZkClient zkClient = controller.getZkClient();
    Assert.assertFalse(zkClient.isClosed());
    Long sessionId = zkClient.getSessionId();
    Assert.assertNotNull(sessionId);

    // check the controller related timer tasks are all active
    //TODO: currently no good way to check if controller timer tasks are all stopped without
    // adding a public method only for test purpose
//    Assert.assertTrue(controller.getControllerTimerTasks().size() > 0);
  }

  /**
   * When the controller is not the leader of a cluster, none of the properties
   * {@link #verifyControllerIsLeader(ClusterControllerManager)} will hold
   * NOTE: it's possible the ZKConnection is open while the controller is not the leader
   */
  private void verifyControllerIsNotLeader(ClusterControllerManager controller) {
    // check against the leader node
    Assert.assertFalse(controller.isLeader());

    // check no callback handler is leaked
    Assert.assertTrue(controller.getHandlers().isEmpty());

    // check the controller related timer tasks are all disabled
//    Assert.assertTrue(controller.getControllerTimerTasks().isEmpty());
  }

  private void verifyZKDisconnected(ClusterControllerManager controller) {
    // If the ZK connection is closed, it also means all ZK watchers of the session
    // will be deleted on ZK servers
    Assert.assertTrue(controller.getZkClient().isClosed());
  }


  @Test
  public void testMissingTopStateDurationMonitoring() throws Exception {
    String clusterName = "testCluster-TestControllerLeadershipChange";
    String instanceName = clusterName + "-participant";
    String resourceName = "testResource";
    int numPartition = 1;
    int numReplica = 1;
    int simulatedTransitionDelayMs = 100;
    String stateModel = "LeaderStandby";
    ObjectName resourceMBeanObjectName = getResourceMonitorObjectName(clusterName, resourceName);
    MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

    // Create cluster
    _gSetupTool.addCluster(clusterName, true);

    // Create cluster verifier
    ZkHelixClusterVerifier clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();

    // Create participant
    _gSetupTool.addInstanceToCluster(clusterName, instanceName);
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, clusterName, instanceName, simulatedTransitionDelayMs);
    participant.syncStart();

    // Create controller, since this is the only controller, it will be the leader
    HelixManager manager1 = HelixManagerFactory
        .getZKHelixManager(clusterName, clusterName + "-manager1", InstanceType.CONTROLLER,
            ZK_ADDR);
    manager1.connect();
    Assert.assertTrue(manager1.isLeader());

    // Create resource
    _gSetupTool.addResourceToCluster(clusterName, resourceName, numPartition, stateModel,
        IdealState.RebalanceMode.SEMI_AUTO.name());

    // Rebalance Resource
    _gSetupTool.rebalanceResource(clusterName, resourceName, numReplica);

    // Wait for rebalance
    Assert.assertTrue(clusterVerifier.verifyByPolling());

    // Trigger missing top state in manager1
    participant.syncStop();

    Thread.sleep(1000);

    // Starting manager2
    HelixManager manager2 = HelixManagerFactory
        .getZKHelixManager(clusterName, clusterName + "-manager2", InstanceType.CONTROLLER,
            ZK_ADDR);
    manager2.connect();

    // Set leader to manager2
    setLeader(manager2);

    Assert.assertFalse(manager1.isLeader());
    Assert.assertTrue(manager2.isLeader());

    // Wait for rebalance
    Assert.assertTrue(clusterVerifier.verify());

    Thread.sleep(1000);

    // The moment before manager1 regain leadership. The topstateless duration will start counting.
    long start = System.currentTimeMillis();
    setLeader(manager1);

    Assert.assertTrue(manager1.isLeader());
    Assert.assertFalse(manager2.isLeader());

    // Make resource top state to come back by restarting participant
    participant = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
    participant.syncStart();

    _gSetupTool.rebalanceResource(clusterName, resourceName, numReplica);

    Assert.assertTrue(clusterVerifier.verifyByPolling());
    // The moment that partition top state has been recovered. The topstateless duration stopped counting.
    long end = System.currentTimeMillis();

    // Resource lost top state, and manager1 lost leadership for 2000ms, because manager1 will
    // clean monitoring cache after re-gaining leadership, so max value of hand off duration should
    // not have such a large value
    long duration = (long) beanServer
        .getAttribute(resourceMBeanObjectName, "PartitionTopStateHandoffDurationGauge.Max");
    long controllerOpDuration = end - start;
    Assert.assertTrue(duration >= simulatedTransitionDelayMs && duration <= controllerOpDuration,
        String.format(
            "The recorded TopState-less duration is %d. But the controller operation duration is %d.",
            duration, controllerOpDuration));

    participant.syncStop();
    manager1.disconnect();
    manager2.disconnect();
    deleteCluster(clusterName);
  }

  private void setLeader(HelixManager manager) throws Exception {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    final LiveInstance leader = new LiveInstance(manager.getInstanceName());
    leader.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName());
    leader.setSessionId(manager.getSessionId());
    leader.setHelixVersion(manager.getVersion());

    // Delete the current controller leader node so it will trigger leader election
    while (!manager.isLeader()) {
      accessor.getBaseDataAccessor()
          .remove(PropertyPathBuilder.controllerLeader(manager.getClusterName()),
              AccessOption.EPHEMERAL);
      Thread.sleep(50);
    }
  }

  private ObjectName getResourceMonitorObjectName(String clusterName, String resourceName)
      throws Exception {
    return new ObjectName(String
        .format("%s:cluster=%s,resourceName=%s", MonitorDomainNames.ClusterStatus.name(),
            clusterName, resourceName));
  }
}
