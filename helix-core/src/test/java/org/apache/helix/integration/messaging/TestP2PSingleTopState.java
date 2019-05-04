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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.DelayedTransitionBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestP2PSingleTopState extends ZkTestBase {

  final String CLASS_NAME = getShortClassName();
  final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  static final int PARTICIPANT_NUMBER = 24;
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
    System.out
        .println("START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < PARTICIPANT_NUMBER / 2; i++) {
      String instance = PARTICIPANT_PREFIX + "_" + (PARTICIPANT_START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance);
      _instances.add(instance);
    }

    // start dummy participants
    for (int i = 0; i < PARTICIPANT_NUMBER / 2; i++) {
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _instances.get(i));
      participant.setTransition(new DelayedTransitionBase(100));
      participant.syncStart();
      participant.setTransition(new TestTransition(participant.getInstanceName()));
      _participants.add(participant);
    }

    _configAccessor = new ConfigAccessor(_gZkClient);
    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);

    enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, true, 1000000);
    // enableDelayRebalanceInCluster(_gZkClient, CLUSTER_NAME, false);
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    enableP2PInCluster(CLUSTER_NAME, _configAccessor, true);

    // start controller
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, _controllerName);
    _controller.syncStart();

    for (int i = 0; i < DB_COUNT; i++) {
      createResourceWithDelayedRebalance(CLUSTER_NAME, "TestDB_" + i,
          BuiltInStateModelDefinitions.MasterSlave.name(), PARTITION_NUMBER, REPLICA_NUMBER,
          REPLICA_NUMBER - 1, 1000000L, CrushEdRebalanceStrategy.class.getName());
    }

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testRollingUpgrade() throws InterruptedException {
    // rolling upgrade the cluster
    for (String ins : _instances) {
      System.out.println("Disable " + ins);
      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, ins, false);
      Thread.sleep(1000L);
      Assert.assertTrue(_clusterVerifier.verifyByPolling());
      System.out.println("Enable " + ins);
      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, ins, true);
      Thread.sleep(1000L);
      Assert.assertTrue(_clusterVerifier.verifyByPolling());
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertFalse(TestTransition.duplicatedPartitionsSnapshot.keys().hasMoreElements());
  }

  @Test
  public void testAddInstances() throws InterruptedException {
    for (int i = PARTICIPANT_NUMBER / 2; i < PARTICIPANT_NUMBER; i++) {
      String instance = PARTICIPANT_PREFIX + "_" + (PARTICIPANT_START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance);
      _instances.add(instance);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _instances.get(i));
      participant.setTransition(new DelayedTransitionBase(100));
      participant.syncStart();
      participant.setTransition(new TestTransition(participant.getInstanceName()));
      _participants.add(participant);
      Thread.sleep(100);
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertFalse(TestTransition.duplicatedPartitionsSnapshot.keys().hasMoreElements());
  }

  static class TestTransition extends MockTransition {
    // resource.partition that having top state.
    static ConcurrentHashMap<String, Map<String, String>> duplicatedPartitionsSnapshot =
        new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, Map<String, String>> ExternalViews = new ConcurrentHashMap<>();
    static AtomicLong totalToMaster = new AtomicLong();
    static AtomicLong totalRelayMessage = new AtomicLong();

    String _instanceName;

    TestTransition(String instanceName) {
      _instanceName = instanceName;
    }

    public void doTransition(Message message, NotificationContext context) {
      String to = message.getToState();
      String resource = message.getResourceName();
      String partition = message.getPartitionName();
      String key = resource + "." + partition;

      if (to.equals(MasterSlaveSMD.States.MASTER.name())) {
        Map<String, String> mapFields = new HashMap<>(ExternalViews.get(key));
        if (mapFields.values().contains(MasterSlaveSMD.States.MASTER.name())) {
          Map<String, String> newMapFile = new HashMap<>(mapFields);
          newMapFile.put(_instanceName, to);
          duplicatedPartitionsSnapshot.put(key, newMapFile);
        }

        totalToMaster.incrementAndGet();
        if (message.isRelayMessage()) {
          totalRelayMessage.incrementAndGet();
        }
      }

      ExternalViews.putIfAbsent(key, new ConcurrentHashMap<>());
      if (to.equalsIgnoreCase("DROPPED")) {
        ExternalViews.get(key).remove(_instanceName);
      } else {
        ExternalViews.get(key).put(_instanceName, to);
      }
    }
  }
}
