package org.apache.helix.integration.rebalancer;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestZeroReplicaAvoidance extends ZkIntegrationTestBase implements
    ExternalViewChangeListener, IdealStateChangeListener{
  final int NUM_NODE = 6;
  final int START_PORT = 12918;
  final String CLASS_NAME = getShortClassName();
  final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  ClusterSetup _setupTool = null;
  List<MockParticipantManager> _participants = new ArrayList<MockParticipantManager>();
  HelixClusterVerifier _clusterVerifier;
  boolean _testSuccess = true;
  boolean _startListen = false;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(_gZkClient);
    _setupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
      participant.setTransition(new DelayedTransition());
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    controller.syncStart();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
  }

  protected String[] TestStateModels = {
      BuiltInStateModelDefinitions.MasterSlave.name(),
      BuiltInStateModelDefinitions.OnlineOffline.name(),
      BuiltInStateModelDefinitions.LeaderStandby.name()
  };

  @Test
  public void test() throws Exception {
    HelixManager manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, null, InstanceType.SPECTATOR, ZK_ADDR);
    manager.connect();
    manager.addExternalViewChangeListener(this);
    manager.addIdealStateChangeListener(this);
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    // Start half number of nodes.
    int i = 0;
    for (; i < NUM_NODE / 2; i++) {
      _participants.get(i).syncStart();
    }

    int replica = 3;
    int partition = 30;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + stateModel;
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, partition, replica, replica,
          0);
    }
    Assert.assertTrue(_clusterVerifier.verify(50000));

    _startListen = true;
    DelayedTransition.setDelay(50);

    // add the other half of nodes.
    for (; i < NUM_NODE; i++) {
      _participants.get(i).syncStart();
    }
    Assert.assertTrue(_clusterVerifier.verify(50000));
    Assert.assertTrue(_testSuccess);
  }

  /**
   * Validate instances for each partition is on different zone and with necessary tagged
   * instances.
   */
  private void validateNoZeroReplica(IdealState is, ExternalView ev) {
    int replica = is.getReplicaCount(NUM_NODE);
    StateModelDefinition stateModelDef =
        BuiltInStateModelDefinitions.valueOf(is.getStateModelDefRef()).getStateModelDefinition();

    for (String partition : is.getPartitionSet()) {
      Map<String, String> evStateMap = ev.getRecord().getMapField(partition);
      Map<String, String> isStateMap = is.getInstanceStateMap(partition);
      validateMap(is.getResourceName(), partition, replica, evStateMap, stateModelDef);
      validateMap(is.getResourceName(), partition, replica, isStateMap, stateModelDef);
    }
  }

  private void validateMap(String resource, String partition, int replica,
      Map<String, String> instanceStateMap, StateModelDefinition stateModelDef) {
    if (instanceStateMap == null || instanceStateMap.isEmpty()) {
      _testSuccess = false;
      Assert.fail(
          String.format("Resource %s partition %s has no active replica!", resource, partition));
    }
    if (instanceStateMap.size() < replica) {
      _testSuccess = false;
      Assert.fail(String
          .format("Resource %s partition %s has %d active replica, less than required %d!",
              resource, partition, instanceStateMap.size(), replica));
    }

    Map<String, Integer> stateCountMap = stateModelDef.getStateCountMap(NUM_NODE, replica);
    String topState = stateModelDef.getStatesPriorityList().get(0);
    if (stateCountMap.get(topState) == 1) {
      int topCount = 0;
      for (String val : instanceStateMap.values()) {
        if (topState.equals(val)) {
          topCount++;
        }
      }
      if (topCount > 1) {
        _testSuccess = false;
        Assert.fail(String
            .format("Resource %s partition %s has %d replica in %s, more than 1!", resource,
                partition, topCount, topState));
      }
    }
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    if (!_startListen) {
      return;
    }
    for (ExternalView view : externalViewList) {
      IdealState is = _setupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, view.getResourceName());
      validateNoZeroReplica(is, view);
    }
  }

  @Override public void onIdealStateChange(List<IdealState> idealStates,
      NotificationContext changeContext) {
    if (!_startListen) {
      return;
    }
    for (IdealState is : idealStates) {
      ExternalView view = _setupTool.getClusterManagementTool()
          .getResourceExternalView(CLUSTER_NAME, is.getResourceName());
      validateNoZeroReplica(is, view);
    }
  }

  private static class DelayedTransition extends MockTransition {
    private static long _delay = 0;

    public static void setDelay (int delay) {
      _delay = delay;
    }
    @Override
    public void doTransition(Message message, NotificationContext context)
        throws InterruptedException {
      if (_delay > 0) {
        Thread.sleep(_delay);
      }
    }
  }
}
