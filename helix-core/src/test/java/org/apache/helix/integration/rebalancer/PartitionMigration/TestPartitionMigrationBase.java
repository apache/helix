package org.apache.helix.integration.rebalancer.PartitionMigration;

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
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.integration.DelayedTransitionBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public class TestPartitionMigrationBase extends ZkTestBase {
  final int NUM_NODE = 6;
  protected static final int START_PORT = 12918;
  protected static final int _PARTITIONS = 50;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;

  List<MockParticipantManager> _participants = new ArrayList<>();
  int _replica = 3;
  int _minActiveReplica = _replica - 1;
  ZkHelixClusterVerifier _clusterVerifier;
  List<String> _testDBs = new ArrayList<>();

  MigrationStateVerifier _migrationVerifier;
  HelixManager _manager;
  ConfigAccessor _configAccessor;


  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      MockParticipantManager participant = createAndStartParticipant(storageNodeName);
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();

    enablePersistIntermediateAssignment(_gZkClient, CLUSTER_NAME, true);

    _manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _configAccessor = new ConfigAccessor(_gZkClient);
  }

  protected MockParticipantManager createAndStartParticipant(String instancename) {
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instancename);

    // start dummy participants
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instancename, 10);
    participant.setTransition(new DelayedTransitionBase(10));
    participant.syncStart();
    return participant;
  }

  protected String[] TestStateModels = {
      BuiltInStateModelDefinitions.MasterSlave.name(),
      BuiltInStateModelDefinitions.OnlineOffline.name(),
      BuiltInStateModelDefinitions.LeaderStandby.name()
  };

  protected Map<String, IdealState> createTestDBs(long delayTime) throws InterruptedException {
    Map<String, IdealState> idealStateMap = new HashMap<>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + i++;
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica, _minActiveReplica,
          -1, CrushRebalanceStrategy.class.getName());
      _testDBs.add(db);
    }
    for (String db : _testDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      idealStateMap.put(db, is);
    }
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(delayTime);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    return idealStateMap;
  }

  class MigrationStateVerifier implements IdealStateChangeListener, ExternalViewChangeListener {
    static final int EXTRA_REPLICA = 1;

    boolean _hasMoreReplica = false;
    boolean _hasLessReplica = false;
    boolean _hasMinActiveReplica = false;
    HelixManager _manager;
    boolean trackEnabled = false;
    Map<String, IdealState> _resourceMap;


    public MigrationStateVerifier(Map<String, IdealState> resourceMap, HelixManager manager) {
      _resourceMap = resourceMap;
      _manager = manager;
    }

    // start tracking changes
    public void start() throws Exception {
      trackEnabled = true;
      _manager.addIdealStateChangeListener(this);
      _manager.addExternalViewChangeListener(this);
    }

    // stop tracking changes
    public void stop() {
      trackEnabled = false;
      PropertyKey.Builder keyBuilder = _manager.getHelixDataAccessor().keyBuilder();
      _manager.removeListener(keyBuilder.idealStates(), this);
      _manager.removeListener(keyBuilder.externalViews(), this);
    }

    @Override
    public void onIdealStateChange(List<IdealState> idealStates, NotificationContext changeContext)
        throws InterruptedException {
      if (!trackEnabled) {
        return;
      }
      for (IdealState is : idealStates) {
        int replica = is.getReplicaCount(NUM_NODE);
        for (String p : is.getPartitionSet()) {
          Map<String, String> stateMap = is.getRecord().getMapField(p);
          verifyPartitionCount(is.getResourceName(), p, stateMap, replica, "IS",
              is.getMinActiveReplicas());
        }
      }
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList,
        NotificationContext changeContext) {
      if (!trackEnabled) {
        return;
      }
      for (ExternalView ev : externalViewList) {
        IdealState is = _resourceMap.get(ev.getResourceName());
        if (is == null) {
          continue;
        }
        int replica = is.getReplicaCount(NUM_NODE);
        for (String p : is.getPartitionSet()) {
          Map<String, String> stateMap = ev.getStateMap(p);
          verifyPartitionCount(is.getResourceName(), p, stateMap, replica, "EV",
              is.getMinActiveReplicas());
        }
      }
    }

    private void verifyPartitionCount(String resource, String partition,
        Map<String, String> stateMap, int replica, String warningPrefix, int minActiveReplica) {
      if (stateMap.size() < replica) {
//        System.out.println(
//            "resource " + resource + ", partition " + partition + " has " + stateMap.size()
//                + " replicas in " + warningPrefix);
        _hasLessReplica = true;
      }

      if (stateMap.size() > replica + EXTRA_REPLICA) {
//        System.out.println(
//            "resource " + resource + ", partition " + partition + " has " + stateMap.size()
//                + " replicas in " + warningPrefix);
//        _hasMoreReplica = true;
      }

      if (stateMap.size() < minActiveReplica) {
//        System.out.println(
//            "resource " + resource + ", partition " + partition + " has " + stateMap.size()
//                + " min active replicas in " + warningPrefix);
        _hasMinActiveReplica = true;
      }
    }

    public boolean hasMoreReplica() {
      return _hasMoreReplica;
    }

    public boolean hasLessReplica() {
      return _hasLessReplica;
    }

    public boolean hasLessMinActiveReplica() {
      return _hasMinActiveReplica;
    }

    public void reset() {
      _hasMoreReplica = false;
      _hasLessReplica = false;
    }
  }


  @AfterClass
  public void afterClass() throws Exception {
    /**
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    _manager.disconnect();
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }
}
