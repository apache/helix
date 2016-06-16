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
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestCrushAutoRebalance extends ZkIntegrationTestBase {
  final int NUM_NODE = 6;
  protected static final int START_PORT = 12918;
  protected static final int _PARTITIONS = 20;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;

  protected ClusterSetup _setupTool = null;
  List<MockParticipantManager> _participants = new ArrayList<MockParticipantManager>();
  Map<String, String> _nodeToZoneMap = new HashMap<String, String>();
  Map<String, String> _nodeToTagMap = new HashMap<String, String>();
  List<String> _nodes = new ArrayList<String>();
  List<String> _allDBs = new ArrayList<String>();
  int _replica = 3;

  @BeforeClass
  public void beforeClass() throws Exception {
    List<MockParticipantManager> participants = new ArrayList<MockParticipantManager>();
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
      String zone = "zone-" + i % 3;
      String tag = "tag-" + i % 2;
      _setupTool.getClusterManagementTool().setInstanceZoneId(CLUSTER_NAME, storageNodeName, zone);
      _setupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, storageNodeName, tag);
      _nodeToZoneMap.put(storageNodeName, zone);
      _nodeToTagMap.put(storageNodeName, tag);
      _nodes.add(storageNodeName);
    }

    // start dummy participants
    for (String node : _nodes) {
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      participant.syncStart();
      participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
  }

  @Test()
  public void testZoneIsolation() throws Exception {
    List<String> testDBs = new ArrayList<String>();
    String[] testModels = { BuiltInStateModelDefinitions.OnlineOffline.name(),
        BuiltInStateModelDefinitions.MasterSlave.name(),
        BuiltInStateModelDefinitions.LeaderStandby.name()
    };
    int i = 0;
    for (String stateModel : testModels) {
      String db = "Test-DB" + i++;
      _setupTool.addResourceToCluster(CLUSTER_NAME, db, _PARTITIONS, stateModel,
          RebalanceMode.FULL_AUTO + "", CrushRebalanceStrategy.class.getName());
      _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      testDBs.add(db);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    for (String db : testDBs) {
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      validateZoneAndTagIsolation(is, ev);
    }
  }

  @Test()
  public void testZoneIsolationWithInstanceTag() throws Exception {
    List<String> testDBs = new ArrayList<String>();
    Set<String> tags = new HashSet<String>(_nodeToTagMap.values());
    int i = 0;
    for (String tag : tags) {
      String db = "Test-DB-Tag" + i++;
      _setupTool.addResourceToCluster(CLUSTER_NAME, db, _PARTITIONS,
          BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO + "",
          CrushRebalanceStrategy.class.getName());
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      is.setInstanceGroupTag(tag);
      _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, is);
      _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      testDBs.add(db);
      _allDBs.add(db);
    }
    Thread.sleep(300);

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    for (String db : testDBs) {
      IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      ExternalView ev =
          _setupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      validateZoneAndTagIsolation(is, ev);
    }
  }

  /**
   * Validate instances for each partition is on different zone and with necessary tagged instances.
   */
  private void validateZoneAndTagIsolation(IdealState is, ExternalView ev) {
    int replica = Integer.valueOf(is.getReplicas());
    String tag = is.getInstanceGroupTag();

    for (String partition : is.getPartitionSet()) {
      Set<String> assignedZones = new HashSet<String>();

      Set<String> instancesInIs = new HashSet<String>(is.getRecord().getListField(partition));
      Map<String, String> assignmentMap = ev.getRecord().getMapField(partition);
      Set<String> instancesInEV = assignmentMap.keySet();
      Assert.assertEquals(instancesInEV, instancesInIs);
      for (String instance : instancesInEV) {
        assignedZones.add(_nodeToZoneMap.get(instance));
        if (tag != null) {
          InstanceConfig config =
              _setupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, instance);
          Assert.assertTrue(config.containsTag(tag));
        }
      }
      Assert.assertEquals(assignedZones.size(), replica);
    }
  }

  @Test()
  public void testAddZone() throws Exception {
    //TODO
  }

  @Test()
  public void testAddNodes() throws Exception {
    //TODO
  }

  @Test()
  public void testNodeFailure() throws Exception {
    //TODO
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

    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }
}
