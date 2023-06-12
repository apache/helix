package org.apache.helix.integration.rebalancer.WagedRebalancer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional warnrmation
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * This test case is specially targeting for n - n+1 issue.
 * Waged is constraint-based algorithm. The end-state of calculated ideal-state will
 * always satisfy hard-constraint.
 * The issue is in order to reach the ideal-state, we need to do intermediate
 * state-transitions.
 * During this phase, hard-constraints are not satisfied.
 * This test case is trying to mimic this by having a very tightly constraint
 * cluster and increasing the weight for some resource and checking if
 * intermediate state satisfies the need or not.
 */
public class TestWagedRebalanceHardConstraint extends ZkTestBase {
  protected final int NUM_NODE = 6;
  protected static final int START_PORT = 13000;
  protected static final int PARTITIONS = 10;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;
  protected AssignmentMetadataStore _assignmentMetadataStore;

  List<MockParticipantManager> _participants = new ArrayList<>();

  List<String> _nodes = new ArrayList<>();
  private final Set<String> _allDBs = new HashSet<>();
  private final int _replica = 3;
  private final Map<String, IdealState> _prevIdealState = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger("TestWagedRebalanceHardConstraint");

  private static final String[] _testModels = {
    BuiltInStateModelDefinitions.OnlineOffline.name(),
    BuiltInStateModelDefinitions.MasterSlave.name(),
    BuiltInStateModelDefinitions.LeaderStandby.name()
  };

  @BeforeClass
  public void beforeClass() throws Exception {
    LOG.info("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      _nodes.add(storageNodeName);
    }

    // start dummy participants
    for (String node : _nodes) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      participant.syncStart();
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    // It's a hacky way to workaround the package restriction. Note that we still want to hide the
    // AssignmentMetadataStore constructor to prevent unexpected update to the assignment records.
    _assignmentMetadataStore =
        new AssignmentMetadataStore(new ZkBucketDataAccessor(ZK_ADDR), CLUSTER_NAME) {
          public Map<String, ResourceAssignment> getBaseline() {
            // Ensure this metadata store always read from the ZK without using cache.
            super.reset();
            return super.getBaseline();
          }

          public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
            // Ensure this metadata store always read from the ZK without using cache.
            super.reset();
            return super.getBestPossibleAssignment();
          }
        };

    // Set test instance capacity and partition weights
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    ClusterConfig clusterConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    String testCapacityKey = "TestCapacityKey";
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(testCapacityKey));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(testCapacityKey, 100));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(testCapacityKey, 6));
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);
  }

  @Test
  public void testInitialPlacement() {
    int i = 0;
    for (String stateModel : _testModels) {
      String db = "Test-WagedDB-" + i++;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, stateModel, PARTITIONS, _replica,
          _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
    validate();
  }

  @Test(dependsOnMethods = { "testInitialPlacement"})
  public void testIncreaseResourcePartitionWeight() throws Exception {
    // For this test, let us record our initial prevIdealState.
    for (String db : _allDBs) {
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      _prevIdealState.put(db, is);
    }

    validateIdealState(true, null);
    printCurrentState();

    // Let us add one more instance
    String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + NUM_NODE);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    _nodes.add(storageNodeName);
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
    participant.syncStart();
    _participants.add(participant);

    // This is to make sure we have run the pipeline.
    Thread.currentThread().sleep(2000);

    LOG.info("After adding the new instance");
    validateIdealState(true, null);
    printCurrentState();


    // Update the weight for one of the resource.
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    String db = "Test-WagedDB-0";
    ResourceConfig resourceConfig = dataAccessor.getProperty(dataAccessor.keyBuilder().resourceConfig(db));
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(db);
    }
    Map<String, Integer> capacityDataMap = ImmutableMap.of("TestCapacityKey", 10);
    resourceConfig.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, capacityDataMap));
    dataAccessor.setProperty(dataAccessor.keyBuilder().resourceConfig(db), resourceConfig);

    // Make sure pipeline is run.
    Thread.currentThread().sleep(2000);

    LOG.info("After changing resource partition weight");
    validate();
    validateIdealState(false, db);
    printCurrentState();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (MockParticipantManager p : _participants) {
      if (p != null && p.isConnected()) {
        p.syncStop();
      }
    }
    deleteCluster(CLUSTER_NAME);
  }

  private void validate() {
    HelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setDeactivatedNodeAwareness(true).setResources(_allDBs)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
    try {
      Assert.assertTrue(_clusterVerifier.verify(5000));
    } finally {
      _clusterVerifier.close();
    }
    for (String db : _allDBs) {
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      validateIsolation(is, ev);
    }
  }
  private void printCurrentState() {
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);

    for (String instance : instances) {
      LiveInstance liveInstance =
          dataAccessor.getProperty(dataAccessor.keyBuilder().liveInstance(instance));
      String sessionId = liveInstance.getEphemeralOwner();
      List<CurrentState> currentStates = dataAccessor.getChildValues(dataAccessor.keyBuilder().currentStates(instance, sessionId),
          true);
      LOG.info("\n\nCurrentState for instance: " + instance);
      for (CurrentState currentState : currentStates) {
          LOG.info("\t" + currentState.getRecord().getMapFields().keySet().toString());
      }
    }
  }
  private void validateIdealState(boolean forAll, String onlyResource) {
    for (String db : _allDBs) {
      IdealState is =
            _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      IdealState prevIdealState = _prevIdealState.get(db);
      if (forAll && prevIdealState != null) {
        Assert.assertNotSame(is.getRecord().getMapFields(), prevIdealState.getRecord().getMapFields());
      } else if (prevIdealState != null && onlyResource.equals(db)) {
        Assert.assertNotSame(is.getRecord().getMapFields(), prevIdealState.getRecord().getMapFields());
      }
      LOG.info("IdealState for resource: " + db);
      for (String partition : is.getPartitionSet()) {
          Map<String, String> assignmentMap = is.getRecord().getMapField(partition);
          LOG.info("\tFor partition: " + partition);
          for (String instance : assignmentMap.keySet()) {
            LOG.info("\t\tinstance: " + instance);
          }
      }
      _prevIdealState.put(db, is);
    }
  }
  private void validateIsolation(IdealState is, ExternalView ev) {
    for (String partition : is.getPartitionSet()) {
      Map<String, String> assignmentMap = ev.getRecord().getMapField(partition);
      Set<String> instancesInEV = assignmentMap.keySet();
      Assert.assertEquals(instancesInEV.size(), _replica);
    }
  }
}

