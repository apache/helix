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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.common.TestClusterOperations.*;


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
public class TestWagedClusterExpansion extends ZkTestBase {
  protected final int NUM_NODE = 6;
  protected static final int START_PORT = 13000;
  protected static final int PARTITIONS = 10;

  protected static final String CLASS_NAME = TestWagedClusterExpansion.class.getSimpleName();
  protected static final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;
  protected AssignmentMetadataStore _assignmentMetadataStore;

  List<MockParticipantManager> _participants = new ArrayList<>();

  List<String> _nodes = new ArrayList<>();
  private final Set<String> _allDBs = new HashSet<>();
  private final int _replica = 3;
  private final int INSTANCE_CAPACITY = 100;
  private final int DEFAULT_PARTITION_CAPACITY = 6;
  private final int INCREASED_PARTITION_CAPACITY = 10;

  private final int REDUCED_INSTANCE_CAPACITY = 98;
  private final int DEFAULT_DELAY = 500; // 0.5 second
  private final String  _testCapacityKey = "TestCapacityKey";
  private final String  _resourceChanged = "Test-WagedDB-0";
  private final Map<String, IdealState> _prevIdealState = new HashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);


  // mock delay master-slave state model
  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "ERROR"
  })
  public static class WagedMasterSlaveModel extends StateModel {
    private static final Logger LOG = LoggerFactory.getLogger(WagedMasterSlaveModel.class);
    private final long _delay;

    public WagedMasterSlaveModel(long delay) {
      _delay = delay;
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      if (_delay > 0) {
        try {
          Thread.currentThread().sleep(_delay);
        } catch (InterruptedException e) {
          // ignore
        }
      }
      LOG.info("Become SLAVE from OFFLINE");
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws InterruptedException {
      LOG.info("Become MASTER from SLAVE");
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      LOG.info("Become Slave from Master");
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      if (_delay > 0) {
        try {
          Thread.currentThread().sleep(_delay);
        } catch (InterruptedException e) {
          // ignore
        }
       }
      LOG.info("Become OFFLINE from SLAVE");
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      if (_delay > 0) {
        try {
          Thread.currentThread().sleep(_delay);
        } catch (InterruptedException e) {
          // ignore
        }
      }
      LOG.info("Become DROPPED FROM OFFLINE");
    }
  }

  public static class WagedDelayMSStateModelFactory extends StateModelFactory<WagedMasterSlaveModel> {
    private long _delay;

    @Override
    public WagedMasterSlaveModel createNewStateModel(String resourceName,
        String partitionKey) {
      WagedMasterSlaveModel model = new WagedMasterSlaveModel(_delay);
      return model;
    }

    public WagedDelayMSStateModelFactory setDelay(long delay) {
      _delay = delay;
      return this;
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    LOG.info("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);
    // create 6 node cluster
    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      _nodes.add(storageNodeName);
    }
    // ST downward message will get delayed by 5sec.
    startParticipants(DEFAULT_DELAY);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

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

    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(_testCapacityKey));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(_testCapacityKey, INSTANCE_CAPACITY));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(_testCapacityKey, DEFAULT_PARTITION_CAPACITY));
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);

    // Create 3 resources with 10 partitions each.
    for (int i = 0; i < 3; i++) {
      String db = "Test-WagedDB-" + i;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, BuiltInStateModelDefinitions.MasterSlave.name(),
          PARTITIONS, _replica, _replica);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _replica);
      _allDBs.add(db);
    }
  }

  private void startParticipants(int delay) {
    // start dummy participants
    for (String node : _nodes) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
      StateMachineEngine stateMach = participant.getStateMachineEngine();
      TestWagedClusterExpansion.WagedDelayMSStateModelFactory delayFactory =
          new TestWagedClusterExpansion.WagedDelayMSStateModelFactory().setDelay(delay);
      stateMach.registerStateModelFactory("MasterSlave", delayFactory);
      participant.syncStart();
      _participants.add(participant);
    }
  }

  // This test case, first adds a new instance which will cause STs.
  // Next, it will try to increase the default weight for one resource.
  @Test
  public void testIncreaseResourcePartitionWeight() throws Exception {
    // For this test, let us record our initial prevIdealState.
    for (String db : _allDBs) {
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      _prevIdealState.put(db, is);
    }

    // Let us add one more instance
    String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + NUM_NODE);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    _nodes.add(storageNodeName);

    // Start the participant.
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
    StateMachineEngine stateMach = participant.getStateMachineEngine();
    TestWagedClusterExpansion.WagedDelayMSStateModelFactory delayFactory =
        new TestWagedClusterExpansion.WagedDelayMSStateModelFactory().setDelay(DEFAULT_DELAY);
    stateMach.registerStateModelFactory("MasterSlave", delayFactory);
    participant.syncStart();
    _participants.add(participant);

    // Check modified time for external view of the first resource.
    // if pipeline is run, then external view would be persisted.
    waitForPipeline(100, 3000);

    LOG.info("After adding the new instance");
    validateIdealState(false /* afterWeightChange */);

    // Update the weight for one of the resource.
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    String db = _resourceChanged;
    ResourceConfig resourceConfig = dataAccessor.getProperty(dataAccessor.keyBuilder().resourceConfig(db));
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(db);
    }
    Map<String, Integer> capacityDataMap = ImmutableMap.of(_testCapacityKey, INCREASED_PARTITION_CAPACITY);
    resourceConfig.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, capacityDataMap));
    dataAccessor.setProperty(dataAccessor.keyBuilder().resourceConfig(db), resourceConfig);

    // Make sure pipeline is run.
    waitForPipeline(100, 10000); // 10 sec. max timeout.

    LOG.info("After changing resource partition weight");
    validateIdealState(true /* afterWeightChange */);
    waitForPipeline(100, 3000); // this is for ZK to sync up.
  }

  // This test case reduces the capacity of one of the instance.
  @Test (dependsOnMethods = "testIncreaseResourcePartitionWeight")
  public void testReduceInstanceCapacity() throws Exception {

    // Reduce capacity for one of the instance
    String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + NUM_NODE);

    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    String db = _resourceChanged;
    InstanceConfig config = dataAccessor.getProperty(dataAccessor.keyBuilder().instanceConfig(storageNodeName));
    if (config == null) {
      config = new InstanceConfig(storageNodeName);
    }
    Map<String, Integer> capacityDataMap = ImmutableMap.of(_testCapacityKey, REDUCED_INSTANCE_CAPACITY);
    config.setInstanceCapacityMap(capacityDataMap);
    dataAccessor.setProperty(dataAccessor.keyBuilder().instanceConfig(storageNodeName), config);

    // Make sure pipeline is run.
    waitForPipeline(100, 10000); // 10 sec. max timeout.

    LOG.info("After changing instance capacity");
    validateIdealState(true);
    waitForPipeline(100, 3000); // this is for ZK to sync up.
  }

  @AfterClass
  public void afterClass() throws Exception {
    try {
      if (_controller != null && _controller.isConnected()) {
        _controller.syncStop();
      }
      for (MockParticipantManager p : _participants) {
        if (p != null && p.isConnected()) {
          p.syncStop();
        }
      }
      deleteCluster(CLUSTER_NAME);
    } catch (Exception e) {
      LOG.info("After class throwing exception, {}", e);
    }
  }

  private void waitForPipeline(long stepSleep, long maxTimeout) {
    // Check modified time for external view of the first resource.
    // if pipeline is run, then external view would be persisted.
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < maxTimeout) {
      String db = _allDBs.iterator().next();
      long modifiedTime = _gSetupTool.getClusterManagementTool().
          getResourceExternalView(CLUSTER_NAME, db).getRecord().getModifiedTime();
      if (modifiedTime - startTime > maxTimeout) {
        break;
      }
      try {
        Thread.currentThread().sleep(stepSleep);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void validateIdealState(boolean afterWeightChange) {
    // Calculate the instance to partition mapping based on previous and new ideal state.

    Map<String, Set<String>> instanceToPartitionMap = new HashMap<>();
    for (String db : _allDBs) {
      IdealState newIdealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      for (String partition : newIdealState.getPartitionSet()) {
        Map<String, String> assignmentMap = newIdealState.getRecord().getMapField(partition);
        List<String> preferenceList = newIdealState.getRecord().getListField(partition);
        for (String instance : assignmentMap.keySet()) {
          if (!preferenceList.contains(instance)) {
            LOG.error("Instance: " + instance + " is not in preference list for partition: " + partition);
          }
          if (!instanceToPartitionMap.containsKey(instance)) {
            instanceToPartitionMap.put(instance, new HashSet<>());
          }
          instanceToPartitionMap.get(instance).add(partition);
        }
      }
    }
    // Now, let us validate the instance to partition mapping.
    for (String instance : instanceToPartitionMap.keySet()) {
      int usedInstanceCapacity = 0;
      for (String partition : instanceToPartitionMap.get(instance)) {
        LOG.info("\tPartition: " + partition);
        if (afterWeightChange && partition.startsWith(_resourceChanged)) {
            usedInstanceCapacity += INCREASED_PARTITION_CAPACITY;
        } else {
          usedInstanceCapacity += DEFAULT_PARTITION_CAPACITY;
        }
      }
      LOG.error("\tInstance: " + instance + " used capacity: " + usedInstanceCapacity);
      // For now, this has to be disabled as this test case is negative scenario.
      if (usedInstanceCapacity > INSTANCE_CAPACITY) {
        LOG.error(instanceToPartitionMap.get(instance).toString());
      }
      Assert.assertTrue(usedInstanceCapacity <= INSTANCE_CAPACITY);
    }
  }
}
