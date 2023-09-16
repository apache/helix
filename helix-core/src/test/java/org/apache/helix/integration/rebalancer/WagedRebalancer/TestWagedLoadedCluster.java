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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.common.TestClusterOperations.*;

/**
 * This test case is specially targeting for n - n+1 issue.
 *
 * Initially, all the partitions are equal size and equal weight
 * from both CU, DISK.
 * All the nodes are equally loaded.
 * The test case will do the following:
 *     For one resource, we will double its CU weight.
 *     The rebalancer will be triggered.
 *
 * We have a monitoring thread which is constantly monitoring the instance capacity.
 * - It looks at current state resource assignment + pending messages
 * - it  has ASSERT in place to make sure we NEVER cross instance capacity (CU)
 */
public class TestWagedLoadedCluster extends ZkTestBase {
  protected final int NUM_NODE = 6;
  protected static final int START_PORT = 13000;
  protected static final int PARTITIONS = 10;

  protected static final String CLASS_NAME = TestWagedLoadedCluster.class.getSimpleName();
  protected static final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;
  protected AssignmentMetadataStore _assignmentMetadataStore;
  List<MockParticipantManager> _participants = new ArrayList<>();
  List<String> _nodes = new ArrayList<>();
  private final Set<String> _allDBs = new HashSet<>();

  private CountDownLatch _completedTest = new CountDownLatch(1);
  private CountDownLatch _weightUpdatedLatch = new CountDownLatch(1); // when 0, weight is updated

  private Thread _verifyThread = null;
  private final Map<String, Integer> _defaultInstanceCapacity =
      ImmutableMap.of("CU", 50, "DISK", 50);

  private final Map<String, Integer> _defaultPartitionWeight =
      ImmutableMap.of("CU", 10, "DISK", 10);

  private final Map<String, Integer> _newPartitionWeight =
      ImmutableMap.of("CU", 20, "DISK", 10);
  private final int DEFAULT_DELAY = 500; // 0.5 second
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

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

    clusterConfig.setDefaultInstanceCapacityMap(_defaultInstanceCapacity);
    clusterConfig.setDefaultPartitionWeightMap(_defaultPartitionWeight);
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);

    // Create 3 resources with 2 partitions each.
    for (int i = 0; i < 3; i++) {
      String db = "Test-WagedDB-" + i;
      createResourceWithWagedRebalance(CLUSTER_NAME, db, BuiltInStateModelDefinitions.MasterSlave.name(),
          2 /*numPartitions*/, 3 /*replicas*/, 3 /*minActiveReplicas*/);
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, 3);
      _allDBs.add(db);
    }

    // Start a thread which will keep validating instance usage using currentState and pending messages.
    _verifyThread = new Thread(() -> {
      while (_completedTest.getCount() > 0) {
        try {
          validateInstanceUsage();
          Thread.currentThread().sleep(100);
        } catch (InterruptedException e) {
          LOG.debug("Exception in validateInstanceUsageThread", e);
        } catch (Exception e) {
          LOG.error("Exception in validateInstanceUsageThread", e);
        }

      }
    });
  }

  public boolean validateInstanceUsage() {
    try {
      HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
      PropertyKey.Builder propertyKeyBuilder = dataAccessor.keyBuilder();
      // For each instance, get the currentState map and pending messages.
      for (MockParticipantManager participant : _participants) {
        String instance = participant.getInstanceName();
        int totalCUUsage = 0;
        List<String> resourceNames = dataAccessor.
            getChildNames(propertyKeyBuilder.currentStates(instance, participant.getSessionId()));
        for (String resourceName : resourceNames) {
          PropertyKey currentStateKey = propertyKeyBuilder.currentState(instance,
              participant.getSessionId(), resourceName);
          CurrentState currentState = dataAccessor.getProperty(currentStateKey);
          if (currentState != null && currentState.getPartitionStateMap().size() > 0) {
            if (_weightUpdatedLatch.getCount() == 0 && resourceName.equals("Test-WagedDB-0")) {
              // For Test-WagedDB-0, the partition weight is updated to 20 CU.
              totalCUUsage += currentState.getPartitionStateMap().size() * 20;
            } else {
              totalCUUsage += currentState.getPartitionStateMap().size() * 10;
            }
          }
        }
        List<Message> messages = dataAccessor.getChildValues(dataAccessor.keyBuilder().messages(instance), false);
        for (Message m : messages) {
          if (m.getFromState().equals("OFFLINE") && m.getToState().equals("SLAVE")) {
            totalCUUsage += 10;
          }
        }
        assert(totalCUUsage <= 50);
      }
    } catch (Exception e) {
      LOG.error("Exception in validateInstanceUsage", e);
      return false;
    }
    return true;
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

  @Test
  public void testUpdateInstanceCapacity() throws Exception {

    // Check modified time for external view of the first resource.
    // if pipeline is run, then external view would be persisted.
    _verifyThread.start();
    // Update the weight for one of the resource.
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    String db = "Test-WagedDB-0";
    ResourceConfig resourceConfig = dataAccessor.getProperty(dataAccessor.keyBuilder().resourceConfig(db));
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(db);
    }
    resourceConfig.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, _newPartitionWeight));
    dataAccessor.setProperty(dataAccessor.keyBuilder().resourceConfig(db), resourceConfig);
    Thread.currentThread().sleep(100);
    _weightUpdatedLatch.countDown();
    Thread.currentThread().sleep(3000);
    _completedTest.countDown();
    Thread.currentThread().sleep(100);
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
      //_verifyThread.interrupt();
    } catch (Exception e) {
      LOG.info("After class throwing exception, {}", e);
    }
  }
}
