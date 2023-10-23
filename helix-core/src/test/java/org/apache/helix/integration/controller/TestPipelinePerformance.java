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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.management.ObjectName;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.model.BuiltInStateModelDefinitions.*;


public class TestPipelinePerformance extends ZkTestBase {

  private static final int NUM_NODE = 6;
  private static final int START_PORT = 12918;
  private static final int REPLICA = 3;
  private static final int PARTITIONS = 20;
  private static final String RESOURCE_NAME = "Test_WAGED_Resource";

  private String _clusterName;
  private ClusterControllerManager _controller;
  private ZkHelixClusterVerifier _clusterVerifier;
  private List<MockParticipantManager> _participants = new ArrayList<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    _clusterName = String.format("CLUSTER_%s_%s", RandomStringUtils.randomAlphabetic(5), getShortClassName());
    _gSetupTool.addCluster(_clusterName, true);

    createResourceWithDelayedRebalance(_clusterName, RESOURCE_NAME, MasterSlave.name(), PARTITIONS, REPLICA, REPLICA, -1);

    for (int i = 0; i < NUM_NODE; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(_clusterName, instanceName);

      // start participants
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, _clusterName, instanceName);
      participant.syncStart();
      _participants.add(participant);
    }

    // start controller
    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    _controller.syncStart();

    enablePersistBestPossibleAssignment(_gZkClient, _clusterName, true);

    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(_clusterName)
        .setZkClient(_gZkClient)
        .setDeactivatedNodeAwareness(true)
        .setResources(Sets.newHashSet(RESOURCE_NAME))
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();

    // Set test instance capacity and partition weights
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(_clusterName, _baseAccessor);
    ClusterConfig clusterConfig = dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    String testCapacityKey = "TestCapacityKey";
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(testCapacityKey));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(testCapacityKey, 100));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(testCapacityKey, 1));
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);
  }

  @AfterClass
  private void windDownTest() {
    _controller.syncStop();
    _participants.forEach(MockParticipantManager::syncStop);
    deleteCluster(_clusterName);
  }

  @Test(enabled = false)
  public void testWagedInstanceCapacityCalculationPerformance() throws Exception {
    ObjectName currentStateMbeanObjectName = new ObjectName(
        String.format("ClusterStatus:cluster=%s,eventName=ClusterEvent,phaseName=CurrentStateComputationStage",
            _clusterName));

    Assert.assertTrue(_server.isRegistered(currentStateMbeanObjectName));
    long initialValue = (Long) _server.getAttribute(currentStateMbeanObjectName, "TotalDurationCounter");

    /************************************************************************************************************
     * Round 1:
     * Enable WAGED on existing resource (this will trigger computation of WagedInstanceCapacity for first time)
     ************************************************************************************************************/
    IdealState currentIdealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(_clusterName, RESOURCE_NAME);
    currentIdealState.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(_clusterName, RESOURCE_NAME, currentIdealState);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    long withComputationValue = (Long) _server.getAttribute(currentStateMbeanObjectName, "TotalDurationCounter");
    long durationWithComputation = withComputationValue - initialValue;

    /************************************************************************************************************
     * Round 2:
     * Perform ideal state change, this wil not cause re-computation of WagedInstanceCapacity
     ************************************************************************************************************/
    currentIdealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(_clusterName, RESOURCE_NAME);
    currentIdealState.setInstanceGroupTag("Test");
    _gSetupTool.getClusterManagementTool().setResourceIdealState(_clusterName, RESOURCE_NAME, currentIdealState);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    long withoutComputationValue = (Long) _server.getAttribute(currentStateMbeanObjectName, "TotalDurationCounter");
    long durationWithoutComputation = withoutComputationValue - durationWithComputation;
    double pctDecrease = (durationWithComputation - durationWithoutComputation) * 100 / durationWithComputation;
    System.out.println(String.format("durationWithComputation: %s, durationWithoutComputation: %s, pctDecrease: %s",
        durationWithComputation, durationWithoutComputation, pctDecrease));

    Assert.assertTrue(durationWithComputation > durationWithoutComputation);
    Assert.assertTrue(pctDecrease > 75.0);
  }

}
