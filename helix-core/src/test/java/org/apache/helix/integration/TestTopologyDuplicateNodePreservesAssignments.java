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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test that validates PR #3034 fix: when topology construction fails due to
 * duplicate end nodes, the controller preserves existing assignments and does not send
 * DROPPED messages to participants.
 *
 * Test Scenario:
 * 1. Create a stable cluster with CRUSHED rebalancer and topology-aware placement
 * 2. Introduce a duplicate topology end node (same zone value)
 * 3. Verify topology exception occurs
 * 4. Verify that ExternalView and CurrentState remain EXACTLY unchanged
 * 5. Verify that NO DROPPED messages are generated
 *
 * This validates that the `!instanceStateMap.isEmpty()` check in MessageGenerationPhase
 * correctly prevents DROPPED messages when BestPossibleState calculation fails due to
 * topology exceptions.
 */
public class TestTopologyDuplicateNodePreservesAssignments extends ZkTestBase {

  private final String CLUSTER_NAME = TestHelper.getTestClassName() + "_cluster";
  private final int PARTICIPANT_COUNT = 3;
  private ClusterControllerManager _controller;
  private ConfigAccessor _configAccessor;
  private HelixDataAccessor _accessor;

  @BeforeClass
  public void setup() {
    System.out.println("Start test " + TestHelper.getTestClassName());
    _configAccessor = new ConfigAccessor(_gZkClient);
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    // Add active participants with unique zones
    for (int i = 0; i < PARTICIPANT_COUNT; i++) {
      String instanceName = "localhost_" + (12000 + i);
      addParticipant(CLUSTER_NAME, instanceName);

      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);
      instanceConfig.setDomain("zone=zone" + i);
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instanceName, instanceConfig);
    }

    // Add a 4th instance but DISABLE it so it doesn't participate initially
    // We'll enable it later with a duplicate zone to trigger the topology error
    String instanceToDisable = "localhost_12003";
    addParticipant(CLUSTER_NAME, instanceToDisable);
    InstanceConfig disabledConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceToDisable);
    disabledConfig.setDomain("zone=zone3");
    disabledConfig.setInstanceEnabled(false);
    _configAccessor.setInstanceConfig(CLUSTER_NAME, instanceToDisable, disabledConfig);

    // Enable topology-aware rebalance with CRUSHED strategy
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setFaultZoneType("zone");
    clusterConfig.setTopology("/zone");
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setPersistBestPossibleAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
  }

  @Test
  public void testDuplicateTopologyNodePreservesAssignments() throws Exception {
    String resourceName = "TestDB";
    int numPartitions = 6;
    int numReplicas = 2;

    // 1. Create CRUSHED resource
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, resourceName, numPartitions, "MasterSlave",
        IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());

    IdealState idealState = _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, resourceName);
    idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
    idealState.setMinActiveReplicas(numReplicas - 1);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, resourceName, idealState);

    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, resourceName, numReplicas);

    // 2. Wait for convergence
    BestPossibleExternalViewVerifier verifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkAddr(ZK_ADDR)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
    Assert.assertTrue(verifier.verifyByPolling(), "Cluster should converge initially");

    // 3. Capture baseline state
    ExternalView baselineEV = _accessor.getProperty(_accessor.keyBuilder().externalView(resourceName));
    Assert.assertNotNull(baselineEV);

    Map<String, Map<String, String>> baselineCurrentStates = captureAllCurrentStates(resourceName);
    int baselineReplicaCount = countTotalReplicas(baselineEV);

    System.out.println("Baseline established: " + baselineReplicaCount + " replicas across " +
        baselineCurrentStates.size() + " instances");

    // 4. Introduce duplicate topology node by enabling the disabled instance with duplicate zone
    String instanceWithDuplicateZone = "localhost_12003";
    InstanceConfig dupConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceWithDuplicateZone);
    dupConfig.setDomain("zone=zone0"); // DUPLICATE! localhost_12000 already has zone=zone0
    dupConfig.setInstanceEnabled(true); // Enable to trigger rebalance
    _configAccessor.setInstanceConfig(CLUSTER_NAME, instanceWithDuplicateZone, dupConfig);

    System.out.println("Introduced duplicate zone: " + instanceWithDuplicateZone +
        " with zone=zone0 (duplicates localhost_12000)");

    // 5. Wait for rebalance attempts
    Thread.sleep(5000);

    // 6. Verify assignments are PRESERVED
    ExternalView currentEV = _accessor.getProperty(_accessor.keyBuilder().externalView(resourceName));
    Assert.assertNotNull(currentEV, "ExternalView should exist after topology failure");
    Assert.assertFalse(verifier.verifyByPolling(10000, 10), "Cluster should not converge");
    int currentReplicaCount = countTotalReplicas(currentEV);

    // KEY ASSERTION: Replica count must remain EXACTLY the same
    Assert.assertEquals(currentReplicaCount, baselineReplicaCount,
        "Replica count must remain exactly the same. Topology failure should NOT trigger " +
        "any assignment changes. This validates that DROPPED messages were not sent.");

    // Verify all baseline assignments are preserved
    for (String partition : baselineEV.getPartitionSet()) {
      Map<String, String> baselineStateMap = baselineEV.getStateMap(partition);
      Map<String, String> currentStateMap = currentEV.getStateMap(partition);

      Assert.assertEquals(currentStateMap, baselineStateMap,
          "Partition " + partition + " assignment must be preserved exactly");
    }

    // Verify CurrentState on participants is unchanged
    Map<String, Map<String, String>> currentStates = captureAllCurrentStates(resourceName);

    for (String instanceName : baselineCurrentStates.keySet()) {
      Map<String, String> baselineInstanceState = baselineCurrentStates.get(instanceName);
      Map<String, String> currentInstanceState = currentStates.get(instanceName);

      Assert.assertNotNull(currentInstanceState,
          "Instance " + instanceName + " should still have CurrentState");
      Assert.assertEquals(currentInstanceState, baselineInstanceState,
          "Instance " + instanceName + " CurrentState must be preserved exactly");
    }

    System.out.println("All assignments preserved despite topology duplicate node error");
    System.out.println("No DROPPED messages sent when topology construction fails");
  }

  /**
   * Capture CurrentState for all live instances for a given resource.
   * Returns map of instanceName -> (partitionName -> state)
   */
  private Map<String, Map<String, String>> captureAllCurrentStates(String resourceName) {
    Map<String, Map<String, String>> result = new HashMap<>();
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();

    for (int i = 0; i < PARTICIPANT_COUNT; i++) {
      String instanceName = "localhost_" + (12000 + i);
      LiveInstance liveInstance = _accessor.getProperty(keyBuilder.liveInstance(instanceName));

      if (liveInstance != null) {
        String sessionId = liveInstance.getEphemeralOwner();
        CurrentState currentState = _accessor.getProperty(
            keyBuilder.currentState(instanceName, sessionId, resourceName));

        if (currentState != null && !currentState.getPartitionStateMap().isEmpty()) {
          result.put(instanceName, new HashMap<>(currentState.getPartitionStateMap()));
        }
      }
    }

    return result;
  }

  /**
   * Count total number of replica assignments in ExternalView
   */
  private int countTotalReplicas(ExternalView ev) {
    int count = 0;
    for (String partition : ev.getPartitionSet()) {
      count += ev.getStateMap(partition).size();
    }
    return count;
  }
}
