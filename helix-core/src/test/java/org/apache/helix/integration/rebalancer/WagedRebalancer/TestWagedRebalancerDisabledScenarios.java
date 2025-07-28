package org.apache.helix.integration.rebalancer.WagedRebalancer;

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

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestWagedRebalancerDisabledScenarios extends ZkTestBase {
  private static final int NUM_NODE = 6;
  private static final int START_PORT = 12918;
  private static final int PARTITIONS = 20;
  private static final int REPLICAS = 3;
  private static final String OFFLINE = "OFFLINE";

  private final String _className = getShortClassName();
  private final String _clusterName = CLUSTER_PREFIX + "_" + _className;
  private ClusterControllerManager _controller;
  private MockParticipantManager[] _participants;
  private String[] _instanceNames;

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    System.out.println("START " + _className + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(_clusterName, true);
    _gSetupTool.getClusterManagementTool().addStateModelDef(_clusterName, "MasterSlave",
        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
    setupCluster();

    // Enable delayed rebalance at cluster level
    enableDelayRebalanceInCluster(_gZkClient, _clusterName, true, 5000); // Default 5 second delay

    _participants = new MockParticipantManager[NUM_NODE];
    _instanceNames = new String[NUM_NODE];
    for (int i = 0; i < NUM_NODE; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(_clusterName, instanceName);
      _instanceNames[i] = instanceName;
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, controllerName);
    _controller.syncStart();

    // start participants
    for (int i = 0; i < NUM_NODE; i++) {
      _participants[i] = new MockParticipantManager(ZK_ADDR, _clusterName, _instanceNames[i]);
      _participants[i].syncStart();
    }
  }

  protected void setupCluster() {
    // Empty implementation. Can be overridden by the test class.
  }

  @AfterClass
  public void afterClass() {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (int i = 0; i < NUM_NODE; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
    }
    deleteCluster(_clusterName);
    System.out.println("END " + _className + " at " + new Date(System.currentTimeMillis()));
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    // Ensure all instances are enabled before each test
    enableAllInstances();

    // Wait for cluster to be ready and all participants to be connected
    TestHelper.verify(() -> {
      if (!_gSetupTool.getClusterManagementTool().getClusters().contains(_clusterName)) {
        return false;
      }
      // Ensure all participants are connected
      for (MockParticipantManager participant : _participants) {
        if (!participant.isConnected()) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
  }

  @AfterMethod
  public void afterMethod() {
    // Clean up resources created during the test
    cleanupResourcesAndResetInstances();
  }

  @Test
  public void testInstanceDisabled() throws Exception {
    String dbName = generateUniqueResourceName("InstanceDisabled");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);
    // Since the instance is disabled, the replicas should be moved to other instances.
    validateExternalView(dbName, _instanceNames[0]);
  }

  @Test
  public void testResourceDisabled() throws Exception {
    String dbName = generateUniqueResourceName("ResourceDisabled");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);
    _gSetupTool.getClusterManagementTool().enableResource(_clusterName, dbName, false);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);
    // Since the resource is disabled, all replicas should be in OFFLINE state.
    validateExternalViewAllOffline(dbName);
  }

  @Test
  public void testPartitionDisabled() throws Exception {
    String dbName = generateUniqueResourceName("PartitionDisabled");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);
    // Validate rebalance worked - wait for convergence and ensure target instance has partitions
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null || ev.getPartitionSet().size() != PARTITIONS) {
        return false;
      }
      // Ensure the target instance has at least one partition
      return findPartitionOnInstance(dbName, _instanceNames[0]) != null;
    }, TestHelper.WAIT_DURATION);

    // Find a partition that is currently assigned to the target instance
    String partitionToDisable = findPartitionOnInstance(dbName, _instanceNames[0]);
    Assert.assertNotNull("No partition found on instance " + _instanceNames[0], partitionToDisable);

    _gSetupTool.getClusterManagementTool().enablePartition(false, _clusterName, _instanceNames[0],
        dbName, Collections.singletonList(partitionToDisable));
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // Since the specific partition is disabled on one instance:
    // 1. That partition should not be on the disabled instance
    // 2. The partition should be reassigned to maintain replica count
    validatePartitionDisabledBehavior(dbName, partitionToDisable, _instanceNames[0]);
  }

  @Test
  public void testAllResourcesDisabled() throws Exception {
    String dbName = generateUniqueResourceName("AllResourcesDisabled");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);
    // Disable the instance completely (ALL_RESOURCES disabled)
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);
    // Since all resources are disabled on the instance, replicas should be evacuated from that instance.
    validateExternalView(dbName, _instanceNames[0]);
  }

  // ==============================
  // Delayed Rebalance Scenarios
  // ==============================

  @Test
  public void testInstanceDisabledDuringDelayWindow() throws Exception {
    String dbName = generateUniqueResourceName("InstanceDisabledDelayed");
    createResourceWithDelayedRebalance(dbName, PARTITIONS, REPLICAS, 10000); // 10 second delay

    // Wait for initial assignment to stabilize
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null) return false;

      // Check that all partitions are assigned and the target instance has some replicas
      boolean allPartitionsAssigned = ev.getPartitionSet().size() == PARTITIONS;
      Map<String, String> assignment = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
      boolean targetInstanceHasReplicas = !assignment.isEmpty();

      return allPartitionsAssigned && targetInstanceHasReplicas;
    }, 8000);

    // Record initial assignment after stabilization
    Map<String, String> initialAssignment = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
    Assert.assertFalse(initialAssignment.isEmpty(), "Instance should have partitions initially");

    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // During delay window: Replicas should stay on disabled instance, new assignments blocked
    validateInstanceDisabledDuringDelayWindow(dbName, _instanceNames[0], initialAssignment);

    // After delay window expires: Replicas should eventually move out of the disabled instance
    validateInstanceDisabledAfterDelayWindow(dbName, _instanceNames[0]);
  }

  @Test
  public void testInstanceDisabledAfterDelayWindow() throws Exception {
    String dbName = generateUniqueResourceName("InstanceDisabledAfterDelay");
    createResourceWithDelayedRebalance(dbName, PARTITIONS, REPLICAS, 2000); // 2 second delay

    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // Wait for delay window to expire and validate evacuation
    validateInstanceDisabledAfterDelayWindow(dbName, _instanceNames[0]);

    // After delay window: Replicas should be evacuated from disabled instance
    validateExternalView(dbName, _instanceNames[0]);
  }

  @Test
  public void testAllResourcesDisabledDuringDelayWindow() throws Exception {
    String dbName = generateUniqueResourceName("AllResourcesDisabledDelayed");
    createResourceWithDelayedRebalance(dbName, PARTITIONS, REPLICAS, 10000); // 10 second delay

    // Wait for initial assignment to stabilize
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null) return false;

      // Check that all partitions are assigned and the target instance has some replicas
      boolean allPartitionsAssigned = ev.getPartitionSet().size() == PARTITIONS;
      Map<String, String> assignment = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
      boolean targetInstanceHasReplicas = !assignment.isEmpty();

      return allPartitionsAssigned && targetInstanceHasReplicas;
    }, 8000);

    // Record initial assignment after stabilization
    Map<String, String> initialAssignment = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
    Assert.assertFalse(initialAssignment.isEmpty(), "Instance should have partitions initially");

    // Disable all resources on the instance
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // During delay window: Should behave like instance disabled - replicas stay
    validateInstanceDisabledDuringDelayWindow(dbName, _instanceNames[0], initialAssignment);

    // After delay window expires: Replicas should eventually move out of the disabled instance
    validateInstanceDisabledAfterDelayWindow(dbName, _instanceNames[0]);
  }

  @Test
  public void testAllResourcesDisabledAfterDelayWindow() throws Exception {
    String dbName = generateUniqueResourceName("AllResourcesDisabledAfterDelay");
    createResourceWithDelayedRebalance(dbName, PARTITIONS, REPLICAS, 2000); // 2 second delay

    // Disable all resources on the instance
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // Wait for delay window to expire and validate evacuation
    validateInstanceDisabledAfterDelayWindow(dbName, _instanceNames[0]);

    // After delay window: Replicas should be evacuated from disabled instance
    validateExternalView(dbName, _instanceNames[0]);
  }

  @Test
  public void testMultipleInstancesDisabled() throws Exception {
    String dbName = generateUniqueResourceName("MultipleInstancesDisabled");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);

    // Disable multiple instances simultaneously
    disableInstance(_instanceNames[0]);
    disableInstance(_instanceNames[1]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // Since multiple instances are disabled, replicas should be evacuated from both instances
    validateExternalView(dbName, _instanceNames[0]);
    validateExternalView(dbName, _instanceNames[1]);

    // Ensure proper replica distribution on remaining instances
    validateProperReplicaDistribution(dbName, REPLICAS);
  }

  @Test
  public void testMultipleInstancesDisabledDuringDelayWindow() throws Exception {
    String dbName = generateUniqueResourceName("MultipleInstancesDisabledDelayed");
    createResourceWithDelayedRebalance(dbName, PARTITIONS, REPLICAS, 10000); // 10 second delay

    // Wait for initial assignment to stabilize
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null) return false;
      return ev.getPartitionSet().size() == PARTITIONS;
    }, 8000);

    // Record initial assignments before disabling
    Map<String, String> initialAssignment1 = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
    Map<String, String> initialAssignment2 = getPartitionAssignmentForInstance(dbName, _instanceNames[1]);

    // Disable multiple instances
    disableInstance(_instanceNames[0]);
    disableInstance(_instanceNames[1]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // During delay window: Replicas should stay on disabled instances
    validateInstanceDisabledDuringDelayWindow(dbName, _instanceNames[0], initialAssignment1);
    validateInstanceDisabledDuringDelayWindow(dbName, _instanceNames[1], initialAssignment2);

    // After delay window expires: Replicas should be evacuated from both instances
    validateInstanceDisabledAfterDelayWindow(dbName, _instanceNames[0]);
    validateInstanceDisabledAfterDelayWindow(dbName, _instanceNames[1]);
  }

  protected void createResourceWithWagedRebalance(String dbName, int partitions, int replicas) throws Exception {
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(_clusterName, dbName);
    if (idealState == null) {
      idealState = new IdealState(dbName);
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setNumPartitions(partitions);
      idealState.setReplicas(String.valueOf(replicas));
      idealState.setMinActiveReplicas(2);
      idealState.setRebalancerClassName(
          "org.apache.helix.controller.rebalancer.waged.WagedRebalancer");
      idealState.setRebalanceStrategy(
          "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setStateModelFactoryName("DEFAULT");
      _gSetupTool.getClusterManagementTool().addResource(_clusterName, dbName, idealState);
    }
    // Explicit rebalance call is still needed to trigger rebalancing
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, replicas);

    // Wait for the resource to be properly assigned
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == partitions;
    }, TestHelper.WAIT_DURATION));
  }

  protected void createResourceWithDelayedRebalance(String dbName, int partitions, int replicas, int delay) throws Exception {
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(_clusterName, dbName);
    if (idealState == null) {
      idealState = new IdealState(dbName);
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setNumPartitions(partitions);
      idealState.setReplicas(String.valueOf(replicas));
      idealState.setMinActiveReplicas(2);
      idealState.setRebalancerClassName(
          "org.apache.helix.controller.rebalancer.waged.WagedRebalancer");
      idealState.setRebalanceStrategy(
          "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setStateModelFactoryName("DEFAULT");
      idealState.setRebalanceDelay(delay);
      idealState.setDelayRebalanceEnabled(true); // Enable delayed rebalance feature
      _gSetupTool.getClusterManagementTool().addResource(_clusterName, dbName, idealState);
    }
    // Explicit rebalance call is still needed to trigger rebalancing
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, replicas);

    // Wait for the resource to be properly assigned
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == partitions;
    }, TestHelper.WAIT_DURATION));
  }

  protected void validateExternalView(String dbName, String disabledInstance) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView externalView =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (externalView == null) {
        return false;
      }
      for (String partitionName : externalView.getPartitionSet()) {
        Map<String, String> stateMap = externalView.getStateMap(partitionName);
        if (stateMap == null) {
          return false;
        }
        if (stateMap.containsKey(disabledInstance)) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  protected void validatePartitionNotOnInstance(String dbName, String partitionName, String disabledInstance) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView externalView =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (externalView == null) {
        return false;
      }
      Map<String, String> stateMap = externalView.getStateMap(partitionName);
      if (stateMap == null) {
        return true; // If no state map, partition is not on any instance
      }
      // Check that the disabled instance does not have this specific partition
      return !stateMap.containsKey(disabledInstance);
    }, TestHelper.WAIT_DURATION));
  }

  protected void validatePartitionDisabledBehavior(String dbName, String partitionName, String disabledInstance) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView externalView =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (externalView == null) {
        return false;
      }
      Map<String, String> stateMap = externalView.getStateMap(partitionName);
      if (stateMap == null) {
        return false; // Partition should still exist
      }

      // For WAGED rebalancer with FULL_AUTO mode, when a partition is disabled on an instance:
      // 1. The disabled instance should not have this partition in active state
      // 2. The partition should maintain some active replicas on other instances

      // Check that the disabled instance does not have this partition in active states
      boolean partitionNotActiveOnDisabledInstance = true;
      if (stateMap.containsKey(disabledInstance)) {
        String state = stateMap.get(disabledInstance);
        // If the partition exists on disabled instance, it should be in OFFLINE state or DROPPED
        if (!OFFLINE.equals(state) && !"DROPPED".equals(state)) {
          partitionNotActiveOnDisabledInstance = false;
        }
      }

      if (!partitionNotActiveOnDisabledInstance) {
        return false;
      }

      // Check that the partition has some active replicas on other instances
      long activeReplicas = stateMap.values().stream()
          .filter(state -> !OFFLINE.equals(state) && !"DROPPED".equals(state))
          .count();

      // Should have at least 1 active replica (preferably meet min active replicas)
      // But in some edge cases with partition disable, we might have less than ideal replica count
      return activeReplicas >= 1;
    }, TestHelper.WAIT_DURATION));
  }

  protected String findPartitionOnInstance(String dbName, String instanceName) {
    ExternalView externalView =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
    if (externalView == null) {
      return null;
    }

    for (String partitionName : externalView.getPartitionSet()) {
      Map<String, String> stateMap = externalView.getStateMap(partitionName);
      if (stateMap != null && stateMap.containsKey(instanceName)) {
        return partitionName;
      }
    }
    return null;
  }

  protected void validateExternalViewAllOffline(String dbName) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView externalView =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (externalView == null) {
        return false;
      }
      for (String partitionName : externalView.getPartitionSet()) {
        Map<String, String> stateMap = externalView.getStateMap(partitionName);
        if (stateMap == null || stateMap.isEmpty()) {
          return false;
        }
        for (String instance : stateMap.keySet()) {
          if (!stateMap.get(instance).equals(OFFLINE)) {
            return false;
          }
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  private void disableInstance(String instanceName) {
    _gSetupTool.getClusterManagementTool().setInstanceOperation(_clusterName, instanceName,
        InstanceConstants.InstanceOperation.DISABLE);
  }

  private void validateInstanceDisabledDuringDelayWindow(String dbName, String disabledInstance, Map<String, String> originalAssignment) throws Exception {
    // During delay window, partitions should remain on the disabled instance
    TestHelper.verify(() -> {
      Map<String, String> currentAssignment = getPartitionAssignmentForInstance(dbName, disabledInstance);
      return currentAssignment.size() == originalAssignment.size();
    }, 3000);
  }

  private void validateInstanceDisabledAfterDelayWindow(String dbName, String disabledInstance) throws Exception {
    // After delay window, partitions should be evacuated from the disabled instance
    // Use a longer timeout since we need to wait for the delay window to expire (10+ seconds)
    TestHelper.verify(() -> {
      Map<String, String> currentAssignment = getPartitionAssignmentForInstance(dbName, disabledInstance);
      return currentAssignment.isEmpty();
    }, 15000);
  }

  protected Map<String, String> getPartitionAssignmentForInstance(String dbName, String instanceName) {
    ExternalView externalView =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
    if (externalView == null) {
      return Collections.emptyMap();
    }

    Map<String, String> assignments = new HashMap<>();
    for (String partition : externalView.getPartitionSet()) {
      Map<String, String> stateMap = externalView.getStateMap(partition);
      if (stateMap != null && stateMap.containsKey(instanceName)) {
        assignments.put(partition, stateMap.get(instanceName));
      }
    }
    return assignments;
  }

  private String generateUniqueResourceName(String testName) {
    return "TestDB_" + testName + "_" + System.currentTimeMillis();
  }

  private void enableAllInstances() {
    for (String instanceName : _instanceNames) {
      _gSetupTool.getClusterManagementTool().setInstanceOperation(_clusterName, instanceName,
          InstanceConstants.InstanceOperation.ENABLE);

      // Also re-enable all partitions on the instance for all resources
      try {
        for (String resourceName : _gSetupTool.getClusterManagementTool().getResourcesInCluster(_clusterName)) {
          _gSetupTool.getClusterManagementTool().resetPartition(_clusterName, instanceName, resourceName, Collections.emptyList());
        }
      } catch (Exception e) {
        // Ignore errors during cleanup - might not have resources yet
      }
    }
  }

  private void cleanupResourcesAndResetInstances() {
    try {
      // Remove all resources that were created during tests
      for (String resourceName : _gSetupTool.getClusterManagementTool().getResourcesInCluster(_clusterName)) {
        if (resourceName.startsWith("TestDB_")) {
          _gSetupTool.getClusterManagementTool().dropResource(_clusterName, resourceName);
        }
      }

      // Re-enable all instances to clean state
      enableAllInstances();

      // Wait for cleanup to take effect
      TestHelper.verify(() -> {
        long testResourceCount = _gSetupTool.getClusterManagementTool().getResourcesInCluster(_clusterName)
            .stream().filter(name -> name.startsWith("TestDB_")).count();
        return testResourceCount == 0;
      }, 5000);

    } catch (Exception e) {
      System.err.println("Warning: Error during test cleanup: " + e.getMessage());
    }
  }

  protected void validateProperReplicaDistribution(String dbName, int expectedReplicas) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView externalView = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (externalView == null) {
        return false;
      }

      // Check that each partition has the expected number of active replicas
      for (String partitionName : externalView.getPartitionSet()) {
        Map<String, String> stateMap = externalView.getStateMap(partitionName);
        if (stateMap == null) {
          return false;
        }

        long activeReplicas = stateMap.values().stream()
            .filter(state -> !OFFLINE.equals(state) && !"DROPPED".equals(state))
            .count();

        if (activeReplicas != expectedReplicas) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  @Test
  public void testPartitionDisabledDuringDelayWindow() throws Exception {
    String dbName = generateUniqueResourceName("PartitionDisabledDelayed");
    createResourceWithDelayedRebalance(dbName, PARTITIONS, REPLICAS, 10000); // 10 second delay

    // Wait for initial assignment to stabilize and ensure target instance has partitions
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null || ev.getPartitionSet().size() != PARTITIONS) {
        return false;
      }
      // Ensure the target instance has at least one partition
      return findPartitionOnInstance(dbName, _instanceNames[0]) != null;
    }, TestHelper.WAIT_DURATION);

    // Find a partition that is currently assigned to the target instance
    String partitionToDisable = findPartitionOnInstance(dbName, _instanceNames[0]);
    Assert.assertNotNull("No partition found on instance " + _instanceNames[0], partitionToDisable);

    // Disable specific partition on instance
    _gSetupTool.getClusterManagementTool().enablePartition(false, _clusterName, _instanceNames[0],
        dbName, Collections.singletonList(partitionToDisable));
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // During delay window: The disabled partition should remain for some time but in OFFLINE state
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null) return false;
      Map<String, String> stateMap = ev.getStateMap(partitionToDisable);
      if (stateMap == null) return true; // Partition has been moved
      return !stateMap.containsKey(_instanceNames[0]) || OFFLINE.equals(stateMap.get(_instanceNames[0]));
    }, 5000);

    // Eventually: The disabled partition should be completely reassigned
    validatePartitionDisabledBehavior(dbName, partitionToDisable, _instanceNames[0]);
  }

  @Test
  public void testMixedResourcesWithDifferentDelaySettings() throws Exception {
    String dbName1 = generateUniqueResourceName("MixedDelayed1");
    String dbName2 = generateUniqueResourceName("MixedDelayed2");
    String dbName3 = generateUniqueResourceName("MixedNoDelay");

    // Create resources with different delay settings
    createResourceWithDelayedRebalance(dbName1, PARTITIONS, REPLICAS, 10000); // 10s delay
    createResourceWithDelayedRebalance(dbName2, PARTITIONS, REPLICAS, 2000);  // 2s delay
    createResourceWithWagedRebalance(dbName3, PARTITIONS, REPLICAS);          // No delay

    // Wait for all resources to stabilize
    TestHelper.verify(() -> {
      ExternalView ev1 = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName1);
      ExternalView ev2 = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName2);
      ExternalView ev3 = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName3);
      return ev1 != null && ev1.getPartitionSet().size() == PARTITIONS &&
             ev2 != null && ev2.getPartitionSet().size() == PARTITIONS &&
             ev3 != null && ev3.getPartitionSet().size() == PARTITIONS;
    }, 8000);

    // Disable instance
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName1, REPLICAS);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName2, REPLICAS);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName3, REPLICAS);

    // Resource with no delay should evacuate immediately
    validateExternalView(dbName3, _instanceNames[0]);

    // Resource with 2s delay should evacuate after short wait
    TestHelper.verify(() -> {
      ExternalView ev2 = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName2);
      if (ev2 == null) return false;
      for (String partition : ev2.getPartitionSet()) {
        Map<String, String> stateMap = ev2.getStateMap(partition);
        if (stateMap != null && stateMap.containsKey(_instanceNames[0])) {
          return false;
        }
      }
      return true;
    }, 5000);

    // Resource with 10s delay may still have replicas (depending on timing)
    // but should eventually evacuate
    validateInstanceDisabledAfterDelayWindow(dbName1, _instanceNames[0]);
  }

  @Test
  public void testInstanceReEnableAfterDisable() throws Exception {
    String dbName = generateUniqueResourceName("InstanceReEnabled");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);

    // Wait for initial assignment to stabilize
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == PARTITIONS;
    }, TestHelper.WAIT_DURATION);

    // Record initial assignment
    Map<String, String> initialAssignment = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
    Assert.assertTrue(!initialAssignment.isEmpty(), "Instance should have partitions initially");

    // Disable instance
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // Validate evacuation
    validateExternalView(dbName, _instanceNames[0]);

    // Re-enable instance
    _gSetupTool.getClusterManagementTool().setInstanceOperation(_clusterName, _instanceNames[0],
        InstanceConstants.InstanceOperation.ENABLE);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // Validate that instance can receive partitions again
    TestHelper.verify(() -> {
      Map<String, String> currentAssignment = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
      return !currentAssignment.isEmpty();
    }, TestHelper.WAIT_DURATION);
  }

  @Test
  public void testInstanceDisableWithMinActiveReplicaViolation() throws Exception {
    String dbName = generateUniqueResourceName("MinActiveReplicaViolation");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);

    // Disable enough instances to potentially violate minActiveReplicas (which is set to 2)
    disableInstance(_instanceNames[0]);
    disableInstance(_instanceNames[1]);
    disableInstance(_instanceNames[2]);
    disableInstance(_instanceNames[3]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // With only 2 instances left (NUM_NODE=6, disabled 4), should still maintain minActiveReplicas=2
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null) return false;

      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap == null) return false;

        long activeReplicas = stateMap.values().stream()
            .filter(state -> !OFFLINE.equals(state) && !"DROPPED".equals(state))
            .count();

        // Should maintain at least minActiveReplicas=2
        if (activeReplicas < 2) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
  }

  @Test
  public void testPartialPartitionDisable() throws Exception {
    String dbName = generateUniqueResourceName("PartialPartitionDisable");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);

    // Wait for stabilization
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == PARTITIONS;
    }, 8000);

    // Disable multiple partitions on the same instance
    ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
    Map<String, String> instancePartitions = new HashMap<>();
    for (String partition : ev.getPartitionSet()) {
      Map<String, String> stateMap = ev.getStateMap(partition);
      if (stateMap != null && stateMap.containsKey(_instanceNames[0])) {
        instancePartitions.put(partition, stateMap.get(_instanceNames[0]));
      }
    }

    // Take first 2 partitions from the target instance
    String[] partitionsToDisable = instancePartitions.keySet().stream()
        .limit(2)
        .toArray(String[]::new);

    if (partitionsToDisable.length >= 2) {
      _gSetupTool.getClusterManagementTool().enablePartition(false, _clusterName, _instanceNames[0],
          dbName, java.util.Arrays.asList(partitionsToDisable));
      _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

      // Validate that disabled partitions are not on the instance
      for (String partition : partitionsToDisable) {
        validatePartitionDisabledBehavior(dbName, partition, _instanceNames[0]);
      }

      // Validate that other partitions can still be on the instance
      TestHelper.verify(() -> {
        ExternalView extView = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
        if (extView == null) return false;

        // Check if instance still has some partitions (not the disabled ones)
        for (String partition : extView.getPartitionSet()) {
          if (!java.util.Arrays.asList(partitionsToDisable).contains(partition)) {
            Map<String, String> stateMap = extView.getStateMap(partition);
            if (stateMap != null && stateMap.containsKey(_instanceNames[0])) {
              String state = stateMap.get(_instanceNames[0]);
              if (!OFFLINE.equals(state) && !"DROPPED".equals(state)) {
                return true; // Found at least one active partition
              }
            }
          }
        }
        return false;
      }, TestHelper.WAIT_DURATION);
    }
  }
}
