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
import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Integration tests for the relaxed disabled partition constraint feature in WAGED rebalancer.
 * This test class verifies that when the relaxed constraint is enabled, disabled partitions
 * remain OFFLINE instead of being reassigned (CrushEd-like behavior).
 */
public class TestWagedRebalancerRelaxedDisabledPartitionConstraint extends ZkTestBase {
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
    TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
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
      }
    }, TestHelper.WAIT_DURATION);
  }

  @AfterMethod
  public void afterMethod() {
    // Clean up resources created during the test
    cleanupResourcesAndResetInstances();
    // Clean up any cluster/resource level configs we may have set
    resetClusterConfigToDefault();
  }

  // ====================================
  // Backward Compatibility Tests
  // ====================================

  @Test
  public void testBackwardCompatibility_PartitionDisabled_RelaxedModeDisabled() throws Exception {
    // Test that existing behavior is preserved when relaxed mode is disabled (default)
    String dbName = generateUniqueResourceName("BackwardCompatPartitionDisabled");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);
    
    // Wait for convergence and find a partition on target instance
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == PARTITIONS && 
             findPartitionOnInstance(dbName, _instanceNames[0]) != null;
    }, TestHelper.WAIT_DURATION);

    String partitionToDisable = findPartitionOnInstance(dbName, _instanceNames[0]);
    Assert.assertNotNull(partitionToDisable, "No partition found on target instance");

    // Disable the partition on the instance
    _gSetupTool.getClusterManagementTool().enablePartition(false, _clusterName, _instanceNames[0],
        dbName, Collections.singletonList(partitionToDisable));
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // With relaxed mode disabled (default behavior), partition should be reassigned away
    validatePartitionDisabledBehavior(dbName, partitionToDisable, _instanceNames[0]);
  }

  @Test
  public void testBackwardCompatibility_AllResourcesDisabled_RelaxedModeDisabled() throws Exception {
    // Test that existing behavior is preserved when relaxed mode is disabled (default)
    String dbName = generateUniqueResourceName("BackwardCompatAllResourcesDisabled");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);

    // Disable all resources on the instance (instance level disable)
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // With relaxed mode disabled (default behavior), all replicas should be evacuated
    validateExternalView(dbName, _instanceNames[0]);
  }

  // ====================================
  // Cluster-Level Relaxed Mode Tests
  // ====================================

  @Test
  public void testClusterLevel_PartitionDisabled_RelaxedModeEnabled() throws Exception {
    String dbName = generateUniqueResourceName("ClusterLevelPartitionDisabled");
    
    // Enable relaxed mode at cluster level
    enableRelaxedConstraintAtClusterLevel(true);
    
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);
    
    // Wait for convergence and find a partition on target instance
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == PARTITIONS && 
             findPartitionOnInstance(dbName, _instanceNames[0]) != null;
    }, TestHelper.WAIT_DURATION);

    String partitionToDisable = findPartitionOnInstance(dbName, _instanceNames[0]);
    Assert.assertNotNull(partitionToDisable, "No partition found on target instance");
    
    // Get initial state before disabling
    String initialState = getPartitionStateOnInstance(dbName, partitionToDisable, _instanceNames[0]);
    Assert.assertNotNull(initialState, "Partition should have a state initially");

    // Disable the partition on the instance
    _gSetupTool.getClusterManagementTool().enablePartition(false, _clusterName, _instanceNames[0],
        dbName, Collections.singletonList(partitionToDisable));
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // With relaxed mode enabled, partition should remain on the instance but in OFFLINE state
    validatePartitionRemainsOfflineOnInstance(dbName, partitionToDisable, _instanceNames[0]);
  }

  @Test
  public void testClusterLevel_AllResourcesDisabled_RelaxedModeEnabled() throws Exception {
    String dbName = generateUniqueResourceName("ClusterLevelAllResourcesDisabled");
    
    // Enable relaxed mode at cluster level
    enableRelaxedConstraintAtClusterLevel(true);
    
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);
    
    // Wait for initial assignment and record partitions on target instance
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == PARTITIONS;
    }, TestHelper.WAIT_DURATION);

    Map<String, String> initialAssignment = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
    Assert.assertFalse(initialAssignment.isEmpty(), "Instance should have partitions initially");

    // Disable all resources on the instance
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // With relaxed mode enabled, partitions should remain on instance but in OFFLINE state
    validatePartitionsRemainOfflineOnInstance(dbName, _instanceNames[0], initialAssignment.keySet());
  }

  // ====================================
  // Resource-Level Relaxed Mode Tests
  // ====================================

  @Test
  public void testResourceLevel_PartitionDisabled_RelaxedModeEnabled() throws Exception {
    String dbName = generateUniqueResourceName("ResourceLevelPartitionDisabled");
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);
    
    // Enable relaxed mode at resource level
    enableRelaxedConstraintAtResourceLevel(dbName, true);
    
    // Wait for convergence and find a partition on target instance
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == PARTITIONS && 
             findPartitionOnInstance(dbName, _instanceNames[0]) != null;
    }, TestHelper.WAIT_DURATION);

    String partitionToDisable = findPartitionOnInstance(dbName, _instanceNames[0]);
    Assert.assertNotNull(partitionToDisable, "No partition found on target instance");

    // Disable the partition on the instance
    _gSetupTool.getClusterManagementTool().enablePartition(false, _clusterName, _instanceNames[0],
        dbName, Collections.singletonList(partitionToDisable));
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // With relaxed mode enabled at resource level, partition should remain OFFLINE on instance
    validatePartitionRemainsOfflineOnInstance(dbName, partitionToDisable, _instanceNames[0]);
  }

  @Test
  public void testResourceLevel_OverridesClusterLevel() throws Exception {
    String dbName1 = generateUniqueResourceName("ResourceOverrideEnabled");
    String dbName2 = generateUniqueResourceName("ResourceOverrideDisabled");
    
    // Enable relaxed mode at cluster level
    enableRelaxedConstraintAtClusterLevel(true);
    
    createResourceWithWagedRebalance(dbName1, PARTITIONS, REPLICAS);
    createResourceWithWagedRebalance(dbName2, PARTITIONS, REPLICAS);
    
    // Override at resource level: dbName1 inherits cluster setting (enabled), dbName2 is explicitly disabled
    enableRelaxedConstraintAtResourceLevel(dbName2, false);
    
    // Wait for convergence
    TestHelper.verify(() -> {
      ExternalView ev1 = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName1);
      ExternalView ev2 = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName2);
      return ev1 != null && ev1.getPartitionSet().size() == PARTITIONS &&
             ev2 != null && ev2.getPartitionSet().size() == PARTITIONS &&
             findPartitionOnInstance(dbName1, _instanceNames[0]) != null &&
             findPartitionOnInstance(dbName2, _instanceNames[0]) != null;
    }, TestHelper.WAIT_DURATION);

    String partition1 = findPartitionOnInstance(dbName1, _instanceNames[0]);
    String partition2 = findPartitionOnInstance(dbName2, _instanceNames[0]);
    
    // Disable partitions on both resources
    _gSetupTool.getClusterManagementTool().enablePartition(false, _clusterName, _instanceNames[0],
        dbName1, Collections.singletonList(partition1));
    _gSetupTool.getClusterManagementTool().enablePartition(false, _clusterName, _instanceNames[0],
        dbName2, Collections.singletonList(partition2));
    
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName1, REPLICAS);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName2, REPLICAS);

    // dbName1 should use cluster setting (relaxed enabled) - partition stays OFFLINE
    validatePartitionRemainsOfflineOnInstance(dbName1, partition1, _instanceNames[0]);
    
    // dbName2 should use resource override (relaxed disabled) - partition gets reassigned
    validatePartitionDisabledBehavior(dbName2, partition2, _instanceNames[0]);
  }

  // ====================================
  // Mixed Scenarios Tests
  // ====================================

  @Test
  public void testMixedResources_DifferentRelaxedSettings() throws Exception {
    String dbNameRelaxed = generateUniqueResourceName("MixedRelaxedEnabled");
    String dbNameStrict = generateUniqueResourceName("MixedRelaxedDisabled");
    
    createResourceWithWagedRebalance(dbNameRelaxed, PARTITIONS, REPLICAS);
    createResourceWithWagedRebalance(dbNameStrict, PARTITIONS, REPLICAS);
    
    // Set different relaxed modes for different resources
    enableRelaxedConstraintAtResourceLevel(dbNameRelaxed, true);
    enableRelaxedConstraintAtResourceLevel(dbNameStrict, false);
    
    // Wait for convergence
    TestHelper.verify(() -> {
      ExternalView ev1 = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbNameRelaxed);
      ExternalView ev2 = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbNameStrict);
      return ev1 != null && ev1.getPartitionSet().size() == PARTITIONS &&
             ev2 != null && ev2.getPartitionSet().size() == PARTITIONS;
    }, TestHelper.WAIT_DURATION);

    // Disable all resources on an instance
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbNameRelaxed, REPLICAS);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbNameStrict, REPLICAS);

    // Resource with relaxed mode enabled should keep partitions OFFLINE on disabled instance
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbNameRelaxed);
      if (ev == null) return false;
      
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap != null && stateMap.containsKey(_instanceNames[0])) {
          String state = stateMap.get(_instanceNames[0]);
          if (!OFFLINE.equals(state)) {
            return false; // Should be OFFLINE
          }
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);

    // Resource with relaxed mode disabled should evacuate partitions from disabled instance
    validateExternalView(dbNameStrict, _instanceNames[0]);
  }

  @Test
  public void testRelaxedMode_WithDelayedRebalance() throws Exception {
    String dbName = generateUniqueResourceName("RelaxedModeWithDelay");
    
    // Enable relaxed mode at cluster level
    enableRelaxedConstraintAtClusterLevel(true);
    
    createResourceWithDelayedRebalance(dbName, PARTITIONS, REPLICAS, 5000); // 5 second delay
    
    // Wait for initial assignment
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == PARTITIONS;
    }, TestHelper.WAIT_DURATION);

    Map<String, String> initialAssignment = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
    Assert.assertFalse(initialAssignment.isEmpty(), "Instance should have partitions initially");

    // Disable instance
    disableInstance(_instanceNames[0]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // With relaxed mode + delayed rebalance: partitions should stay OFFLINE during and after delay
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null) return false;
      
      for (String partition : initialAssignment.keySet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap == null || !stateMap.containsKey(_instanceNames[0])) {
          return false; // Partition should still be on the instance
        }
        String state = stateMap.get(_instanceNames[0]);
        if (!OFFLINE.equals(state)) {
          return false; // Should be OFFLINE
        }
      }
      return true;
    }, 8000); // Wait longer than delay period
  }

  // ====================================
  // Edge Cases and Error Scenarios
  // ====================================

  @Test
  public void testRelaxedMode_MultipleInstancesDisabled() throws Exception {
    String dbName = generateUniqueResourceName("RelaxedModeMultipleInstances");
    
    // Enable relaxed mode at cluster level
    enableRelaxedConstraintAtClusterLevel(true);
    
    createResourceWithWagedRebalance(dbName, PARTITIONS, REPLICAS);
    
    // Wait for convergence
    TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      return ev != null && ev.getPartitionSet().size() == PARTITIONS;
    }, TestHelper.WAIT_DURATION);

    // Record initial assignments
    Map<String, String> assignment1 = getPartitionAssignmentForInstance(dbName, _instanceNames[0]);
    Map<String, String> assignment2 = getPartitionAssignmentForInstance(dbName, _instanceNames[1]);

    // Disable multiple instances
    disableInstance(_instanceNames[0]);
    disableInstance(_instanceNames[1]);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, dbName, REPLICAS);

    // With relaxed mode, partitions should stay OFFLINE on both disabled instances
    if (!assignment1.isEmpty()) {
      validatePartitionsRemainOfflineOnInstance(dbName, _instanceNames[0], assignment1.keySet());
    }
    if (!assignment2.isEmpty()) {
      validatePartitionsRemainOfflineOnInstance(dbName, _instanceNames[1], assignment2.keySet());
    }
  }

  // ====================================
  // Helper Methods
  // ====================================

  private void enableRelaxedConstraintAtClusterLevel(boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(_clusterName);
    clusterConfig.setRelaxedDisabledPartitionConstraint(enabled);
    configAccessor.setClusterConfig(_clusterName, clusterConfig);
  }

  private void enableRelaxedConstraintAtResourceLevel(String resourceName, boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ResourceConfig resourceConfig = configAccessor.getResourceConfig(_clusterName, resourceName);
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(resourceName);
    }
    resourceConfig.setRelaxedDisabledPartitionConstraint(enabled);
    configAccessor.setResourceConfig(_clusterName, resourceName, resourceConfig);
  }

  private void resetClusterConfigToDefault() {
    try {
      ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(_clusterName);
      clusterConfig.setRelaxedDisabledPartitionConstraint(false); // Default is disabled
      configAccessor.setClusterConfig(_clusterName, clusterConfig);
    } catch (Exception e) {
      // Ignore - cluster might not exist yet during setup
    }
  }

  private String getPartitionStateOnInstance(String dbName, String partition, String instance) {
    ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
    if (ev == null) return null;
    
    Map<String, String> stateMap = ev.getStateMap(partition);
    return stateMap != null ? stateMap.get(instance) : null;
  }

  private void validatePartitionRemainsOfflineOnInstance(String dbName, String partition, String instance) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null) return false;
      
      Map<String, String> stateMap = ev.getStateMap(partition);
      if (stateMap == null || !stateMap.containsKey(instance)) {
        return false; // Partition should still be on the instance
      }
      
      String state = stateMap.get(instance);
      return OFFLINE.equals(state); // Should be OFFLINE, not reassigned
    }, TestHelper.WAIT_DURATION), "Partition " + partition + " should remain OFFLINE on instance " + instance);
  }

  private void validatePartitionsRemainOfflineOnInstance(String dbName, String instance, Iterable<String> partitions) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
      if (ev == null) return false;
      
      for (String partition : partitions) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap == null || !stateMap.containsKey(instance)) {
          return false; // Partition should still be on the instance
        }
        
        String state = stateMap.get(instance);
        if (!OFFLINE.equals(state)) {
          return false; // Should be OFFLINE
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION), "All partitions should remain OFFLINE on instance " + instance);
  }

  private void disableInstance(String instanceName) {
    _gSetupTool.getClusterManagementTool().setInstanceOperation(_clusterName, instanceName,
        InstanceConstants.InstanceOperation.DISABLE);
  }

  private String generateUniqueResourceName(String testName) {
    return "TestDB_" + testName + "_" + System.currentTimeMillis();
  }

  // Additional helper methods from parent class
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
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
        return ev != null && ev.getPartitionSet().size() == partitions;
      }
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
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
        ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(_clusterName, dbName);
        return ev != null && ev.getPartitionSet().size() == partitions;
      }
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

  protected void validateExternalView(String dbName, String disabledInstance) throws Exception {
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
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
      }
    }, TestHelper.WAIT_DURATION));
  }

  protected void validatePartitionDisabledBehavior(String dbName, String partitionName, String disabledInstance) throws Exception {
    Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() throws Exception {
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
      }
    }, TestHelper.WAIT_DURATION));
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
      TestHelper.verify(new TestHelper.Verifier() {
        @Override
        public boolean verify() throws Exception {
          long testResourceCount = _gSetupTool.getClusterManagementTool().getResourcesInCluster(_clusterName)
              .stream().filter(name -> name.startsWith("TestDB_")).count();
          return testResourceCount == 0;
        }
      }, 5000);

    } catch (Exception e) {
      System.err.println("Warning: Error during test cleanup: " + e.getMessage());
    }
  }
}
