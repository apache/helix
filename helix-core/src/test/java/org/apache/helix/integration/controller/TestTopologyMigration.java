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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.examples.LeaderStandbyStateModelFactory;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTopologyMigration extends ZkTestBase {
  private static final int START_PORT = 12918; // Starting port for mock participants
  private static int _nextStartPort = START_PORT; // Incremental port for participants
  private static final String TEST_CAPACITY_KEY = "TestCapacityKey";
  private static final int TEST_CAPACITY_VALUE = 100; // Default instance capacity for testing
  private static final String RACK = "rack"; // Rack identifier in topology
  private static final String HOST = "host"; // Host identifier in topology
  private static final String APPLICATION_INSTANCE_ID = "applicationInstanceId";
  private static final String MZ = "mz"; // Migrated zone identifier
  private static final String INIT_TOPOLOGY = String.format("/%s/%s", RACK, HOST);
  // Initial topology format
  private static final String MIGRATED_TOPOLOGY =
      String.format("/%s/%s/%s", MZ, HOST, APPLICATION_INSTANCE_ID); // New topology format
  private static final int INIT_ZONE_COUNT = 12; // Initial zone count
  private static final int MIGRATE_ZONE_COUNT = 6; // Zone count post-migration
  private static final int RESOURCE_COUNT = 2; // Number of resources in the cluster
  private static final int INSTANCES_PER_RESOURCE = 12; // Number of instances per resource
  private static final int PARTITIONS = 3; // Number of partitions
  private static final int REPLICA = 6; // Number of replicas
  private static final long DEFAULT_RESOURCE_DELAY_TIME = 1800000L;
  // Delay time for resource rebalance

  private final String CLASS_NAME = getShortClassName(); // Test class name
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME; // Cluster name for testing

  protected ClusterControllerManager _controller; // Cluster controller instance
  private final List<MockParticipantManager> _participants = new ArrayList<>();
  // List of participant managers
  private final Set<String> _allDBs = new HashSet<>(); // Set of all databases
  private ZkHelixClusterVerifier _clusterVerifier; // Cluster verifier
  private ConfigAccessor _configAccessor; // Config accessor

  /**
   * Sets up the test cluster and initializes participants before running tests.
   */
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    // Start cluster controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
    _configAccessor = new ConfigAccessor(_gZkClient);

    // Set up cluster configuration and participants
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    setupClusterConfig(INIT_TOPOLOGY, RACK);
    setupInitResourcesAndParticipants();

    // Initialize cluster verifier for validating state
    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setResources(_allDBs).setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
  }

  /**
   * Cleans up after the test by stopping participants and dropping resources.
   */
  @AfterClass
  public void afterClass() {
    // Drop all databases from the cluster
    for (String db : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Stop all participants and controller
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    _controller.syncStop();
  }

  /**
   * Sets up the cluster configuration with the given topology and fault zone type.
   */
  private void setupClusterConfig(String topology, String faultZoneType) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(DEFAULT_RESOURCE_DELAY_TIME);
    clusterConfig.setTopology(topology);
    clusterConfig.setFaultZoneType(faultZoneType);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(TEST_CAPACITY_KEY));
    clusterConfig.setDefaultInstanceCapacityMap(
        Collections.singletonMap(TEST_CAPACITY_KEY, TEST_CAPACITY_VALUE));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(TEST_CAPACITY_KEY, 1));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  /**
   * Sets up initial resources and mock participants for the cluster.
   */
  private void setupInitResourcesAndParticipants() throws Exception {
    for (int i = 0; i < RESOURCE_COUNT; i++) {
      String dbName = "TestDB_" + i;

      // Create and start participants for the resource
      for (int j = 0; j < INSTANCES_PER_RESOURCE; j++) {
        String participantName = "localhost_" + _nextStartPort;

        InstanceConfig instanceConfig = new InstanceConfig.Builder().setDomain(
                String.format("%s=%s, %s=%s", RACK, j % INIT_ZONE_COUNT, HOST, participantName))
            .addTag(dbName).build(participantName);

        _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, instanceConfig);

        MockParticipantManager participant = createParticipant(participantName);
        participant.syncStart();
        _nextStartPort++;
        _participants.add(participant);
      }

      // Set up IdealState for the resource
      IdealState is = createResourceWithWagedRebalance(CLUSTER_NAME, dbName,
          BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA - 1);
      is.setResourceGroupName(dbName);
      _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, dbName, is);

      _allDBs.add(dbName);
    }
  }

  /**
   * Creates and starts a mock participant with a registered state model factory.
   */
  private MockParticipantManager createParticipant(String participantName) throws Exception {
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, participantName, 10, null);
    participant.getStateMachineEngine()
        .registerStateModelFactory("LeaderStandby", new LeaderStandbyStateModelFactory());
    return participant;
  }

  /**
   * Tests topology migration with and without domain updates, ensuring no shuffling occurs.
   */
  @Test
  public void testTopologyMigrationByResourceGroup() throws Exception {
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Step 1: Migrate to new topology in maintenance mode
    Map<String, ExternalView> originalEVs = getEVs();
    List<InstanceConfig> instanceConfigs =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME).stream().map(
            instanceName -> _gSetupTool.getClusterManagementTool()
                .getInstanceConfig(CLUSTER_NAME, instanceName)).collect(Collectors.toList());

    setAndVerifyMaintenanceMode(true);
    setupClusterConfig(MIGRATED_TOPOLOGY, MZ);
    migrateInstanceConfigTopology(instanceConfigs);
    setAndVerifyMaintenanceMode(false);

    // Verify cluster did not have shuffling anywhere after
    // the migration to the new topology
    validateNoShufflingOccurred(originalEVs, null);

    // Step 2: Update domain values for one resource group at a time
    for (String updatingDb : _allDBs) {
      Map<String, ExternalView> preMigrationEVs = getEVs();
      setAndVerifyMaintenanceMode(true);
      migrateDomainForResourceGroup(updatingDb);
      setAndVerifyMaintenanceMode(false);

      // Verify cluster only had shuffling in the resource group that was updated
      validateNoShufflingOccurred(preMigrationEVs, updatingDb);
    }
  }

  /**
   * Set MaintenanceMode and verify that controller has processed it.
   */
  private void setAndVerifyMaintenanceMode(boolean enable) throws Exception {
    if (enable) {
      // Check that the cluster converged to the best possible state that should be calculated
      // by the controller before we change the maintenance mode.
      Assert.assertTrue(_clusterVerifier.verifyByPolling());
    }

    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, enable, "", Collections.emptyMap());

    if (!enable) {
      // Check that the cluster converged to the best possible state that should be calculated
      // by the controller after we have changed the maintenance mode.
      Assert.assertTrue(_clusterVerifier.verifyByPolling());
    }
  }

  /**
   * Retrieves the ExternalViews for all databases in the cluster.
   */
  private Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<>();
    for (String db : _allDBs) {
      externalViews.put(db,
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db));
    }
    return externalViews;
  }

  /**
   * Compares the contents of two ExternalViews to determine if they are equal.
   */
  private boolean compareExternalViews(ExternalView oldEV, ExternalView newEV) {
    if (oldEV == null || newEV == null) {
      return false;
    }

    Map<String, Map<String, String>> oldEVMap = oldEV.getRecord().getMapFields();
    Map<String, Map<String, String>> newEVMap = newEV.getRecord().getMapFields();

    if (oldEVMap.size() != newEVMap.size()) {
      return false;
    }

    for (String partition : oldEVMap.keySet()) {
      if (!oldEVMap.get(partition).equals(newEVMap.get(partition))) {
        return false;
      }
    }
    return true;
  }

  private void validateNoShufflingOccurred(Map<String, ExternalView> originalEVs,
      String shouldShuffleDB) {
    Map<String, ExternalView> updatedEVs = getEVs();
    for (String db : _allDBs) {
      if (db.equals(shouldShuffleDB)) {
        Assert.assertFalse(compareExternalViews(originalEVs.get(db), updatedEVs.get(db)),
            String.format("Expected shuffling didn't occur for database %s", db));
      } else {
        Assert.assertTrue(compareExternalViews(originalEVs.get(db), updatedEVs.get(db)),
            String.format("Unexpected shuffling occurred for database %s", db));
      }
    }
  }

  private void migrateInstanceConfigTopology(List<InstanceConfig> instanceConfigs)
      throws Exception {

    for (InstanceConfig instanceConfig : instanceConfigs) {
      String rackId = instanceConfig.getDomainAsMap().get(RACK);
      String hostId = instanceConfig.getDomainAsMap().get(HOST);

      // Set new domain based on the new topology format
      String newDomain =
          String.format("%s=%s, %s=%s, %s=%s", MZ, rackId, HOST, hostId, APPLICATION_INSTANCE_ID,
              hostId);
      instanceConfig.setDomain(newDomain);

      // Update the instance configuration in the cluster
      _gSetupTool.getClusterManagementTool()
          .setInstanceConfig(CLUSTER_NAME, instanceConfig.getInstanceName(), instanceConfig);
    }
  }

  private void migrateDomainForResourceGroup(String resourceGroup) throws Exception {
    int instanceIndex = 0;
    for (MockParticipantManager participant : _participants) {
      InstanceConfig instanceConfig = _gSetupTool.getClusterManagementTool()
          .getInstanceConfig(CLUSTER_NAME, participant.getInstanceName());
      if (instanceConfig.containsTag(resourceGroup)) {
        Map<String, String> newDomain = instanceConfig.getDomainAsMap();
        newDomain.put(MZ, String.valueOf(instanceIndex % MIGRATE_ZONE_COUNT));
        newDomain.put(APPLICATION_INSTANCE_ID, UUID.randomUUID().toString());
        instanceConfig.setDomain(newDomain);
        _gSetupTool.getClusterManagementTool()
            .setInstanceConfig(CLUSTER_NAME, participant.getInstanceName(), instanceConfig);
        instanceIndex++;
      }
    }
  }
}
