package org.apache.helix.controller.changedetector;

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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test contains a series of unit tests for ResourceChangeDetector.
 */
public class TestResourceChangeDetector extends ZkTestBase {

  // All possible change types for ResourceChangeDetector except for ClusterConfig
  // since we don't provide the names of changed fields for ClusterConfig
  private static final ChangeType[] RESOURCE_CHANGE_TYPES =
      {ChangeType.IDEAL_STATE, ChangeType.INSTANCE_CONFIG, ChangeType.LIVE_INSTANCE, ChangeType.RESOURCE_CONFIG, ChangeType.CLUSTER_CONFIG};

  private static final String CLUSTER_NAME = TestHelper.getTestClassName();
  private static final String RESOURCE_NAME = "TestDB";
  private static final String NEW_RESOURCE_NAME = "TestDB2";
  private static final String STATE_MODEL = "MasterSlave";
  // There are 5 possible change types for ResourceChangeDetector
  private static final int NUM_CHANGE_TYPES = 5;
  private static final int NUM_RESOURCES = 1;
  private static final int NUM_PARTITIONS = 10;
  private static final int NUM_REPLICAS = 3;
  private static final int NUM_NODES = 5;

  // Create a mock of ResourceControllerDataProvider so that we could manipulate it
  private ResourceControllerDataProvider _dataProvider;
  private ResourceChangeDetector _resourceChangeDetector;
  private ClusterControllerManager _controller;
  private MockParticipantManager[] _participants = new MockParticipantManager[NUM_NODES];
  private HelixDataAccessor _dataAccessor;
  private PropertyKey.Builder _keyBuilder;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    // Set up a mock cluster
    TestHelper.setupCluster(CLUSTER_NAME, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        RESOURCE_NAME, // resource name prefix
        NUM_RESOURCES, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_NODES, // nodes
        NUM_REPLICAS, // replicas
        STATE_MODEL, true); // do rebalance

    // Start a controller
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "controller_0");
    _controller.syncStart();

    // Start Participants
    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = "localhost_" + (12918 + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }

    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    _keyBuilder = _dataAccessor.keyBuilder();
    _resourceChangeDetector = new ResourceChangeDetector();

    // Create a custom data provider
    _dataProvider = new ResourceControllerDataProvider(CLUSTER_NAME);
  }

  @AfterClass
  public void afterClass() throws Exception {
    for (MockParticipantManager participant : _participants) {
      if (participant != null && participant.isConnected()) {
        participant.syncStop();
      }
    }
    _controller.syncStop();
    deleteCluster(CLUSTER_NAME);
    Assert.assertFalse(TestHelper.verify(() -> _dataAccessor.getBaseDataAccessor()
        .exists("/" + CLUSTER_NAME, AccessOption.PERSISTENT), 20000L));
  }

  /**
   * Tests the initialization of the change detector. It should tell us that there's been changes
   * for every change type and for all items per type.
   * @throws Exception
   */
  @Test
  public void testResourceChangeDetectorInit() {
    _dataProvider.refresh(_dataAccessor);
    _resourceChangeDetector.updateSnapshots(_dataProvider);

    Collection<ChangeType> changeTypes = _resourceChangeDetector.getChangeTypes();
    Assert.assertEquals(changeTypes.size(), NUM_CHANGE_TYPES,
        "Not all change types have been detected for ResourceChangeDetector!");

    // Check that the right amount of resources show up as added
    checkDetectionCounts(_resourceChangeDetector, ChangeType.IDEAL_STATE, NUM_RESOURCES, 0, 0);

    // Check that the right amount of instances show up as added
    checkDetectionCounts(_resourceChangeDetector, ChangeType.LIVE_INSTANCE, NUM_NODES, 0, 0);
    checkDetectionCounts(_resourceChangeDetector, ChangeType.INSTANCE_CONFIG, NUM_NODES, 0, 0);

    // Check that the right amount of cluster config item show up
    checkDetectionCounts(_resourceChangeDetector, ChangeType.CLUSTER_CONFIG, 1, 0, 0);
  }

  /**
   * Add a resource (IS and ResourceConfig) and see if the detector detects it.
   */
  @Test(dependsOnMethods = "testResourceChangeDetectorInit")
  public void testAddResource() {
    // Create an IS and ResourceConfig
    _gSetupTool.getClusterManagementTool()
        .addResource(CLUSTER_NAME, NEW_RESOURCE_NAME, NUM_PARTITIONS, STATE_MODEL);
    ResourceConfig resourceConfig = new ResourceConfig(NEW_RESOURCE_NAME);
    _dataAccessor.setProperty(_keyBuilder.resourceConfig(NEW_RESOURCE_NAME), resourceConfig);
    // Manually notify dataProvider
    _dataProvider.notifyDataChange(ChangeType.IDEAL_STATE);
    _dataProvider.notifyDataChange(ChangeType.RESOURCE_CONFIG);

    // Refresh the data provider
    _dataProvider.refresh(_dataAccessor);

    // Update the detector
    _resourceChangeDetector.updateSnapshots(_dataProvider);

    checkChangeTypes(ChangeType.IDEAL_STATE, ChangeType.RESOURCE_CONFIG);
    // Check the counts
    for (ChangeType type : RESOURCE_CHANGE_TYPES) {
      if (type == ChangeType.IDEAL_STATE || type == ChangeType.RESOURCE_CONFIG) {
        checkDetectionCounts(_resourceChangeDetector, type, 1, 0, 0);
      } else {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 0);
      }
    }
    // Check that detector gives the right item
    Assert.assertTrue(_resourceChangeDetector.getAdditionsByType(ChangeType.RESOURCE_CONFIG)
        .contains(NEW_RESOURCE_NAME));
  }

  /**
   * Modify a resource config for the new resource and test that detector detects it.
   */
  @Test(dependsOnMethods = "testAddResource")
  public void testModifyResource() {
    // Modify resource config
    ResourceConfig resourceConfig =
        _dataAccessor.getProperty(_keyBuilder.resourceConfig(NEW_RESOURCE_NAME));
    resourceConfig.getRecord().setSimpleField("Did I change?", "Yes!");
    _dataAccessor.updateProperty(_keyBuilder.resourceConfig(NEW_RESOURCE_NAME), resourceConfig);

    // Notify data provider and check
    _dataProvider.notifyDataChange(ChangeType.RESOURCE_CONFIG);
    _dataProvider.refresh(_dataAccessor);
    _resourceChangeDetector.updateSnapshots(_dataProvider);

    checkChangeTypes(ChangeType.RESOURCE_CONFIG);
    // Check the counts
    for (ChangeType type : RESOURCE_CHANGE_TYPES) {
      if (type == ChangeType.RESOURCE_CONFIG) {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 1, 0);
      } else {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 0);
      }
    }
    Assert.assertTrue(_resourceChangeDetector.getChangesByType(ChangeType.RESOURCE_CONFIG)
        .contains(NEW_RESOURCE_NAME));
  }

  /**
   * Delete the new resource and test that detector detects it.
   */
  @Test(dependsOnMethods = "testModifyResource")
  public void testDeleteResource() {
    // Delete the newly added resource
    _dataAccessor.removeProperty(_keyBuilder.idealStates(NEW_RESOURCE_NAME));
    _dataAccessor.removeProperty(_keyBuilder.resourceConfig(NEW_RESOURCE_NAME));

    // Notify data provider and check
    _dataProvider.notifyDataChange(ChangeType.IDEAL_STATE);
    _dataProvider.notifyDataChange(ChangeType.RESOURCE_CONFIG);
    _dataProvider.refresh(_dataAccessor);
    _resourceChangeDetector.updateSnapshots(_dataProvider);

    checkChangeTypes(ChangeType.RESOURCE_CONFIG, ChangeType.IDEAL_STATE);
    // Check the counts
    for (ChangeType type : RESOURCE_CHANGE_TYPES) {
      if (type == ChangeType.IDEAL_STATE || type == ChangeType.RESOURCE_CONFIG) {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 1);
      } else {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 0);
      }
    }
  }

  /**
   * Disconnect and reconnect a Participant and see if detector detects.
   */
  @Test(dependsOnMethods = "testDeleteResource")
  public void testDisconnectReconnectInstance() {
    // Disconnect a Participant
    _participants[0].syncStop();
    _dataProvider.notifyDataChange(ChangeType.LIVE_INSTANCE);
    _dataProvider.refresh(_dataAccessor);
    _resourceChangeDetector.updateSnapshots(_dataProvider);

    checkChangeTypes(ChangeType.LIVE_INSTANCE);
    // Check the counts
    for (ChangeType type : RESOURCE_CHANGE_TYPES) {
      if (type == ChangeType.LIVE_INSTANCE) {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 1);
      } else {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 0);
      }
    }

    // Reconnect the Participant
    _participants[0] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, "localhost_12918");
    _participants[0].syncStart();
    _dataProvider.notifyDataChange(ChangeType.LIVE_INSTANCE);
    _dataProvider.refresh(_dataAccessor);
    _resourceChangeDetector.updateSnapshots(_dataProvider);

    checkChangeTypes(ChangeType.LIVE_INSTANCE);
    // Check the counts
    for (ChangeType type : RESOURCE_CHANGE_TYPES) {
      if (type == ChangeType.LIVE_INSTANCE) {
        checkDetectionCounts(_resourceChangeDetector, type, 1, 0, 0);
      } else {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 0);
      }
    }
  }

  /**
   * Remove an instance completely and see if detector detects.
   */
  @Test(dependsOnMethods = "testDisconnectReconnectInstance")
  public void testRemoveInstance() throws Exception {
    String instanceName = _participants[0].getInstanceName();
    _participants[0].syncStop();
    InstanceConfig instanceConfig =
        _dataAccessor.getProperty(_keyBuilder.instanceConfig(_participants[0].getInstanceName()));
    _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, instanceConfig);
    // Verify that instance has been removed
    Assert.assertTrue(TestHelper.verify(() -> _dataAccessor
        .getProperty(_dataAccessor.keyBuilder().instance(_participants[0].getInstanceName()))
        == null, TestHelper.WAIT_DURATION));

    _dataProvider.notifyDataChange(ChangeType.LIVE_INSTANCE);
    _dataProvider.notifyDataChange(ChangeType.INSTANCE_CONFIG);
    _dataProvider.refresh(_dataAccessor);
    _resourceChangeDetector.updateSnapshots(_dataProvider);

    checkChangeTypes(ChangeType.LIVE_INSTANCE, ChangeType.INSTANCE_CONFIG);
    // Check the counts
    for (ChangeType type : RESOURCE_CHANGE_TYPES) {
      if (type == ChangeType.LIVE_INSTANCE || type == ChangeType.INSTANCE_CONFIG) {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 1);
      } else {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 0);
      }
    }

    // recovery the environment
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
    _participants[0] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    _participants[0].syncStart();
  }

  /**
   * Modify cluster config and see if detector detects.
   */
  @Test(dependsOnMethods = "testRemoveInstance")
  public void testModifyClusterConfig() {
    // Modify cluster config
    ClusterConfig clusterConfig = _dataAccessor.getProperty(_keyBuilder.clusterConfig());
    clusterConfig.setTopology("Change");
    _dataAccessor.updateProperty(_keyBuilder.clusterConfig(), clusterConfig);

    _dataProvider.notifyDataChange(ChangeType.CLUSTER_CONFIG);
    _dataProvider.refresh(_dataAccessor);
    _resourceChangeDetector.updateSnapshots(_dataProvider);

    checkChangeTypes(ChangeType.CLUSTER_CONFIG);
    // Check the counts for other types
    for (ChangeType type : RESOURCE_CHANGE_TYPES) {
      if (type == ChangeType.CLUSTER_CONFIG) {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 1, 0);
      } else {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 0);
      }
    }
  }

  /**
   * Test that change detector gives correct results when there are no changes after updating
   * snapshots.
   */
  @Test(dependsOnMethods = "testModifyClusterConfig")
  public void testNoChange() {
    // Test twice to make sure that no change is stable across different runs
    for (int i = 0; i < 2; i++) {
      _dataProvider.refresh(_dataAccessor);
      _resourceChangeDetector.updateSnapshots(_dataProvider);

      Assert.assertEquals(_resourceChangeDetector.getChangeTypes().size(), 0);
      // Check the counts for all the other types
      for (ChangeType type : RESOURCE_CHANGE_TYPES) {
        checkDetectionCounts(_resourceChangeDetector, type, 0, 0, 0);
      }
    }
  }

  /**
   * Modify IdealState mapping fields for a FULL_AUTO resource and see if detector detects.
   */
  @Test(dependsOnMethods = "testNoChange")
  public void testIgnoreNonTopologyChanges() {
    // Modify cluster config and IdealState to ensure the mapping field of the IdealState will be
    // considered as the fields that are modified by Helix logic.
    ClusterConfig clusterConfig = _dataAccessor.getProperty(_keyBuilder.clusterConfig());
    clusterConfig.setPersistBestPossibleAssignment(true);
    _dataAccessor.updateProperty(_keyBuilder.clusterConfig(), clusterConfig);

    // Create an new IS
    String resourceName = "Resource" + TestHelper.getTestMethodName();
    _gSetupTool.getClusterManagementTool()
        .addResource(CLUSTER_NAME, resourceName, NUM_PARTITIONS, STATE_MODEL);
    IdealState idealState = _dataAccessor.getProperty(_keyBuilder.idealStates(resourceName));
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.getRecord().getMapFields().put("Partition1", new HashMap<>());
    _dataAccessor.updateProperty(_keyBuilder.idealStates(resourceName), idealState);
    Arrays.stream(ChangeType.values()).forEach(type -> {
      _dataProvider.notifyDataChange(type);
    });
    _dataProvider.refresh(_dataAccessor);

    // Test with ignore option to be true
    ResourceChangeDetector changeDetector = new ResourceChangeDetector(true);
    changeDetector.updateSnapshots(_dataProvider);

    // 1. Modify ideal state map fields of a FULL_AUTO resource
    idealState.getRecord().getMapFields().put("Partition1", Collections.singletonMap("foo", "bar"));
    _dataAccessor.updateProperty(_keyBuilder.idealStates(resourceName), idealState);
    _dataProvider.notifyDataChange(ChangeType.IDEAL_STATE);
    _dataProvider.refresh(_dataAccessor);
    changeDetector.updateSnapshots(_dataProvider);
    Assert.assertEquals(changeDetector.getChangeTypes(),
        Collections.singleton(ChangeType.IDEAL_STATE));
    checkDetectionCounts(changeDetector, ChangeType.IDEAL_STATE, 0, 0, 0);

    // 2. Modify an instance "enabled" state
    String instanceName = _participants[0].getInstanceName();
    InstanceConfig instanceConfig =
        _dataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
    Assert.assertTrue(instanceConfig.getInstanceEnabled());
    try {
      instanceConfig.setInstanceEnabled(false);
      _dataAccessor.updateProperty(_keyBuilder.instanceConfig(instanceName), instanceConfig);
      _dataProvider.notifyDataChange(ChangeType.INSTANCE_CONFIG);
      _dataProvider.refresh(_dataAccessor);
      changeDetector.updateSnapshots(_dataProvider);
      Assert.assertEquals(changeDetector.getChangeTypes(),
          Collections.singleton(ChangeType.INSTANCE_CONFIG));
      checkDetectionCounts(changeDetector, ChangeType.INSTANCE_CONFIG, 0, 0, 0);
    } finally {
      // remove newly added resource/ideastate
      _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, resourceName);
      instanceConfig.setInstanceEnabled(true);
      _dataAccessor.updateProperty(_keyBuilder.instanceConfig(instanceName), instanceConfig);
    }
  }

  @Test(dependsOnMethods = "testIgnoreNonTopologyChanges")
  public void testResetSnapshots() {
    // ensure the cluster converged before the test to ensure IS is not modified unexpectedly
    HelixClusterVerifier _clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setDeactivatedNodeAwareness(true)
            .setResources(new HashSet<>(_dataAccessor.getChildNames(_keyBuilder.idealStates())))
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
    try {
      Assert.assertTrue(_clusterVerifier.verify());
    } finally {
      _clusterVerifier.close();
    }
    // Initialize a new detector with the existing data
    ResourceChangeDetector changeDetector = new ResourceChangeDetector();
    _dataProvider.notifyDataChange(ChangeType.IDEAL_STATE);
    _dataProvider.refresh(_dataAccessor);
    changeDetector.updateSnapshots(_dataProvider);
    checkDetectionCounts(changeDetector, ChangeType.IDEAL_STATE, 1, 0, 0);

    // Update the detector with old data, since nothing changed, the result shall be empty.
    changeDetector.updateSnapshots(_dataProvider);
    checkDetectionCounts(changeDetector, ChangeType.IDEAL_STATE, 0, 0, 0);

    // Reset the snapshots
    changeDetector.resetSnapshots();
    // After reset, all the data in the data provider will be treated as new changes
    changeDetector.updateSnapshots(_dataProvider);
    checkDetectionCounts(changeDetector, ChangeType.IDEAL_STATE, 1, 0, 0);
  }

  /**
   * Check that the given change types appear in detector's change types.
   * @param types
   */
  private void checkChangeTypes(ChangeType... types) {
    for (ChangeType type : types) {
      Assert.assertTrue(_resourceChangeDetector.getChangeTypes().contains(type));
    }
  }

  /**
   * Convenience method for checking three types of detections.
   * @param changeType
   * @param additions
   * @param changes
   * @param deletions
   */
  private void checkDetectionCounts(ChangeDetector changeDetector, ChangeType changeType,
      int additions, int changes, int deletions) {
    Assert.assertEquals(changeDetector.getAdditionsByType(changeType).size(), additions);
    Assert.assertEquals(changeDetector.getChangesByType(changeType).size(), changes);
    Assert.assertEquals(changeDetector.getRemovalsByType(changeType).size(), deletions);
  }
}
