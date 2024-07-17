package org.apache.helix.rest.server;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestPartitionAssignmentAPI extends AbstractTestClass {
  private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);

  private static final int REPLICAS = 3;
  private static final int MIN_ACTIVE_REPLICAS = 2;
  private static final String INSTANCE_CAPACITY_KEY = "PARTCOUNT";
  private static final int DEFAULT_INSTANCE_COUNT = 4;
  private static final int DEFAULT_INSTANCE_CAPACITY = 50;

  private static final String INSTANCE_NAME_PREFIX = "localhost_";
  private static final int INSTANCE_START_PORT = 12918;
  private static final String PARTITION_ASSIGNMENT_PATH_TEMPLATE =
      "/clusters/%s/partitionAssignment/";
  private static final String CLUSTER_NAME = "PartitionAssignmentTestCluster";
  private static ClusterControllerManager _controller;
  private static HelixDataAccessor _helixDataAccessor;
  private static ConfigAccessor _configAccessor;
  private static BestPossibleExternalViewVerifier _clusterVerifier;
  private static List<MockParticipantManager> _participants = new ArrayList<>();
  private static List<String> _resources = new ArrayList<>();

  @BeforeMethod
  public void beforeTest() {
    System.out.println("Start setup:" + TestHelper.getTestMethodName());
    // Create test cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    // Setup cluster configs
    _configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setPersistBestPossibleAssignment(true);
    clusterConfig.setDefaultInstanceCapacityMap(
        Collections.singletonMap(INSTANCE_CAPACITY_KEY, DEFAULT_INSTANCE_CAPACITY));
    clusterConfig.setInstanceCapacityKeys(ImmutableList.of(INSTANCE_CAPACITY_KEY));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    _controller = startController(CLUSTER_NAME);

    // Create HelixDataAccessor
    _helixDataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);

    // Create cluster verifier
    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setResources(new HashSet<>(_resources))
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Add and start instances to cluster
    for (int i = 0; i < DEFAULT_INSTANCE_COUNT; i++) {
      String instanceName = INSTANCE_NAME_PREFIX + (INSTANCE_START_PORT + i);
      InstanceConfig instanceConfig = new InstanceConfig(instanceName);
      instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
      instanceConfig.setInstanceCapacityMap(
          Collections.singletonMap(INSTANCE_CAPACITY_KEY, DEFAULT_INSTANCE_CAPACITY));
      _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, instanceConfig);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      participant.syncStart();
      _participants.add(participant);
    }

    System.out.println("End setup:" + TestHelper.getTestMethodName());
  }

  @AfterMethod
  public void afterTest() throws Exception {
    System.out.println("Start teardown:" + TestHelper.getTestMethodName());

    // Drop all resources
    for (String resource : _resources) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, resource);
    }
    _resources.clear();

    // Stop and remove all instances
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
      InstanceConfig instanceConfig = _helixDataAccessor.getProperty(
          _helixDataAccessor.keyBuilder().instanceConfig(participant.getInstanceName()));
      if (instanceConfig != null) {
        _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, instanceConfig);
      }
    }
    _participants.clear();

    // Stop controller
    _controller.syncStop();

    // Drop cluster
    _gSetupTool.deleteCluster(CLUSTER_NAME);

    System.out.println("End teardown:" + TestHelper.getTestMethodName());
  }

  @Test
  public void testComputePartitionAssignmentAddInstance() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Create 5 WAGED resources
    String wagedResourcePrefix = "TEST_WAGED_DB_";
    int resourceCount = 5;
    for (int i = 0; i < resourceCount; i++) {
      createWagedResource(wagedResourcePrefix + i,
          DEFAULT_INSTANCE_CAPACITY * DEFAULT_INSTANCE_COUNT / REPLICAS / resourceCount,
          MIN_ACTIVE_REPLICAS, 100000L);
    }

    // Add Instance to cluster as disabled
    String toAddInstanceName = "dummyInstance";
    InstanceConfig toAddInstanceConfig = new InstanceConfig(toAddInstanceName);
    toAddInstanceConfig.setInstanceCapacityMap(
        Collections.singletonMap(INSTANCE_CAPACITY_KEY, DEFAULT_INSTANCE_CAPACITY));
    _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, toAddInstanceConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Use partition assignment API to get CurrentStateView for just that instance as active
    String payload = "{\"InstanceChange\" : { \"ActivateInstances\" : [\"" + toAddInstanceName
        + "\"] }, \"Options\" : { \"ReturnFormat\" : \"CurrentStateFormat\", \"InstanceFilter\" : [\""
        + toAddInstanceName + "\"] }}";
    Response response = post(getPartitionAssignmentPath(), null,
        Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode(),
        true);
    String body = response.readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments =
        OBJECT_MAPPER.readValue(body,
            new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
            });

    // Actually create the live instance
    MockParticipantManager toAddParticipant = createParticipant(toAddInstanceName);
    toAddParticipant.syncStart();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Get the current state of the instance
    // Compare the current state of the instance to CS previously returned by partition assignment API
    LiveInstance liveInstance = _helixDataAccessor.getProperty(
        _helixDataAccessor.keyBuilder().liveInstance(toAddInstanceName));
    String liveSession = liveInstance.getEphemeralOwner();
    Assert.assertTrue(TestHelper.verify(() -> {
      try {
        Map<String, Set<String>> instanceResourceMap =
            resourceAssignments.getOrDefault(toAddInstanceName, Collections.emptyMap()).entrySet()
                .stream().collect(HashMap::new,
                    (resourceMap, entry) -> resourceMap.put(entry.getKey(), entry.getValue().keySet()),
                    HashMap::putAll);
        Map<String, Set<String>> instanceResourceMapCurrentState = new HashMap<>();
        for (String resource : _resources) {
          CurrentState currentState = _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder()
              .currentState(toAddInstanceName, liveSession, resource));
          instanceResourceMapCurrentState.put(resource,
              currentState != null ? currentState.getPartitionStateMap().keySet()
                  : Collections.emptySet());
        }
        Assert.assertEquals(instanceResourceMapCurrentState, instanceResourceMap);
      } catch (AssertionError e) {
        LOG.error("Current state does not match partition assignment", e);
        return false;
      }
      return true;
    }, 30000));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testComputePartitionAssignmentReplaceInstance() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Create 5 WAGED resources
    String wagedResourcePrefix = "TEST_WAGED_DB_";
    int resourceCount = 5;
    for (int i = 0; i < resourceCount; i++) {
      createWagedResource(wagedResourcePrefix + i,
          (DEFAULT_INSTANCE_CAPACITY * 2 / 3) * (DEFAULT_INSTANCE_COUNT - 1) / REPLICAS / resourceCount,
          MIN_ACTIVE_REPLICAS, 1000L);
    }

    // Kill an instance to simulate a dead instance and wait longer than delay window to simulate delayed rebalance
    MockParticipantManager deadParticipant = _participants.get(0);
    deadParticipant.syncStop();
    Thread.sleep(3000L);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Enter the cluster into MM
    _gSetupTool.getClusterManagementTool()
        .enableMaintenanceMode(CLUSTER_NAME, true, "BHP enters MM.");

    // Drop the dead instance from the cluster
    _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME,
        _gSetupTool.getClusterManagementTool()
            .getInstanceConfig(CLUSTER_NAME, deadParticipant.getInstanceName()));

    // Add new instance to cluster
    String toAddInstanceName = "dummyInstance";
    InstanceConfig toAddInstanceConfig = new InstanceConfig(toAddInstanceName);
    toAddInstanceConfig.setInstanceCapacityMap(
        Collections.singletonMap(INSTANCE_CAPACITY_KEY, DEFAULT_INSTANCE_CAPACITY));
    _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, toAddInstanceConfig);

    // Disable the added instance
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, toAddInstanceName, false);

    // Start the added instance
    MockParticipantManager toAddParticipant = createParticipant(toAddInstanceName);
    toAddParticipant.syncStart();

    // Exit the cluster from MM
    _gSetupTool.getClusterManagementTool()
        .enableMaintenanceMode(CLUSTER_NAME, false, "BHP exits MM.");
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Use partition assignment API to get CurrentStateView for just that instance as active
    String payload = "{\"InstanceChange\" : { \"ActivateInstances\" : [\"" + toAddInstanceName
        + "\"] }, \"Options\" : { \"ReturnFormat\" : \"CurrentStateFormat\", \"InstanceFilter\" : [\""
        + toAddInstanceName + "\"] }}";
    Response response = post(getPartitionAssignmentPath(), null,
        Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode(),
        true);
    String body = response.readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments =
        OBJECT_MAPPER.readValue(body,
            new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
            });

    // Simulate workflow doing work based off of partition assignment API result
    Thread.sleep(5000L);

    // Enable the instance
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, toAddInstanceName, true);

    // Wait for the cluster to converge
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Get the current state of the instance
    // Compare the current state of the instance to CS previously returned by partition assignment API
    LiveInstance liveInstance = _helixDataAccessor.getProperty(
        _helixDataAccessor.keyBuilder().liveInstance(toAddInstanceName));
    String liveSession = liveInstance.getEphemeralOwner();
    Assert.assertTrue(TestHelper.verify(() -> {
      try {
        Map<String, Set<String>> instanceResourceMap =
            resourceAssignments.getOrDefault(toAddInstanceName, Collections.emptyMap()).entrySet()
                .stream().collect(HashMap::new,
                    (resourceMap, entry) -> resourceMap.put(entry.getKey(), entry.getValue().keySet()),
                    HashMap::putAll);
        Map<String, Set<String>> instanceResourceMapCurrentState = new HashMap<>();
        for (String resource : _resources) {
          CurrentState currentState = _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder()
              .currentState(toAddInstanceName, liveSession, resource));
          instanceResourceMapCurrentState.put(resource,
              currentState != null ? currentState.getPartitionStateMap().keySet()
                  : Collections.emptySet());
        }
        Assert.assertEquals(instanceResourceMapCurrentState, instanceResourceMap);
      } catch (AssertionError e) {
        LOG.error("Current state does not match partition assignment", e);
        return false;
      }
      return true;
    }, 30000));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private String getPartitionAssignmentPath() {
    return String.format(PARTITION_ASSIGNMENT_PATH_TEMPLATE, CLUSTER_NAME);
  }

  private MockParticipantManager createParticipant(String instanceName) {
    MockParticipantManager toAddParticipant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    _participants.add(toAddParticipant);
    return toAddParticipant;
  }

  private void createWagedResource(String db, int numPartition, int minActiveReplica, long delay)
      throws IOException {
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, numPartition, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO + "", null);
    _resources.add(db);

    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    idealState.setMinActiveReplicas(minActiveReplica);
    idealState.setDelayRebalanceEnabled(true);
    idealState.setRebalanceDelay(delay);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, idealState);

    ResourceConfig resourceConfig = new ResourceConfig(db);
    Map<String, Map<String, Integer>> capacityMap = new HashMap<>();
    capacityMap.put("DEFAULT", Collections.singletonMap(INSTANCE_CAPACITY_KEY, 1));
    resourceConfig.setPartitionCapacityMap(capacityMap);
    _configAccessor.setResourceConfig(CLUSTER_NAME, db, resourceConfig);

    _gSetupTool.rebalanceResource(CLUSTER_NAME, db, REPLICAS);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private void createCrushedResource(String db, int numPartition, int minActiveReplica, long delay) {
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, numPartition, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO + "", null);
    _resources.add(db);

    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    idealState.setMinActiveReplicas(minActiveReplica);
    idealState.setDelayRebalanceEnabled(true);
    idealState.setRebalanceDelay(delay);
    idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
    idealState.setRebalanceStrategy(CrushEdRebalanceStrategy.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, idealState);

    ResourceConfig resourceConfig = new ResourceConfig(db);
    _configAccessor.setResourceConfig(CLUSTER_NAME, db, resourceConfig);
    _gSetupTool.rebalanceResource(CLUSTER_NAME, db, REPLICAS);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  private void createAutoRebalanceResource(String db) {
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, 1, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO + "", null);
    _resources.add(db);

    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);

    idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
    idealState.setRebalanceStrategy(AutoRebalanceStrategy.class.getName());
    idealState.setReplicas("ANY_LIVEINSTANCE");
    idealState.enable(true);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, idealState);

    ResourceConfig resourceConfig = new ResourceConfig(db);
    _configAccessor.setResourceConfig(CLUSTER_NAME, db, resourceConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @Test
  private void testComputePartitionAssignmentMaintenanceMode() throws Exception {

    // Create 5 WAGED resources
    String wagedResourcePrefix = "TEST_WAGED_DB_";
    int wagedResourceCount = 5;
    for (int i = 0; i < wagedResourceCount; i++) {
      createWagedResource(wagedResourcePrefix + i,
          DEFAULT_INSTANCE_CAPACITY * DEFAULT_INSTANCE_COUNT / REPLICAS / wagedResourceCount,
          MIN_ACTIVE_REPLICAS, 100000L);
    }

    String crushedResourcePrefix = "TEST_CRUSHED_DB_";
    int crushedResourceCount = 3;
    for (int i = 0; i < crushedResourceCount; i++) {
      createCrushedResource(crushedResourcePrefix + i,
          DEFAULT_INSTANCE_CAPACITY * DEFAULT_INSTANCE_COUNT / REPLICAS / crushedResourceCount,
          MIN_ACTIVE_REPLICA, 100000L);
    }

    createAutoRebalanceResource("TEST_AUTOREBALANCE_DB_0");

    // Wait for cluster to converge after adding resources
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Enter the cluster into MM
    _gSetupTool.getClusterManagementTool()
        .enableMaintenanceMode(CLUSTER_NAME, true,
            "testComputePartitionAssignmentMaintenanceMode enters cluster into MM.");

    // Add instance to cluster as enabled
    String toAddInstanceName = "dummyInstance";
    InstanceConfig toAddInstanceConfig = new InstanceConfig(toAddInstanceName);
    toAddInstanceConfig.setInstanceCapacityMap(
        Collections.singletonMap(INSTANCE_CAPACITY_KEY, DEFAULT_INSTANCE_CAPACITY));
    _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, toAddInstanceConfig);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, toAddInstanceName, true);

    // Actually create the live instance
    MockParticipantManager toAddParticipant = createParticipant(toAddInstanceName);
    toAddParticipant.syncStart();

    // Choose participant to simulate killing in API call
    MockParticipantManager participantToKill = _participants.get(0);

    // Use partition assignment API to simulate adding and killing separate instances
    // returns idealStates for all resources in cluster
    String payload = "{\"InstanceChange\" : { \"ActivateInstances\" : [\"" + toAddInstanceName
        + "\"], \"DeactivateInstances\" : [\"" + participantToKill.getInstanceName() + "\"] }, "
        + "\"Options\" : { \"ReturnFormat\" : \"IdealStateFormat\" }}";
    Response response = post(getPartitionAssignmentPath(), null,
        Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode(),
        true);
    String body = response.readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> partitionAssignmentIdealStates =
        OBJECT_MAPPER.readValue(body,
            // Map of resources --> map of partitions --> MAP of instances --> state
            new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
            });

    // Kill the instance we simulated in partitionAssignment API and wait for delay window
    participantToKill.syncStop();
    Thread.sleep(3000L);

    // Exit the cluster from MM
    _gSetupTool.getClusterManagementTool()
        .enableMaintenanceMode(CLUSTER_NAME, false,
            "testComputePartitionAssignmentMaintenanceMode exits cluster out of MM.");

    // Wait for cluster to converge after exiting maintenance mode
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Assert that all resource idealStates calculated by the partitionAssignment API during MM
    // is identical to the idealStates calculated by the controller after it exits MM.
    Assert.assertTrue(TestHelper.verify(() -> {
      try {
        Map<String, Map<String, Map<String, String>>> idealStatesMap = new HashMap<>();


        for (String resource : _resources) {
          IdealState idealState = _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().idealStates(resource));
          idealStatesMap.put(resource, idealState.getRecord().getMapFields());
        }
        Assert.assertEquals(partitionAssignmentIdealStates, idealStatesMap);
      } catch (AssertionError e) {
        LOG.error("Ideal state does not match partition assignment", e);
        return false;
      }
      return true;
    }, 30000));
  }
}
