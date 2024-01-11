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
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestPartitionAssignmentAPIHostAssignment extends AbstractTestClass {
  private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);

  private static final int REPLICAS = 3;
  private static final int MIN_ACTIVE_REPLICAS = 2;
  private static final String INSTANCE_CAPACITY_KEY = "PARTCOUNT";
  private static final int DEFAULT_INSTANCE_COUNT = 4;
  private static final int DEFAULT_INSTANCE_CAPACITY = 50;

  private static final String INSTANCE_NAME_PREFIX = "localhost_";
  private static final int INSTANCE_START_PORT = 12918;
  private static String _clusterName = "PartitionAssignmentTestCluster";
  private static String _urlBase = "clusters/" + _clusterName + "/partitionAssignment/";
  private static ClusterControllerManager _controller;
  private static HelixDataAccessor _helixDataAccessor;
  private static ConfigAccessor _configAccessor;
  private static BestPossibleExternalViewVerifier _clusterVerifier;
  private static List<MockParticipantManager> _participants = new ArrayList<>();
  private static List<String> _resources = new ArrayList<>();

  @BeforeTest
  public void beforeTest() {
    System.out.println("Start setup:" + TestHelper.getTestMethodName());
    // Create test cluster
    _gSetupTool.addCluster(_clusterName, true);

    // Setup cluster configs
    _configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(_clusterName);
    clusterConfig.setPersistBestPossibleAssignment(true);
    clusterConfig.setDefaultInstanceCapacityMap(
        Collections.singletonMap(INSTANCE_CAPACITY_KEY, DEFAULT_INSTANCE_CAPACITY));
    clusterConfig.setInstanceCapacityKeys(List.of(INSTANCE_CAPACITY_KEY));
    _configAccessor.setClusterConfig(_clusterName, clusterConfig);
    _controller = startController(_clusterName);

    // Create HelixDataAccessor
    _helixDataAccessor = new ZKHelixDataAccessor(_clusterName, _baseAccessor);

    // Create cluster verifier
    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkAddr(ZK_ADDR)
        .setResources(new HashSet<>(_resources))
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Add and start instances to cluster
    for (int i = 0; i < DEFAULT_INSTANCE_COUNT; i++) {
      String instanceName = INSTANCE_NAME_PREFIX + (INSTANCE_START_PORT + i);
      InstanceConfig instanceConfig = new InstanceConfig(instanceName);
      instanceConfig.setInstanceEnabled(true);
      instanceConfig.setInstanceCapacityMap(
          Collections.singletonMap(INSTANCE_CAPACITY_KEY, DEFAULT_INSTANCE_CAPACITY));
      _gSetupTool.getClusterManagementTool().addInstance(_clusterName, instanceConfig);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, _clusterName, instanceName);
      participant.syncStart();
      _participants.add(participant);
    }
  }

  @AfterTest
  public void afterTest() throws Exception {
    // Drop all resources
    for (String resource : _resources) {
      _gSetupTool.dropResourceFromCluster(_clusterName, resource);
    }
    _resources.clear();

    // Stop and remove all instances
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
      _gSetupTool.getClusterManagementTool().dropInstance(_clusterName,
          _gSetupTool.getClusterManagementTool()
              .getInstanceConfig(_clusterName, participant.getInstanceName()));
    }

    _participants.clear();

    // Drop cluster
    _gSetupTool.deleteCluster(_clusterName);

    // Stop controller
    _controller.syncStop();
  }

  @Test
  public void testComputePartitionAssignmentAddInstanceCompareReal() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Create 5 WAGED resources
    String wagedResourcePrefix = "TEST_WAGED_DB_";
    int resourceCount = 5;
    for (int i = 0; i < resourceCount; i++) {
      createWagedResource(wagedResourcePrefix + i,
          DEFAULT_INSTANCE_CAPACITY * DEFAULT_INSTANCE_COUNT / REPLICAS / resourceCount, MIN_ACTIVE_REPLICAS,
          100000L);
    }

    // Add Instance to cluster as disabled
    String toAddInstanceName = "dummyInstance";
    InstanceConfig toAddInstanceConfig = new InstanceConfig(toAddInstanceName);
    toAddInstanceConfig.setInstanceCapacityMap(
        Collections.singletonMap(INSTANCE_CAPACITY_KEY, 50));
//    toAddInstanceConfig.setInstanceEnabled(false);
    _gSetupTool.getClusterManagementTool().addInstance(_clusterName, toAddInstanceConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Use partition assignment API to get CurrentStateView for just that instance as active
    String payload = "{\"InstanceChange\" : { \"ActivateInstances\" : [\"" + toAddInstanceName
        + "\"] }, \"Options\" : { \"ReturnFormat\" : \"CurrentStateFormat\", \"InstanceFilter\" : [\""
        + toAddInstanceName + "\"] }}";
    Response response =
        post(_urlBase, null, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE),
            Response.Status.OK.getStatusCode(), true);
    String body = response.readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments =
        OBJECT_MAPPER.readValue(body,
            new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
            });

    // Actually create the live instance
    MockParticipantManager toAddParticipant =
        new MockParticipantManager(ZK_ADDR, _clusterName, toAddInstanceName);
    toAddParticipant.syncStart();
//    _gSetupTool.getClusterManagementTool().enableInstance(cluster, toAddInstanceName, true);
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

  private void createWagedResource(String db, int numPartition, int minActiveReplica, long delay)
      throws IOException {
    _gSetupTool.addResourceToCluster(_clusterName, db, numPartition, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO + "", null);
    _resources.add(db);

    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(_clusterName, db);
    idealState.setMinActiveReplicas(minActiveReplica);
    idealState.setDelayRebalanceEnabled(true);
    idealState.setRebalanceDelay(delay);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(_clusterName, db, idealState);

    ResourceConfig resourceConfig = new ResourceConfig(db);
    Map<String, Map<String, Integer>> capacityMap = new HashMap<>();
    capacityMap.put("DEFAULT", Collections.singletonMap(INSTANCE_CAPACITY_KEY, 1));
    resourceConfig.setPartitionCapacityMap(capacityMap);
    _configAccessor.setResourceConfig(_clusterName, db, resourceConfig);

    _gSetupTool.rebalanceResource(_clusterName, db, REPLICAS);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }
}