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
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPartitionAssignmentAPIHostAssignment extends AbstractTestClass {
  private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);

  String cluster = "StoppableTestCluster";
  String urlBase = "clusters/StoppableTestCluster/partitionAssignment/";
  String toEnabledInstance;
  HelixDataAccessor helixDataAccessor;
  List<String> resources;
  List<String> liveInstances;
  BestPossibleExternalViewVerifier _clusterVerifier;

  @BeforeClass
  public void beforeClass() {
    helixDataAccessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    resources = _gSetupTool.getClusterManagementTool().getResourcesInCluster(cluster);
    liveInstances = helixDataAccessor.getChildNames(helixDataAccessor.keyBuilder().liveInstances());
    Assert.assertFalse(resources.isEmpty() || liveInstances.isEmpty());

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(cluster);
    clusterConfig.setPersistBestPossibleAssignment(true);
    configAccessor.setClusterConfig(cluster, clusterConfig);

    // set all resource to FULL_AUTO
    for (String resource : resources) {
//      IdealState idealState =
//          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
//      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
//      idealState.setDelayRebalanceEnabled(true);
//      idealState.setRebalanceDelay(360000);
//      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, resource, idealState);
      _gSetupTool.dropResourceFromCluster(cluster, resource);
    }

    resources = _gSetupTool.getClusterManagementTool().getResourcesInCluster(cluster);

    // 5 WAGED resources
    String wagedResourcePrefix = "TEST_WAGED_DB_";
    int numPartition = 10;
    int minActiveReplica = 2;
    long delay = 100000L;

    for (int i = 0; i < 5; i++) {
      String db = wagedResourcePrefix + i;
      // Add a few waged resources.
      _gSetupTool.addResourceToCluster(cluster, db, numPartition, "LeaderStandby",
          IdealState.RebalanceMode.FULL_AUTO + "", null);
      resources.add(db);
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, db);
      idealState.setMinActiveReplicas(minActiveReplica);
      idealState.setDelayRebalanceEnabled(true);
      idealState.setRebalanceDelay(delay);
      idealState.setRebalancerClassName(WagedRebalancer.class.getName());
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, db, idealState);
      _gSetupTool.rebalanceStorageCluster(cluster, db, 3);
    }

    // Create cluster verifier
    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(cluster).setZkAddr(ZK_ADDR)
        .setResources(new HashSet<>(resources))
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @AfterClass
  public void afterClass() {
    for (String resource : resources) {
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, resource, idealState);
    }
    InstanceConfig config =
        _gSetupTool.getClusterManagementTool().getInstanceConfig(cluster, toEnabledInstance);
    config.setInstanceEnabled(true);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(cluster, toEnabledInstance, config);
    _gSetupTool.getClusterManagementTool()
        .enableMaintenanceMode(cluster, false, TestHelper.getTestMethodName());
  }

  @Test
  public void testComputePartitionAssignmentAddInstanceCompareReal() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Add Instance to cluster as disabled
    String toAddInstanceName = "dummyInstance_localhost_12931";
    InstanceConfig toAddInstanceConfig = new InstanceConfig(toAddInstanceName);
    toAddInstanceConfig.setDomain("helixZoneId=zone16,host=" + toAddInstanceName);
//    toAddInstanceConfig.setInstanceEnabled(false);
    _gSetupTool.getClusterManagementTool().addInstance(cluster, toAddInstanceConfig);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Use partition assignment API to get CurrentStateView for just that instance as active
    String payload = "{\"InstanceChange\" : { \"ActivateInstances\" : [\"" + toAddInstanceName
        + "\"] }, \"Options\" : { \"ReturnFormat\" : \"CurrentStateFormat\", \"InstanceFilter\" : [\""
        + toAddInstanceName + "\"] }}";
    Response response = post(urlBase, null, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true);
    String body = response.readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments =
        OBJECT_MAPPER.readValue(body,
            new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
            });

    // Actually create the live instance
    MockParticipantManager toAddParticipant =
        new MockParticipantManager(ZK_ADDR, cluster, toAddInstanceName);
    toAddParticipant.syncStart();
//    _gSetupTool.getClusterManagementTool().enableInstance(cluster, toAddInstanceName, true);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Get the current state of the instance
    // Compare the current state of the instance to CS previously returned by partition assignment API
    LiveInstance liveInstance = helixDataAccessor.getProperty(
        helixDataAccessor.keyBuilder().liveInstance(toAddInstanceName));
    String liveSession = liveInstance.getEphemeralOwner();
    Assert.assertTrue(TestHelper.verify(() -> {
      try {
        Map<String, Set<String>> instanceResourceMap =
            resourceAssignments.getOrDefault(toAddInstanceName, Collections.emptyMap()).entrySet()
                .stream().collect(HashMap::new,
                    (resourceMap, entry) -> resourceMap.put(entry.getKey(), entry.getValue().keySet()),
                    HashMap::putAll);
        Map<String, Set<String>> instanceResourceMapCurrentState = new HashMap<>();
        for (String resource : resources) {
          CurrentState currentState = helixDataAccessor.getProperty(helixDataAccessor.keyBuilder()
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
}
