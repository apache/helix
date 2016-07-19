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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.ConfigAccessor;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestRebalancerPersistAssignments extends ZkStandAloneCMTestBase {
  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);
    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }
  }

  @DataProvider(name = "rebalanceModes")
  public static RebalanceMode [][] rebalanceModes() {
    return new RebalanceMode[][] { {RebalanceMode.FULL_AUTO},
        {RebalanceMode.SEMI_AUTO}
    };
  }

  @Test(dataProvider = "rebalanceModes")
  public void testAutoRebalanceWithPersistAssignmentEnable(RebalanceMode rebalanceMode)
      throws Exception {
    String testDb = "TestDB1-" + rebalanceMode.name();
    enablePersistAssignment(true);

    _setupTool.addResourceToCluster(CLUSTER_NAME, testDb, 30,
        BuiltInStateModelDefinitions.MasterSlave.name(), rebalanceMode.name());
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, 3);

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    IdealState idealState =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, testDb);
    verifyAssignmentInIdealStateWithPersistEnabled(idealState, new HashSet<String>());

    // kill 1 node
    _participants[0].syncStop();

    result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    idealState = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, testDb);
    // verify that IdealState contains updated assignment in it map fields.

    Set<String> excludedInstances = new HashSet<String>();
    excludedInstances.add(_participants[0].getInstanceName());
    verifyAssignmentInIdealStateWithPersistEnabled(idealState, excludedInstances);

    _setupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, testDb);
  }

  @Test(dataProvider = "rebalanceModes")
  public void testAutoRebalanceWithPersistAssignmentDisabled(RebalanceMode rebalanceMode)
      throws Exception {
    String testDb = "TestDB2-" + rebalanceMode.name();
    _setupTool.addResourceToCluster(CLUSTER_NAME, testDb, 30,
        BuiltInStateModelDefinitions.MasterSlave.name(), rebalanceMode.name());
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, testDb, 3);

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    // kill 1 node
    _participants[0].syncStop();

    result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    IdealState idealState =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, testDb);

    Set<String> excludedInstances = new HashSet<String>();
    excludedInstances.add(_participants[0].getInstanceName());
    Thread.sleep(2000);
    verifyAssignmentInIdealStateWithPersistDisabled(idealState, excludedInstances);

    _setupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, testDb);
  }

  private void enablePersistAssignment(Boolean enable) {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    HelixConfigScope clusterScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
            .forCluster(CLUSTER_NAME).build();

    configAccessor.set(clusterScope,
        ClusterConfig.ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.name(),
        enable.toString());
  }

  // verify that the disabled or failed instance should not be included in bestPossible assignment.
  private void verifyAssignmentInIdealStateWithPersistEnabled(IdealState idealState,
      Set<String> excludedInstances) {
    for (String partition : idealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(partition);
      Assert.assertNotNull(instanceStateMap);
      Assert.assertFalse(instanceStateMap.isEmpty());

      Set<String> instancesInMap = instanceStateMap.keySet();
      Set<String> instanceInList = idealState.getInstanceSet(partition);
      Assert.assertTrue(instanceInList.containsAll(instancesInMap));

      for (String ins : excludedInstances) {
        Assert.assertFalse(instancesInMap.contains(ins));
      }
    }
  }

  // verify that the bestPossible assignment should be empty or should not be changed.
  private void verifyAssignmentInIdealStateWithPersistDisabled(IdealState idealState,
      Set<String> excludedInstances) {
    boolean mapFieldEmpty = true;
    boolean assignmentNotChanged = false;
    for (String partition : idealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(partition);
      if (instanceStateMap == null || instanceStateMap.isEmpty()) {
        continue;
      }
      mapFieldEmpty = false;
      Set<String> instancesInMap = instanceStateMap.keySet();
      for (String ins : excludedInstances) {
        if(instancesInMap.contains(ins)) {
          // if at least one excluded instance is included, it means assignment was not updated.
          assignmentNotChanged = true;
        }
      }
    }

    Assert.assertTrue((mapFieldEmpty || assignmentNotChanged),
        "BestPossible assignment was updated.");
  }
}
