package org.apache.helix.tools;

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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Arrays;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.SleepTransition;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestClusterVerifier extends ZkUnitTestBase {
  final String[] RESOURCES = {
      "resource_semi_MasterSlave", "resource_semi_OnlineOffline",
      "resource_full_MasterSlave", "resource_full_OnlineOffline"
  };
  final String[] SEMI_AUTO_RESOURCES = {RESOURCES[0], RESOURCES[1]};
  final String[] FULL_AUTO_RESOURCES = {RESOURCES[2], RESOURCES[3]};

  private HelixAdmin _admin;
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;
  private String _clusterName;
  private ClusterSetup _setupTool;

  @BeforeMethod
  public void beforeMethod() throws InterruptedException {
    final int NUM_PARTITIONS = 10;
    final int NUM_REPLICAS = 3;

    // Cluster and resource setup
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    _clusterName = className + "_" + methodName;
    _setupTool = new ClusterSetup(ZK_ADDR);
    _admin = _setupTool.getClusterManagementTool();
    _setupTool.addCluster(_clusterName, true);
    _setupTool.addResourceToCluster(_clusterName, RESOURCES[0], NUM_PARTITIONS,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.SEMI_AUTO.toString());
    _setupTool.addResourceToCluster(_clusterName, RESOURCES[1], NUM_PARTITIONS,
        BuiltInStateModelDefinitions.OnlineOffline.name(), RebalanceMode.SEMI_AUTO.toString());

    _setupTool.addResourceToCluster(_clusterName, RESOURCES[2], NUM_PARTITIONS,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.FULL_AUTO.toString());
    _setupTool.addResourceToCluster(_clusterName, RESOURCES[3], NUM_PARTITIONS,
        BuiltInStateModelDefinitions.OnlineOffline.name(), RebalanceMode.FULL_AUTO.toString());

    // Enable persist best possible assignment
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(_clusterName);
    clusterConfig.setPersistBestPossibleAssignment(true);
    configAccessor.setClusterConfig(_clusterName, clusterConfig);


    // Configure and start the participants
    _participants = new MockParticipantManager[RESOURCES.length];
    for (int i = 0; i < _participants.length; i++) {
      String host = "localhost";
      int port = 12918 + i;
      String id = host + '_' + port;
      _setupTool.addInstanceToCluster(_clusterName, id);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _clusterName, id);
      _participants[i].syncStart();
    }

    // Rebalance the resources
    for (int i = 0; i < RESOURCES.length; i++) {
      _setupTool.rebalanceResource(_clusterName, RESOURCES[i], NUM_REPLICAS);
    }

    // Start the controller
    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    _controller.syncStart();
    Thread.sleep(1000);
  }

  @AfterMethod
  public void afterMethod() {
    // Cleanup
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    _admin.dropCluster(_clusterName);
  }

  @Test
  public void testDisablePartitionAndStopInstance() throws Exception {
    // Just ensure that the entire cluster passes
    // ensure that the external view coalesces
    HelixClusterVerifier bestPossibleVerifier =
        new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient).build();
    Assert.assertTrue(bestPossibleVerifier.verify(10000));

    HelixClusterVerifier strictMatchVerifier =
        new StrictMatchExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient).build();
    Assert.assertTrue(strictMatchVerifier.verify(10000));

    // Disable partition for 1 instance, then Full-Auto ExternalView should not match IdealState.
    _admin.enablePartition(false, _clusterName, _participants[0].getInstanceName(), FULL_AUTO_RESOURCES[0],
        Lists.newArrayList(FULL_AUTO_RESOURCES[0] + "_0"));

    boolean isVerifiedFalse = TestHelper.verify(() -> {
      HelixClusterVerifier strictMatchVerifierTemp =
          new StrictMatchExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient).build();
      boolean verified = strictMatchVerifierTemp.verify(3000);
      return (!verified);
    }, 60 * 1000);
    Assert.assertTrue(isVerifiedFalse);

    // Enable the partition back
    _admin.enablePartition(true, _clusterName, _participants[0].getInstanceName(), FULL_AUTO_RESOURCES[0],
        Lists.newArrayList(FULL_AUTO_RESOURCES[0] + "_0"));
    Thread.sleep(1000);
    Assert.assertTrue(strictMatchVerifier.verify(10000));

    // Make 1 instance non-live
    _participants[0].syncStop();
    Thread.sleep(1000);

    // Semi-Auto ExternalView should not match IdealState
    for (String resource : SEMI_AUTO_RESOURCES) {
      System.out.println("Un-verify resource: " + resource);
      strictMatchVerifier = new StrictMatchExternalViewVerifier.Builder(_clusterName)
          .setZkClient(_gZkClient).setResources(Sets.newHashSet(resource)).build();
      Assert.assertFalse(strictMatchVerifier.verify(3000));
    }

    // Full-Auto still match, because preference list wouldn't contain non-live instances
    strictMatchVerifier = new StrictMatchExternalViewVerifier.Builder(_clusterName)
        .setZkClient(_gZkClient).setResources(Sets.newHashSet(FULL_AUTO_RESOURCES)).build();
    Assert.assertTrue(strictMatchVerifier.verify(10000));
  }

  @Test
  public void testResourceSubset() throws InterruptedException {
    String testDB = "resource-testDB";
    _setupTool.addResourceToCluster(_clusterName, testDB, 1,
        BuiltInStateModelDefinitions.MasterSlave.name(), RebalanceMode.SEMI_AUTO.toString());

    IdealState idealState = _admin.getResourceIdealState(_clusterName, testDB);
    idealState.setReplicas(Integer.toString(2));
    idealState.getRecord().setListField(testDB + "_0",
        Arrays.asList(_participants[1].getInstanceName(), _participants[2].getInstanceName()));
    _admin.setResourceIdealState(_clusterName, testDB, idealState);

    // Ensure that this passes even when one resource is down
    _admin.enableInstance(_clusterName, "localhost_12918", false);

    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient)
            .setResources(Sets.newHashSet(testDB)).build();
    Assert.assertTrue(verifier.verifyByPolling());

    _admin.enableCluster(_clusterName, false);
    _admin.enableInstance(_clusterName, "localhost_12918", true);

    verifier =
        new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient)
            .setResources(Sets.newHashSet(testDB)).build();
    Assert.assertTrue(verifier.verifyByPolling());

    verifier = new StrictMatchExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient)
        .setResources(Sets.newHashSet(testDB)).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // But the full cluster verification should fail
    verifier =
        new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient).build();
    Assert.assertFalse(verifier.verify(3000));

    verifier =
        new StrictMatchExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient).build();
    Assert.assertFalse(verifier.verify(3000));

    _admin.enableCluster(_clusterName, true);
  }

  @Test
  public void testSleepTransition() throws InterruptedException {

    HelixClusterVerifier bestPossibleVerifier =
        new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient).build();
    Assert.assertTrue(bestPossibleVerifier.verify(10000));

    HelixClusterVerifier strictMatchVerifier =
        new StrictMatchExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient).build();
    Assert.assertTrue(strictMatchVerifier.verify(10000));

    // Re-start a new participant with sleeping transition(all state model transition cannot finish)
    _participants[0].syncStop();
    Thread.sleep(1000);

    _participants[0] = new MockParticipantManager(ZK_ADDR, _clusterName, _participants[0].getInstanceName());
    _participants[0].setTransition(new SleepTransition(99999999));
    _participants[0].syncStart();

    // The new participant causes rebalance, but the state transitions are all stuck
    Thread.sleep(1000);
    Assert.assertFalse(strictMatchVerifier.verify(3000));
  }
}
