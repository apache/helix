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

import java.util.Arrays;

import com.google.common.collect.Sets;
import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Deprecated
public class TestClusterStateVerifier extends ZkUnitTestBase {
  final String[] RESOURCES = {
      "resource0", "resource1"
  };
  private HelixAdmin _admin;
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;
  private String _clusterName;

  @BeforeMethod
  public void beforeMethod() throws InterruptedException {
    final int NUM_PARTITIONS = 1;
    final int NUM_REPLICAS = 1;

    // Cluster and resource setup
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    _clusterName = className + "_" + methodName;
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    _admin = setupTool.getClusterManagementTool();
    setupTool.addCluster(_clusterName, true);
    setupTool.addResourceToCluster(_clusterName, RESOURCES[0], NUM_PARTITIONS, "OnlineOffline",
        RebalanceMode.SEMI_AUTO.toString());
    setupTool.addResourceToCluster(_clusterName, RESOURCES[1], NUM_PARTITIONS, "OnlineOffline",
        RebalanceMode.SEMI_AUTO.toString());

    // Configure and start the participants
    _participants = new MockParticipantManager[RESOURCES.length];
    for (int i = 0; i < _participants.length; i++) {
      String host = "localhost";
      int port = 12918 + i;
      String id = host + '_' + port;
      setupTool.addInstanceToCluster(_clusterName, id);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _clusterName, id);
      _participants[i].syncStart();
    }

    // Rebalance the resources
    for (int i = 0; i < RESOURCES.length; i++) {
      IdealState idealState = _admin.getResourceIdealState(_clusterName, RESOURCES[i]);
      idealState.setReplicas(Integer.toString(NUM_REPLICAS));
      idealState.getRecord().setListField(RESOURCES[i] + "_0",
          Arrays.asList(_participants[i].getInstanceName()));
      _admin.setResourceIdealState(_clusterName, RESOURCES[i], idealState);
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
  public void testEntireCluster() {
    // Just ensure that the entire cluster passes
    // ensure that the external view coalesces
    boolean result = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, _clusterName));
    Assert.assertTrue(result);
  }

  @Test
  public void testResourceSubset() throws InterruptedException {
    // Ensure that this passes even when one resource is down
    _admin.enableInstance(_clusterName, "localhost_12918", false);
    Thread.sleep(1000);
    _admin.enableCluster(_clusterName, false);
    _admin.enableInstance(_clusterName, "localhost_12918", true);
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            _clusterName, null, Sets.newHashSet(RESOURCES[1])));
    Assert.assertTrue(result);
    String[] args = {
        "--zkSvr", ZK_ADDR, "--cluster", _clusterName, "--resources", RESOURCES[1]
    };
    result = ClusterStateVerifier.verifyState(args);
    Assert.assertTrue(result);

    // But the full cluster verification should fail
    boolean fullResult = new BestPossAndExtViewZkVerifier(ZK_ADDR, _clusterName).verify();
    Assert.assertFalse(fullResult);
    _admin.enableCluster(_clusterName, true);
  }
}
