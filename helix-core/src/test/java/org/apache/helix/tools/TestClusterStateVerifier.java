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

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestClusterStateVerifier extends ZkTestBase {
  final String[] RESOURCES = {
      "resource0", "resource1"
  };
  private HelixAdmin _admin;
  private MockParticipant[] _participants;
  private MockController _controller;
  private String _clusterName;

  @BeforeMethod
  public void beforeMethod() throws InterruptedException {
    final int NUM_PARTITIONS = 1;
    final int NUM_REPLICAS = 1;

    // Cluster and resource setup
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    _clusterName = className + "_" + methodName;
    _admin = _setupTool.getClusterManagementTool();
    _setupTool.addCluster(_clusterName, true);
    _setupTool.addResourceToCluster(_clusterName, RESOURCES[0], NUM_PARTITIONS, "OnlineOffline",
        RebalanceMode.SEMI_AUTO.toString());
    _setupTool.addResourceToCluster(_clusterName, RESOURCES[1], NUM_PARTITIONS, "OnlineOffline",
        RebalanceMode.SEMI_AUTO.toString());

    // Configure and start the participants
    _participants = new MockParticipant[RESOURCES.length];
    for (int i = 0; i < _participants.length; i++) {
      String host = "localhost";
      int port = 12918 + i;
      String id = host + '_' + port;
      _setupTool.addInstanceToCluster(_clusterName, id);
      _participants[i] = new MockParticipant(_zkaddr, _clusterName, id);
      _participants[i].syncStart();
    }

    // Rebalance the resources
    for (int i = 0; i < RESOURCES.length; i++) {
      IdealState idealState = _admin.getResourceIdealState(_clusterName, RESOURCES[i]);
      idealState.setReplicas(Integer.toString(NUM_REPLICAS));
      idealState.setPreferenceList(RESOURCES[i] + "_0",
          Arrays.asList(_participants[i].getInstanceName()));
      _admin.setResourceIdealState(_clusterName, RESOURCES[i], idealState);
    }

    // Start the controller
    _controller = new MockController(_zkaddr, _clusterName, "controller_0");
    _controller.syncStart();
    Thread.sleep(1000);
  }

  @AfterMethod
  public void afterMethod() {
    // Cleanup
    _controller.syncStop();
    for (MockParticipant participant : _participants) {
      participant.syncStop();
    }
    _admin.dropCluster(_clusterName);
  }

  @Test
  public void testEntireCluster() {
    // Just ensure that the entire cluster passes
    // ensure that the external view coalesces
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            _clusterName));
    Assert.assertTrue(result);
  }

  @Test
  public void testResourceSubset() throws InterruptedException {
    // Ensure that this passes even when one resource is down
    _admin.enableInstance(_clusterName, "localhost_12918", false);
    Thread.sleep(1000);
    _admin.enableCluster(_clusterName, false);
    Thread.sleep(1000);
    _admin.enableInstance(_clusterName, "localhost_12918", true);
    Thread.sleep(1000);
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            _clusterName, null, Sets.newHashSet(RESOURCES[1])));
    Assert.assertTrue(result);
    String[] args = {
        "--zkSvr", _zkaddr, "--cluster", _clusterName, "--resources", RESOURCES[1]
    };
    result = ClusterStateVerifier.verifyState(args);
    Assert.assertTrue(result);

    // But the full cluster verification should fail
    boolean fullResult = new BestPossAndExtViewZkVerifier(_zkaddr, _clusterName).verify();
    Assert.assertFalse(fullResult);
    _admin.enableCluster(_clusterName, true);
  }
}
