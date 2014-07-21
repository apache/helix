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

import java.util.Date;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestEntropyFreeNodeBounce extends ZkTestBase {
  @Test
  public void testBounceAll() throws Exception {
    // pick numbers that don't divide evenly
    final int NUM_PARTICIPANTS = 5;
    final int NUM_PARTITIONS = 123;
    final int NUM_REPLICAS = 1;
    final String RESOURCE_PREFIX = "TestDB";
    final String RESOURCE_NAME = RESOURCE_PREFIX + "0";

    // create a cluster name based on this test name
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // Set up cluster
    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "OnlineOffline", RebalanceMode.FULL_AUTO, // use FULL_AUTO mode to test node tagging
        true); // do rebalance

    // Start the participants
    HelixManager[] participants = new HelixManager[NUM_PARTICIPANTS];
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      final String instanceName = "localhost_" + (12918 + i);
      participants[i] = createParticipant(clusterName, instanceName);
      participants[i].connect();
    }

    // Start the controller
    ClusterControllerManager controller =
        new ClusterControllerManager(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // get an admin and accessor
    HelixAdmin helixAdmin = new ZKHelixAdmin(_zkclient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // do the test
    try {
      Thread.sleep(1000);
      // ensure that the external view coalesces
      boolean result =
          ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
              clusterName));
      Assert.assertTrue(result);
      ExternalView stableExternalView =
          accessor.getProperty(keyBuilder.externalView(RESOURCE_NAME));
      for (HelixManager participant : participants) {
        // disable the controller, bounce the node, re-enable the controller, verify assignments
        // remained the same
        helixAdmin.enableCluster(clusterName, false);
        participant.disconnect();
        Thread.sleep(1000);
        participant = createParticipant(clusterName, participant.getInstanceName());
        participant.connect();
        Thread.sleep(1000);
        helixAdmin.enableCluster(clusterName, true);
        Thread.sleep(1000);
        result =
            ClusterStateVerifier.verifyByZkCallback(new MatchingExternalViewVerifier(
                stableExternalView, clusterName));
        Assert.assertTrue(result);
      }
    } finally {
      // clean up
      controller.syncStop();
      for (HelixManager participant : participants) {
        participant.disconnect();
      }
      System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
    }
  }

  private HelixManager createParticipant(String clusterName, String instanceName) {
    HelixManager participant =
        new ZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, _zkaddr);
    participant.getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.from("OnlineOffline"), new TestHelixConnection.MockStateModelFactory());
    return participant;
  }

  /**
   * Simple verifier: just check that the external view matches a reference
   */
  private static class MatchingExternalViewVerifier extends ZkVerifier {
    private final HelixDataAccessor _accessor;
    private final ExternalView _reference;

    public MatchingExternalViewVerifier(ExternalView reference, String clusterName) {
      super(clusterName, _zkclient);
      _accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
      _reference = reference;
    }

    @Override
    public boolean verify() {
      ExternalView externalView =
          _accessor.getProperty(_accessor.keyBuilder().externalView(_reference.getResourceName()));
      return _reference.equals(externalView);
    }
  }
}
