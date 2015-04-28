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
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestEntropyFreeNodeBounce extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestEntropyFreeNodeBounce.class);

  // TODO Fix this test. It fails when participants don't evenly divide partitions because of
  // HELIX-543 RB-27808. Basically, the new placement algorithm determines node order based on the
  // number of currently assigned partitons, so the bounced node will appear at the end of the
  // list. Consequently, it will have floor capacity for partitions. If it previously had ceiling
  // capacity, then one of its partitions will be assigned elsewhere.
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
    MockController controller = new MockController(_zkaddr, clusterName, "controller");
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
      for (int i = 0; i < NUM_PARTICIPANTS; i++) {
        // disable the controller, bounce the node, re-enable the controller, verify assignments
        // remained the same
        helixAdmin.enableCluster(clusterName, false);
        participants[i].disconnect();
        Thread.sleep(1000);
        participants[i] = createParticipant(clusterName, participants[i].getInstanceName());
        participants[i].connect();
        Thread.sleep(1000);
        helixAdmin.enableCluster(clusterName, true);
        Thread.sleep(1000);
        result =
            ClusterStateVerifier.verifyByZkCallback(new MatchingExternalViewVerifier(
                stableExternalView, clusterName, 1));
        if (!result) {
          ExternalView currentExternalView =
              accessor.getProperty(keyBuilder.externalView(RESOURCE_NAME));
          for (String partition : stableExternalView.getPartitionSet()) {
            Map<String, String> expect = stableExternalView.getStateMap(partition);
            Map<String, String> actual = currentExternalView.getStateMap(partition);
            if (!expect.equals(actual)) {
              LOG.error(partition + " is moved. expect: " + expect + ", actual: " + actual);
            }
          }
        }
        Assert.assertTrue(result);

        // Due to the TODO above, the external view is not as stable as desired.
        stableExternalView = accessor.getProperty(keyBuilder.externalView(RESOURCE_NAME));
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
   * Simple verifier: just check that the external view matches a reference, allowing up to a
   * predefined number of differing partitions.
   */
  private static class MatchingExternalViewVerifier extends ZkVerifier {
    private final HelixDataAccessor _accessor;
    private final ExternalView _reference;
    private final int _maxDifferences;

    public MatchingExternalViewVerifier(ExternalView reference, String clusterName,
        int maxDifferences) {
      super(clusterName, _zkclient);
      _accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
      _reference = reference;
      _maxDifferences = maxDifferences;
    }

    @Override
    public boolean verify() {
      ExternalView externalView =
          _accessor.getProperty(_accessor.keyBuilder().externalView(_reference.getResourceName()));
      int differences = 0;
      for (String partition : _reference.getPartitionSet()) {
        Map<String, String> expect = _reference.getStateMap(partition);
        Map<String, String> actual = externalView.getStateMap(partition);
        if (!expect.equals(actual)) {
          differences++;
        }
      }
      return differences <= _maxDifferences;
    }
  }
}
