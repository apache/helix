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
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisableResource extends ZkTestBase {
  private static final int N = 2;
  private static final int PARTITION_NUM = 1;

  @Test
  public void testDisableResourceInSemiAutoMode() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        PARTITION_NUM, // partitions per resource
        N, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipant participants[] = new MockParticipant[N];
    for (int i = 0; i < N; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // Disable TestDB0
    enableResource(clusterName, false);
    checkExternalView(clusterName);

    // Re-enable TestDB0
    enableResource(clusterName, true);
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    // Clean up
    controller.syncStop();
    for (int i = 0; i < N; i++) {
      participants[i].syncStop();
    }
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDisableResourceInFullAutoMode() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        PARTITION_NUM, // partitions per resource
        N, // number of nodes
        2, // replicas
        "MasterSlave", RebalanceMode.FULL_AUTO, true); // do rebalance

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipant participants[] = new MockParticipant[N];
    for (int i = 0; i < N; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // disable TestDB0
    enableResource(clusterName, false);
    checkExternalView(clusterName);

    // Re-enable TestDB0
    enableResource(clusterName, true);
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    // Clean up
    controller.syncStop();
    for (int i = 0; i < N; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDisableResourceInCustomMode() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        PARTITION_NUM, // partitions per resource
        N, // number of nodes
        2, // replicas
        "MasterSlave", RebalanceMode.CUSTOMIZED, true); // do rebalance

    // set up custom ideal-state
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setPartitionState("TestDB0_0", "localhost_12918", "SLAVE");
    idealState.setPartitionState("TestDB0_0", "localhost_12919", "SLAVE");
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipant participants[] = new MockParticipant[N];
    for (int i = 0; i < N; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // Disable TestDB0
    enableResource(clusterName, false);
    checkExternalView(clusterName);

    // Re-enable TestDB0
    enableResource(clusterName, true);
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    // Clean up
    controller.syncStop();
    for (int i = 0; i < N; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private void enableResource(String clusterName, boolean enabled) {
    HelixAdmin admin = new ZKHelixAdmin(_zkclient);
    admin.enableResource(clusterName, "TestDB0", enabled);
  }

  /**
   * Check all partitions are in OFFLINE state
   * @param accessor
   * @throws Exception
   */
  private void checkExternalView(String clusterName) throws Exception {
    final HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);

    // verify that states of TestDB0 are all OFFLINE
    boolean result = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();

        ExternalView extView = accessor.getProperty(keyBuilder.externalView("TestDB0"));
        if (extView == null) {
          return false;
        }
        Set<String> partitionSet = extView.getPartitionSet();
        if (partitionSet == null || partitionSet.size() != PARTITION_NUM) {
          return false;
        }
        for (String partition : partitionSet) {
          Map<String, String> instanceStates = extView.getStateMap(partition);
          for (String state : instanceStates.values()) {
            if (!"OFFLINE".equals(state)) {
              return false;
            }
          }
        }
        return true;
      }
    }, 10 * 1000);
    Assert.assertTrue(result);
  }
}
