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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisableResource extends ZkUnitTestBase {
  private static final int N = 2;
  private static final int PARTITION_NUM = 1;

  @Test
  public void testDisableResourceInSemiAutoMode() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        PARTITION_NUM, // partitions per resource
        N, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[N];
    for (int i = 0; i < N; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }
    // Check for connection status
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    boolean result = TestHelper.verify(() -> {
      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
      for (int i = 0; i < N; i++) {
        if (!participants[i].isConnected()
            || !liveInstances.contains(participants[i].getInstanceName())) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // Disable TestDB0
    enableResource(clusterName, false);
    result =
        TestHelper.verify(
            () -> !_gSetupTool.getClusterManagementTool()
                .getResourceIdealState(clusterName, "TestDB0").isEnabled(),
            TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);
    checkExternalView(clusterName);

    // Re-enable TestDB0
    enableResource(clusterName, true);
    result =
        TestHelper.verify(
            () -> _gSetupTool.getClusterManagementTool()
                .getResourceIdealState(clusterName, "TestDB0").isEnabled(),
            TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // Clean up
    controller.syncStop();
    for (int i = 0; i < N; i++) {
      participants[i].syncStop();
    }
    result = TestHelper.verify(() -> {
      if (accessor.getPropertyStat(accessor.keyBuilder().controllerLeader()) != null) {
        return false;
      }
      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
      for (int i = 0; i < N; i++) {
        if (participants[i].isConnected()
            || liveInstances.contains(participants[i].getInstanceName())) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDisableResourceInFullAutoMode() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        PARTITION_NUM, // partitions per resource
        N, // number of nodes
        2, // replicas
        "MasterSlave", RebalanceMode.FULL_AUTO, true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[N];
    for (int i = 0; i < N; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }
    // Check for connection status
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    boolean result = TestHelper.verify(() -> {
      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
      for (int i = 0; i < N; i++) {
        if (!participants[i].isConnected()
            || !liveInstances.contains(participants[i].getInstanceName())) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    // disable TestDB0
    enableResource(clusterName, false);
    result =
        TestHelper.verify(
            () -> !_gSetupTool.getClusterManagementTool()
                .getResourceIdealState(clusterName, "TestDB0").isEnabled(),
            TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    checkExternalView(clusterName);

    // Re-enable TestDB0
    enableResource(clusterName, true);
    result =
        TestHelper.verify(
            () -> _gSetupTool.getClusterManagementTool()
                .getResourceIdealState(clusterName, "TestDB0").isEnabled(),
            TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // Clean up
    controller.syncStop();
    for (int i = 0; i < N; i++) {
      participants[i].syncStop();
    }
    result = TestHelper.verify(() -> {
      if (accessor.getPropertyStat(accessor.keyBuilder().controllerLeader()) != null) {
        return false;
      }
      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
      for (int i = 0; i < N; i++) {
        if (participants[i].isConnected()
            || liveInstances.contains(participants[i].getInstanceName())) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDisableResourceInCustomMode() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
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

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[N];
    for (int i = 0; i < N; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }
    // Check for connection status
    boolean result = TestHelper.verify(() -> {
      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
      for (int i = 0; i < N; i++) {
        if (!participants[i].isConnected()
            || !liveInstances.contains(participants[i].getInstanceName())) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR)
            .setZkClient(_gZkClient).build();

    // Disable TestDB0
    enableResource(clusterName, false);

    // Check that the resource has been disabled
    result =
        TestHelper.verify(
            () -> !_gSetupTool.getClusterManagementTool()
                .getResourceIdealState(clusterName, "TestDB0").isEnabled(),
            TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);
    Assert.assertTrue(verifier.verifyByPolling());

    checkExternalView(clusterName);

    // Re-enable TestDB0
    enableResource(clusterName, true);
    // Check that the resource has been enabled
    result =
        TestHelper.verify(
            () -> _gSetupTool.getClusterManagementTool()
                .getResourceIdealState(clusterName, "TestDB0").isEnabled(),
            TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);
    Assert.assertTrue(verifier.verifyByPolling());

    // Clean up
    controller.syncStop();
    for (int i = 0; i < N; i++) {
      participants[i].syncStop();
    }
    result = TestHelper.verify(() -> {
      if (accessor.getPropertyStat(accessor.keyBuilder().controllerLeader()) != null) {
        return false;
      }
      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
      for (int i = 0; i < N; i++) {
        if (participants[i].isConnected()
            || liveInstances.contains(participants[i].getInstanceName())) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private void enableResource(String clusterName, boolean enabled) {
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.enableResource(clusterName, "TestDB0", enabled);
  }

  /**
   * Check all partitions are in OFFLINE state
   * @param clusterName
   * @throws Exception
   */
  private void checkExternalView(String clusterName) throws Exception {
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    final HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);

    // verify that states of TestDB0 are all OFFLINE
    boolean result = TestHelper.verify(() -> {
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
    }, 10 * 1000);
    Assert.assertTrue(result);
  }
}
