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
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisable extends ZkTestBase {

  @Test
  public void testDisableNodeCustomIS() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;
    String disableNode = "localhost_12918";

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipantManager[] participants = new MockParticipantManager[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // set ideal state to customized mode
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setRebalanceMode(RebalanceMode.CUSTOMIZED);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ZkHelixClusterVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // disable localhost_12918
    String command =
        "--zkSvr " + ZK_ADDR + " --enableInstance " + clusterName + " " + disableNode + " false";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // make sure localhost_12918 is in OFFLINE state
    Map<String, Map<String, String>> expectStateMap = new HashMap<>();
    Map<String, String> expectInstanceStateMap = new HashMap<>();
    expectInstanceStateMap.put(disableNode, "OFFLINE");
    expectStateMap.put(".*", expectInstanceStateMap);
    boolean result =
        ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "==");
    Assert.assertTrue(result, disableNode + " should be in OFFLINE");

    // re-enable localhost_12918
    command =
        "--zkSvr " + ZK_ADDR + " --enableInstance " + clusterName + " " + disableNode + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // make sure localhost_12918 is NOT in OFFLINE state
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "!=");
    Assert.assertTrue(result, disableNode + " should NOT be in OFFLINE");

    // clean up
    // wait for all zk callbacks done
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDisableNodeAutoIS() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;
    String disableNode = "localhost_12919";

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipantManager[] participants = new MockParticipantManager[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ZkHelixClusterVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // disable localhost_12919
    String command =
        "--zkSvr " + ZK_ADDR + " --enableInstance " + clusterName + " " + disableNode + " false";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // make sure localhost_12919 is in OFFLINE state
    Map<String, Map<String, String>> expectStateMap = new HashMap<>();
    Map<String, String> expectInstanceStateMap = new HashMap<>();
    expectInstanceStateMap.put(disableNode, "OFFLINE");
    expectStateMap.put(".*", expectInstanceStateMap);
    boolean result =
        ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "==");
    Assert.assertTrue(result, disableNode + " should be in OFFLINE");

    // re-enable localhost_12919
    command =
        "--zkSvr " + ZK_ADDR + " --enableInstance " + clusterName + " " + disableNode + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // make sure localhost_12919 is NOT in OFFLINE state
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "!=");
    Assert.assertTrue(result, disableNode + " should NOT be in OFFLINE");

    // clean up
    // wait for all zk callbacks done
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDisablePartitionCustomIS() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipantManager[] participants = new MockParticipantManager[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // set ideal state to customized mode
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setRebalanceMode(RebalanceMode.CUSTOMIZED);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    BestPossibleExternalViewVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR)
            .setZkClient(_gZkClient).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // disable [TestDB0_0, TestDB0_5] on localhost_12919
    String command = "--zkSvr " + ZK_ADDR + " --enablePartition false " + clusterName
        + " localhost_12919 TestDB0 TestDB0_0 TestDB0_5";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // make sure localhost_12918 is in OFFLINE state for [TestDB0_0, TestDB0_5]
    Map<String, Map<String, String>> expectStateMap = new HashMap<>();
    Map<String, String> expectInstanceStateMap = new HashMap<>();
    expectInstanceStateMap.put("localhost_12919", "OFFLINE");
    expectStateMap.put("TestDB0_0", expectInstanceStateMap);
    expectStateMap.put("TestDB0_5", expectInstanceStateMap);
    boolean result =
        ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "==");
    Assert.assertTrue(result,
        "localhost_12919" + " should be in OFFLINE for [TestDB0_0, TestDB0_5]");

    // re-enable localhost_12919 for [TestDB0_0, TestDB0_5]
    command = "--zkSvr " + ZK_ADDR + " --enablePartition true " + clusterName
        + " localhost_12919 TestDB0 TestDB0_0 TestDB0_5";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // make sure localhost_12919 is NOT in OFFLINE state for [TestDB0_0, TestDB0_5]
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "!=");
    Assert.assertTrue(result, "localhost_12919" + " should NOT be in OFFLINE");

    // clean up
    // wait for all zk callbacks done
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDisablePartitionAutoIS() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipantManager[] participants = new MockParticipantManager[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ZkHelixClusterVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // disable [TestDB0_0, TestDB0_5] on localhost_12919
    String command = "--zkSvr " + ZK_ADDR + " --enablePartition false " + clusterName
        + " localhost_12919 TestDB0 TestDB0_0 TestDB0_5";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // make sure localhost_12918 is in OFFLINE state for [TestDB0_0, TestDB0_5]
    Map<String, Map<String, String>> expectStateMap = new HashMap<>();
    Map<String, String> expectInstanceStateMap = new HashMap<>();
    expectInstanceStateMap.put("localhost_12919", "OFFLINE");
    expectStateMap.put("TestDB0_0", expectInstanceStateMap);
    expectStateMap.put("TestDB0_5", expectInstanceStateMap);
    boolean result =
        ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "==");
    Assert.assertTrue(result,
        "localhost_12919" + " should be in OFFLINE for [TestDB0_0, TestDB0_5]");

    // re-enable localhost_12919 for [TestDB0_0, TestDB0_5]
    command = "--zkSvr " + ZK_ADDR + " --enablePartition true " + clusterName
        + " localhost_12919 TestDB0 TestDB0_0 TestDB0_5";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // make sure localhost_12919 is NOT in OFFLINE state for [TestDB0_0, TestDB0_5]
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "!=");
    Assert.assertTrue(result, "localhost_12919" + " should NOT be in OFFLINE");

    // clean up
    // wait for all zk callbacks done
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
