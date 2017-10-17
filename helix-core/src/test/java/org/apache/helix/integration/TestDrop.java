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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDrop extends ZkIntegrationTestBase {

  /**
   * Assert externalView and currentState for each participant are empty
   * @param clusterName
   * @param db
   * @param participants
   */
  private void assertEmptyCSandEV(String clusterName, String db, MockParticipantManager[] participants) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNull(accessor.getProperty(keyBuilder.externalView(db)));

    for (MockParticipantManager participant : participants) {
      String instanceName = participant.getInstanceName();
      String sessionId = participant.getSessionId();
      Assert.assertNull(accessor.getProperty(keyBuilder.currentState(instanceName, sessionId, db)));
    }
  }

  @Test
  public void testBasic() throws Exception {
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
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    HelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verify());

    // Drop TestDB0
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.dropResource(clusterName, "TestDB0");

    Thread.sleep(1000);

    assertEmptyCSandEV(clusterName, "TestDB0", participants);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDropErrorPartitionAutoIS() throws Exception {
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
        10, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    Map<String, Set<String>> errTransitions = new HashMap<String, Set<String>>();
    errTransitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
    errTransitions.put("OFFLINE-SLAVE", TestHelper.setOf("TestDB0_8"));

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0) {
        participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
        participants[i].setTransition(new ErrTransition(errTransitions));
      } else {
        participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      }
      participants[i].syncStart();
    }

    Map<String, Map<String, String>> errStateMap = new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");
    errStateMap.get("TestDB0").put("TestDB0_8", "localhost_12918");

    HelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR)
            .setErrStates(errStateMap).build();
    Assert.assertTrue(verifier.verify());

    // drop resource containing error partitions should drop the partition successfully
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", ZK_ADDR, "--dropResource", clusterName, "TestDB0"
    });

    // make sure TestDB0_4 and TestDB0_8 partitions are dropped
    verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verify());

    assertEmptyCSandEV(className, "TestDB0", participants);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDropErrorPartitionFailedAutoIS() throws Exception {
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
    Map<String, Set<String>> errTransitions = new HashMap<String, Set<String>>();
    errTransitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
    errTransitions.put("ERROR-DROPPED", TestHelper.setOf("TestDB0_4"));

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0) {
        participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
        participants[i].setTransition(new ErrTransition(errTransitions));
      } else {
        participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      }
      participants[i].syncStart();
    }

    Map<String, Map<String, String>> errStateMap = new HashMap<>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");

    HelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR)
            .setErrStates(errStateMap).build();
    Assert.assertTrue(verifier.verify());

    // drop resource containing error partitions should invoke error->dropped transition
    // if error happens during error->dropped transition, partition should be disabled
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", ZK_ADDR, "--dropResource", clusterName, "TestDB0"
    });

    Thread.sleep(100);
    // make sure TestDB0_4 stay in ERROR state and is disabled
    Assert.assertTrue(verifier.verify());

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig("localhost_12918"));
    List<String> disabledPartitions = config.getDisabledPartitions();
    // System.out.println("disabledPartitions: " + disabledPartitions);
    Assert.assertEquals(disabledPartitions.size(), 1, "TestDB0_4 should be disabled");
    Assert.assertEquals(disabledPartitions.get(0), "TestDB0_4");

    // ExteranlView should have TestDB0_4->localhost_12918_>ERROR
    Thread.sleep(2000);
    ExternalView ev = accessor.getProperty(keyBuilder.externalView("TestDB0"));
    Set<String> partitions = ev.getPartitionSet();
    Assert.assertEquals(partitions.size(), 1, "Should have TestDB0_4->localhost_12918->ERROR");
    String errPartition = partitions.iterator().next();
    Assert.assertEquals(errPartition, "TestDB0_4");
    Map<String, String> stateMap = ev.getStateMap(errPartition);
    Assert.assertEquals(stateMap.size(), 1);
    Assert.assertEquals(stateMap.keySet().iterator().next(), "localhost_12918");
    Assert.assertEquals(stateMap.get("localhost_12918"), HelixDefinedState.ERROR.name());

    // localhost_12918 should have TestDB0_4 in ERROR state
    CurrentState cs = accessor.getProperty(keyBuilder.currentState(participants[0].getInstanceName(),
        participants[0].getSessionId(), "TestDB0"));
    Map<String, String> partitionStateMap = cs.getPartitionStateMap();
    Assert.assertEquals(partitionStateMap.size(), 1);
    Assert.assertEquals(partitionStateMap.keySet().iterator().next(), "TestDB0_4");
    Assert.assertEquals(partitionStateMap.get("TestDB0_4"), HelixDefinedState.ERROR.name());

    // all other participants should have cleaned up empty current state
    for (int i = 1; i < n; i++) {
      String instanceName = participants[i].getInstanceName();
      String sessionId = participants[i].getSessionId();
      Assert.assertNull(accessor.getProperty(keyBuilder.currentState(instanceName, sessionId, "TestDB0")));
    }

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDropErrorPartitionCustomIS() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipantManager[] participants = new MockParticipantManager[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        2, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", false); // do rebalance

    // set custom ideal-state
    CustomModeISBuilder isBuilder = new CustomModeISBuilder("TestDB0");
    isBuilder.setNumPartitions(2);
    isBuilder.setNumReplica(2);
    isBuilder.setStateModel("MasterSlave");
    isBuilder.assignInstanceAndState("TestDB0_0", "localhost_12918", "MASTER");
    isBuilder.assignInstanceAndState("TestDB0_0", "localhost_12919", "SLAVE");
    isBuilder.assignInstanceAndState("TestDB0_1", "localhost_12919", "MASTER");
    isBuilder.assignInstanceAndState("TestDB0_1", "localhost_12918", "SLAVE");

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), isBuilder.build());

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    Map<String, Set<String>> errTransitions = new HashMap<String, Set<String>>();
    errTransitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_0"));

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0) {
        participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
        participants[i].setTransition(new ErrTransition(errTransitions));
      } else {
        participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      }
      participants[i].syncStart();
    }

    Map<String, Map<String, String>> errStateMap = new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_0", "localhost_12918");

    HelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR)
            .setErrStates(errStateMap).build();
    Assert.assertTrue(verifier.verify());

    // drop resource containing error partitions should drop the partition successfully
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", ZK_ADDR, "--dropResource", clusterName, "TestDB0"
    });

    // make sure TestDB0_0 partition is dropped
    verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verify(), "Should be empty exeternal-view");

    assertEmptyCSandEV(clusterName, "TestDB0", participants);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDropSchemataResource() throws Exception {
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

    HelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verify());

    // add schemata resource group
    String command =
        "--zkSvr " + ZK_ADDR + " --addResource " + clusterName
            + " schemata 1 STORAGE_DEFAULT_SM_SCHEMATA";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    command = "--zkSvr " + ZK_ADDR + " --rebalance " + clusterName + " schemata " + n;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    Assert.assertTrue(verifier.verify());

    // drop schemata resource group
    // System.out.println("Dropping schemata resource group...");
    command = "--zkSvr " + ZK_ADDR + " --dropResource " + clusterName + " schemata";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Assert.assertTrue(verifier.verify());

    Thread.sleep(2000);
    assertEmptyCSandEV(clusterName, "schemata", participants);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
