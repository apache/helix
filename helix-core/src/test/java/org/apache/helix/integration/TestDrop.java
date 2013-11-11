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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDrop extends ZkIntegrationTestBase {
  @Test
  public void testDropResourceWithErrorPartitionSemiAuto() throws Exception {
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
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName, errStateMap));
    Assert.assertTrue(result);

    // drop resource containing error partitions should drop the partition successfully
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", ZK_ADDR, "--dropResource", clusterName, "TestDB0"
    });

    // make sure TestDB0_4 and TestDB0_8 partitions are dropped
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testFailToDropResourceWithErrorPartitionSemiAuto() throws Exception {
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

    Map<String, Map<String, String>> errStateMap = new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName, errStateMap));
    Assert.assertTrue(result);

    // drop resource containing error partitions should invoke error->dropped transition
    // if error happens during error->dropped transition, partition should be disabled
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", ZK_ADDR, "--dropResource", clusterName, "TestDB0"
    });

    // make sure TestDB0_4 stay in ERROR state and is disabled
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName, errStateMap));
    Assert.assertTrue(result);

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig("localhost_12918"));
    List<String> disabledPartitions = config.getDisabledPartitions();
    // System.out.println("disabledPartitions: " + disabledPartitions);
    Assert.assertEquals(disabledPartitions.size(), 1, "TestDB0_4 should be disabled");
    Assert.assertEquals(disabledPartitions.get(0), "TestDB0_4");

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDropResourceWithErrorPartitionCustom() throws Exception {
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
    Builder keyBuiler = accessor.keyBuilder();
    accessor.setProperty(keyBuiler.idealStates("TestDB0"), isBuilder.build());

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
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName, errStateMap));
    Assert.assertTrue(result);

    // drop resource containing error partitions should drop the partition successfully
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", ZK_ADDR, "--dropResource", clusterName, "TestDB0"
    });

    // make sure TestDB0_0 partition is dropped
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result, "Should be empty exeternal-view");

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

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // add schemata resource group
    String command =
        "--zkSvr " + ZK_ADDR + " --addResource " + clusterName
            + " schemata 1 STORAGE_DEFAULT_SM_SCHEMATA";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    command = "--zkSvr " + ZK_ADDR + " --rebalance " + clusterName + " schemata " + n;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // drop schemata resource group
    System.out.println("Dropping schemata resource group...");
    command = "--zkSvr " + ZK_ADDR + " --dropResource " + clusterName + " schemata";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // make sure schemata external view is empty
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    ExternalView extView = accessor.getProperty(keyBuilder.externalView("schemata"));
    Assert.assertEquals(extView.getPartitionSet().size(), 0,
        "schemata externalView should be empty but was \"" + extView + "\"");

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Drop a single partition in a resource of semi-auto mode
   */
  @Test
  public void testDropSinglePartitionSemiAuto() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);

    // remove one partition from ideal-state should drop that partition
    String partitionToDrop = "TestDB0_1";
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.getRecord().getListFields().remove(partitionToDrop);
    idealState.getRecord().getMapFields().remove(partitionToDrop);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);

    ExternalView externalView = accessor.getProperty(keyBuilder.externalView("TestDB0"));
    Assert.assertFalse(externalView.getPartitionSet().contains(partitionToDrop),
        "TestDB0_0 should be dropped since it's not in ideal-state");
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
