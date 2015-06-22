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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestDrop extends ZkTestBase {

  /**
   * Assert externalView and currentState for each participant are empty
   * @param clusterName
   * @param db
   * @param participants
   */
  private void assertEmptyCSandEV(String clusterName, String db,
      MockParticipant[] participants) {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNull(accessor.getProperty(keyBuilder.externalView(db)));

    for (MockParticipant participant : participants) {
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

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // Drop TestDB0
    HelixAdmin admin = new ZKHelixAdmin(_zkclient);
    admin.dropResource(clusterName, "TestDB0");

    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    assertEmptyCSandEV(clusterName, "TestDB0", participants);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDropResourceWithErrorPartitionSemiAuto() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    Map<String, Set<String>> errTransitions = new HashMap<String, Set<String>>();
    errTransitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
    errTransitions.put("OFFLINE-SLAVE", TestHelper.setOf("TestDB0_8"));

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0) {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
        participants[i].setTransition(new ErrTransition(errTransitions));
      } else {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      }
      participants[i].syncStart();
    }

    Map<String, Map<String, String>> errStateMap = new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");
    errStateMap.get("TestDB0").put("TestDB0_8", "localhost_12918");
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName, errStateMap));
    Assert.assertTrue(result);

    // drop resource containing error partitions should drop the partition successfully
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--dropResource", clusterName, "TestDB0"
    });

    // make sure TestDB0_4 and TestDB0_8 partitions are dropped
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    assertEmptyCSandEV(className, "TestDB0", participants);

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

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    Map<String, Set<String>> errTransitions = new HashMap<String, Set<String>>();
    errTransitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
    errTransitions.put("ERROR-DROPPED", TestHelper.setOf("TestDB0_4"));

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0) {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
        participants[i].setTransition(new ErrTransition(errTransitions));
      } else {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      }
      participants[i].syncStart();
    }

    Map<String, Map<String, String>> errStateMap = new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_4", "localhost_12918");
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName, errStateMap));
    Assert.assertTrue(result);

    // drop resource containing error partitions should invoke error->dropped transition
    // if error happens during error->dropped transition, partition should be disabled
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--dropResource", clusterName, "TestDB0"
    });

    // make sure TestDB0_4 stay in ERROR state and is disabled
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName, errStateMap));
    Assert.assertTrue(result);

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig("localhost_12918"));
    List<String> disabledPartitions = config.getDisabledPartitions();
    // System.out.println("disabledPartitions: " + disabledPartitions);
    Assert.assertEquals(disabledPartitions.size(), 1, "TestDB0_4 should be disabled");
    Assert.assertEquals(disabledPartitions.get(0), "TestDB0_4");

    // ExteranlView should have TestDB0_4->localhost_12918_>ERROR
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
    CurrentState cs =
        accessor.getProperty(keyBuilder.currentState(participants[0].getInstanceName(),
            participants[0].getSessionId(), "TestDB0"));
    Map<String, String> partitionStateMap = cs.getPartitionStateMap();
    Assert.assertEquals(partitionStateMap.size(), 1);
    Assert.assertEquals(partitionStateMap.keySet().iterator().next(), "TestDB0_4");
    Assert.assertEquals(partitionStateMap.get("TestDB0_4"), HelixDefinedState.ERROR.name());

    // all other participants should have cleaned up empty current state
    for (int i = 1; i < n; i++) {
      String instanceName = participants[i].getInstanceName();
      String sessionId = participants[i].getSessionId();
      Assert.assertNull(accessor.getProperty(keyBuilder.currentState(instanceName, sessionId,
          "TestDB0")));
    }

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

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
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

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), isBuilder.build());

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    Map<String, Set<String>> errTransitions = new HashMap<String, Set<String>>();
    errTransitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_0"));

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0) {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
        participants[i].setTransition(new ErrTransition(errTransitions));
      } else {
        participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      }
      participants[i].syncStart();
    }

    Map<String, Map<String, String>> errStateMap = new HashMap<String, Map<String, String>>();
    errStateMap.put("TestDB0", new HashMap<String, String>());
    errStateMap.get("TestDB0").put("TestDB0_0", "localhost_12918");
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName, errStateMap));
    Assert.assertTrue(result);

    // drop resource containing error partitions should drop the partition successfully
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--dropResource", clusterName, "TestDB0"
    });

    // make sure TestDB0_0 partition is dropped
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result, "Should be empty exeternal-view");

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

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // add schemata resource group
    String command =
        "--zkSvr " + _zkaddr + " --addResource " + clusterName
            + " schemata 1 STORAGE_DEFAULT_SM_SCHEMATA";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    command = "--zkSvr " + _zkaddr + " --rebalance " + clusterName + " schemata " + n;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // drop schemata resource group
    // System.out.println("Dropping schemata resource group...");
    command = "--zkSvr " + _zkaddr + " --dropResource " + clusterName + " schemata";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    assertEmptyCSandEV(clusterName, "schemata", participants);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @DataProvider(name = "RebalanceModeProvider")
  public static Object [][] rebalanceModes() {
    IdealState.RebalanceMode modes [] = IdealState.RebalanceMode.values();
    Object [][] args = new Object[modes.length][];
    for (int i = 0; i < modes.length; i++) {
      args[i] = new Object [] { modes[i] };
    }
    return args;
  }


  @Test(dataProvider = "RebalanceModeProvider")
  public void testDropSinglePartition(IdealState.RebalanceMode mode)
      throws Exception {
    if (!mode.equals(IdealState.RebalanceMode.FULL_AUTO) &&
        !mode.equals(IdealState.RebalanceMode.SEMI_AUTO) &&
        !mode.equals(IdealState.RebalanceMode.CUSTOMIZED)) {
      return;
    }

    String clusterName = TestHelper.getTestClassName() + "_" + TestHelper.getTestMethodName() + "_" + mode.name();
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", mode, true); // do rebalance

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
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
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    ExternalView externalView = accessor.getProperty(keyBuilder.externalView("TestDB0"));
    Assert.assertFalse(externalView.getPartitionSet().contains(partitionToDrop),
        "TestDB0_0 should be dropped since it's not in ideal-state");
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
