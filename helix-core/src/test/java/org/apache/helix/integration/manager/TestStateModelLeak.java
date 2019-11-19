package org.apache.helix.integration.manager;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.participant.HelixStateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * test drop resource should remove state-models
 */
public class TestStateModelLeak extends ZkUnitTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestStateModelLeak.class);

  /**
   * test drop resource should remove all state models
   * @throws Exception
   */
  @Test
  public void testDrop() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
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

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // check state-models in state-machine
    HelixStateMachineEngine stateMachine =
        (HelixStateMachineEngine) participants[0].getStateMachineEngine();
    StateModelFactory<? extends StateModel> fty = stateMachine.getStateModelFactory("MasterSlave");
    Map<String, String> expectStateModelMap = new TreeMap<String, String>();
    expectStateModelMap.put("TestDB0_0", "SLAVE");
    expectStateModelMap.put("TestDB0_1", "MASTER");
    expectStateModelMap.put("TestDB0_2", "SLAVE");
    expectStateModelMap.put("TestDB0_3", "MASTER");
    checkStateModelMap(fty, expectStateModelMap);

    // drop resource
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.dropResource(clusterName, "TestDB0");

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // check state models have been dropped also
    Assert.assertTrue(fty.getPartitionSet("TestDB0").isEmpty(),
        "All state-models should be dropped, but was " + fty.getPartitionSet("TestDB0"));

    // cleanup
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * test drop resource in error state should remove all state-models
   * @throws Exception
   */
  @Test
  public void testDropErrorPartition() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
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

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      if (i == 0) {
        Map<String, Set<String>> errTransitionMap = new HashMap<String, Set<String>>();
        Set<String> partitions = new HashSet<String>();
        partitions.add("TestDB0_0");
        errTransitionMap.put("OFFLINE-SLAVE", partitions);
        participants[0].setTransition(new ErrTransition(errTransitionMap));
      }

      participants[i].syncStart();
    }

    Map<String, Map<String, String>> errStates = new HashMap<String, Map<String, String>>();
    errStates.put("TestDB0", new HashMap<String, String>());
    errStates.get("TestDB0").put("TestDB0_0", "localhost_12918");
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName, errStates));
    Assert.assertTrue(result);

    // check state-models in state-machine
    HelixStateMachineEngine stateMachine =
        (HelixStateMachineEngine) participants[0].getStateMachineEngine();
    StateModelFactory<? extends StateModel> fty = stateMachine.getStateModelFactory("MasterSlave");
    Map<String, String> expectStateModelMap = new TreeMap<String, String>();
    expectStateModelMap.put("TestDB0_0", "ERROR");
    expectStateModelMap.put("TestDB0_1", "MASTER");
    expectStateModelMap.put("TestDB0_2", "SLAVE");
    expectStateModelMap.put("TestDB0_3", "MASTER");
    checkStateModelMap(fty, expectStateModelMap);

    // drop resource
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.dropResource(clusterName, "TestDB0");

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // check state models have been dropped also
    Assert.assertTrue(fty.getPartitionSet("TestDB0").isEmpty(),
        "All state-models should be dropped, but was " + fty.getPartitionSet("TestDB0"));

    // cleanup
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * check state-model factory contains state-models same as in expect-state-model map
   * @param fty
   * @param expectStateModelMap
   */
  static void checkStateModelMap(StateModelFactory<? extends StateModel> fty,
      Map<String, String> expectStateModelMap) {
    Assert.assertEquals(fty.getPartitionSet("TestDB0").size(), expectStateModelMap.size());
    for (String partition : fty.getPartitionSet("TestDB0")) {
      StateModel stateModel = fty.getStateModel("TestDB0", partition);
      String actualState = stateModel.getCurrentState();
      String expectState = expectStateModelMap.get(partition);
      LOG.debug(partition + " actual state: " + actualState + ", expect state: " + expectState);
      Assert.assertEquals(actualState, expectState, "partition: " + partition
          + " should be in state: " + expectState + " but was " + actualState);
    }
  }
}
