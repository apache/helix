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
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.participant.HelixStateMachineEngine;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * test drop resource should remove state-models
 */
public class TestStateModelLeak extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestStateModelLeak.class);

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

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller = new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // check state-models in state-machine
    HelixStateMachineEngine stateMachine =
        (HelixStateMachineEngine) participants[0].getStateMachineEngine();
    StateTransitionHandlerFactory<? extends TransitionHandler> fty =
        stateMachine.getStateModelFactory(StateModelDefId.from("MasterSlave"));
    Map<PartitionId, String> expectStateModelMap = new TreeMap<PartitionId, String>();
    expectStateModelMap.put(PartitionId.from("TestDB0_0"), "SLAVE");
    expectStateModelMap.put(PartitionId.from("TestDB0_1"), "MASTER");
    expectStateModelMap.put(PartitionId.from("TestDB0_2"), "SLAVE");
    expectStateModelMap.put(PartitionId.from("TestDB0_3"), "MASTER");
    checkStateModelMap(fty, expectStateModelMap);

    // drop resourcePartitionId
    HelixAdmin admin = new ZKHelixAdmin(_zkclient);
    admin.dropResource(clusterName, "TestDB0");

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // check state models have been dropped also
    for (ResourceId resource : fty.getResourceSet()) {
      Assert.assertTrue(fty.getPartitionSet(resource).isEmpty(),
          "All state-models should be dropped, but was " + fty.getPartitionSet(resource));
    }

    // cleanup
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

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

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller = new MockController(_zkaddr, clusterName, "controller");
    controller.syncStart();

    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
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
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName, errStates));
    Assert.assertTrue(result);

    // check state-models in state-machine
    HelixStateMachineEngine stateMachine =
        (HelixStateMachineEngine) participants[0].getStateMachineEngine();
    StateTransitionHandlerFactory<? extends TransitionHandler> fty =
        stateMachine.getStateModelFactory(StateModelDefId.from("MasterSlave"));
    Map<PartitionId, String> expectStateModelMap = new TreeMap<PartitionId, String>();
    expectStateModelMap.put(PartitionId.from("TestDB0_0"), "ERROR");
    expectStateModelMap.put(PartitionId.from("TestDB0_1"), "MASTER");
    expectStateModelMap.put(PartitionId.from("TestDB0_2"), "SLAVE");
    expectStateModelMap.put(PartitionId.from("TestDB0_3"), "MASTER");
    checkStateModelMap(fty, expectStateModelMap);

    // drop resource
    HelixAdmin admin = new ZKHelixAdmin(_zkclient);
    admin.dropResource(clusterName, "TestDB0");

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // check state models have been dropped also
    for (ResourceId resource : fty.getResourceSet()) {
      Assert.assertTrue(fty.getPartitionSet(resource).isEmpty(),
          "All state-models should be dropped, but was " + fty.getPartitionSet(resource));
    }
    // cleanup
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * check state-model factory contains state-models same as in expect-state-model map
   * @param fty
   * @param expectStateModelMap
   */
  static void checkStateModelMap(StateTransitionHandlerFactory<? extends TransitionHandler> fty,
      Map<PartitionId, String> expectStateModelMap) {
    ResourceId resource = ResourceId.from("TestDB0");
    Assert.assertEquals(fty.getPartitionSet(resource).size(), expectStateModelMap.size());
    for (PartitionId partition : fty.getPartitionSet(resource)) {
      TransitionHandler stateModel = fty.getTransitionHandler(resource, partition);
      String actualState = stateModel.getCurrentState();
      String expectState = expectStateModelMap.get(partition);
      LOG.debug(partition + " actual state: " + actualState + ", expect state: " + expectState);
      Assert.assertEquals(actualState, expectState, "partition: " + partition
          + " should be in state: " + expectState + " but was " + actualState);
    }
  }
}
