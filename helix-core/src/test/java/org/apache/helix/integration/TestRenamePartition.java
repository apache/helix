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

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.strategy.DefaultTwoStateStrategy;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.testutil.TestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRenamePartition extends ZkTestBase {
  // map from clusterName to participants
  final Map<String, MockParticipant[]> _participantMap =
      new ConcurrentHashMap<String, MockParticipant[]>();

  // map from clusterName to controllers
  final Map<String, MockController> _controllerMap =
      new ConcurrentHashMap<String, MockController>();

  @Test()
  public void testRenamePartitionAutoIS() throws Exception {
    String clusterName = TestUtil.getTestName();
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant start port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    startAndVerify(clusterName);

    // rename partition name TestDB0_0 tp TestDB0_100
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();

    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));

    List<String> prioList = idealState.getRecord().getListFields().remove("TestDB0_0");
    idealState.getRecord().getListFields().put("TestDB0_100", prioList);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    stop(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testRenamePartitionCustomIS() throws Exception {

    String clusterName = TestUtil.getTestName();
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant start port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", false); // do rebalance

    // calculate idealState
    List<String> instanceNames =
        Arrays.asList("localhost_12918", "localhost_12919", "localhost_12920", "localhost_12921",
            "localhost_12922");
    ZNRecord destIS =
        DefaultTwoStateStrategy.calculateIdealState(instanceNames, 10, 3 - 1, "TestDB0", "MASTER",
            "SLAVE");
    IdealState idealState = new IdealState(destIS);
    idealState.setRebalanceMode(RebalanceMode.CUSTOMIZED);
    idealState.setReplicas("3");
    idealState.setStateModelDefId(StateModelDefId.from("MasterSlave"));

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();

    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    startAndVerify(clusterName);

    Map<String, String> stateMap = idealState.getRecord().getMapFields().remove("TestDB0_0");
    idealState.getRecord().getMapFields().put("TestDB0_100", stateMap);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    stop(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  private void startAndVerify(String clusterName) throws Exception {
    MockParticipant[] participants = new MockParticipant[5];

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    _participantMap.put(clusterName, participants);
    _controllerMap.put(clusterName, controller);
  }

  private void stop(String clusterName) {
    MockController controller = _controllerMap.get(clusterName);
    if (controller != null) {
      controller.syncStop();
    }

    MockParticipant[] participants = _participantMap.get(clusterName);
    if (participants != null) {
      for (MockParticipant participant : participants) {
        participant.syncStop();
      }
    }
  }
}
