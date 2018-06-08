package org.apache.helix.integration.rebalancer.CrushRebalancers;

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
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCrushAutoRebalanceTopoplogyAwareDisabled extends TestCrushAutoRebalanceNonRack {

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (TestCrushAutoRebalanceNonRack.START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      _nodes.add(storageNodeName);
      String tag = "tag-" + i % 2;
      _gSetupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, storageNodeName, tag);
      _nodeToTagMap.put(storageNodeName, tag);
    }

    // start dummy participants
    for (String node : _nodes) {
      MockParticipantManager participant =
          new MockParticipantManager(ZkTestBase.ZK_ADDR, CLUSTER_NAME, node);
      participant.syncStart();
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZkTestBase.ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    enablePersistBestPossibleAssignment(ZkTestBase._gZkClient, CLUSTER_NAME, true);
  }

  @Test(dataProvider = "rebalanceStrategies")
  public void test(String rebalanceStrategyName,
      String rebalanceStrategyClass) throws Exception {
    super.test(rebalanceStrategyName, rebalanceStrategyClass);
  }

  @Test(dataProvider = "rebalanceStrategies", dependsOnMethods = "test")
  public void testWithInstanceTag(
      String rebalanceStrategyName, String rebalanceStrategyClass) throws Exception {
    super.testWithInstanceTag(rebalanceStrategyName, rebalanceStrategyClass);
  }

  @Test(dataProvider = "rebalanceStrategies", dependsOnMethods = { "test", "testWithInstanceTag"
  })
  public void testLackEnoughLiveInstances(String rebalanceStrategyName,
      String rebalanceStrategyClass) throws Exception {
    super.testLackEnoughLiveInstances(rebalanceStrategyName, rebalanceStrategyClass);
  }

  @Test(dataProvider = "rebalanceStrategies", dependsOnMethods = { "test", "testWithInstanceTag"
  })
  public void testLackEnoughInstances(String rebalanceStrategyName,
      String rebalanceStrategyClass) throws Exception {
    super.testLackEnoughInstances(rebalanceStrategyName, rebalanceStrategyClass);
  }
}
