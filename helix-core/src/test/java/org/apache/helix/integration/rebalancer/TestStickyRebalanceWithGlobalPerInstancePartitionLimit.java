package org.apache.helix.integration.rebalancer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestStickyRebalanceWithGlobalPerInstancePartitionLimit extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 10;
    _numReplicas = 2;
    _numDbs = 1;
    _numPartitions = 4;
    super.beforeClass();
  }

  @AfterClass
  public void afterClass() throws Exception {
    /*
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLUSTER_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testGreedyRebalanceWithGlobalPerInstancePartitionLimit() throws InterruptedException {
    // Update cluster config and greedy rebalance strategy
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setGlobalMaxPartitionAllowedPerInstance(1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);
    IdealState idealState = _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    idealState.setRebalanceStrategy(
        "org.apache.helix.controller.rebalancer.strategy.StickyRebalanceStrategy");
    _gSetupTool.getClusterManagementTool()
        .setResourceIdealState(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, idealState);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, "NewDB", 2, "OnlineOffline",
        IdealState.RebalanceMode.FULL_AUTO.name(),
        "org.apache.helix.controller.rebalancer.strategy.StickyRebalanceStrategy");
    _gSetupTool.getClusterManagementTool().rebalance(CLUSTER_NAME, "NewDB", 1);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // Process instance -> number of assigned partitions
    ExternalView TGTDBView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    ExternalView newDBView =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, "NewDB");
    Map<String, Integer> instancePartitionCountMap = new HashMap<>();
    TGTDBView.getPartitionSet().stream()
        .forEach(partition -> TGTDBView.getStateMap(partition).keySet().forEach(instance -> {
          instancePartitionCountMap.put(instance,
              instancePartitionCountMap.getOrDefault(instance, 0) + 1);
        }));
    newDBView.getPartitionSet().stream()
        .forEach(partition -> newDBView.getStateMap(partition).keySet().forEach(instance -> {
          instancePartitionCountMap.put(instance,
              instancePartitionCountMap.getOrDefault(instance, 0) + 1);
        }));

    Assert.assertEquals(
        instancePartitionCountMap.values().stream().filter(count -> count != 1).count(), 0);
  }
}
