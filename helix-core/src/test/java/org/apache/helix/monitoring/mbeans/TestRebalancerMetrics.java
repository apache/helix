package org.apache.helix.monitoring.mbeans;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BaseStageTest;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.testng.Assert;
import org.testng.annotations.Test;

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

public class TestRebalancerMetrics extends BaseStageTest {

  @Test
  public void testRecoveryRebalanceMetrics() {
    System.out
        .println("START testRecoveryRebalanceMetrics at " + new Date(System.currentTimeMillis()));
    String resource = "testResourceName";

    int numPartition = 100;
    int numReplica = 3;
    int maxPending = 3;

    setupIdealState(5, new String[] {resource}, numPartition,
        numReplica, IdealState.RebalanceMode.FULL_AUTO,
        BuiltInStateModelDefinitions.MasterSlave.name());
    setupInstances(5);
    setupLiveInstances(5);
    setupStateModel();

    Map<String, Resource> resourceMap =
        getResourceMap(new String[] {resource}, numPartition,
            BuiltInStateModelDefinitions.MasterSlave.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new ResourceControllerDataProvider());
    ClusterStatusMonitor monitor = new ClusterStatusMonitor(_clusterName);
    monitor.active();
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), monitor);

    runStage(event, new ReadClusterDataStage());
    ResourceControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    setupThrottleConfig(cache.getClusterConfig(),
        StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE, maxPending);
    runStage(event, new BestPossibleStateCalcStage());
    runStage(event, new IntermediateStateCalcStage());

    ClusterStatusMonitor clusterStatusMonitor = event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceMonitor resourceMonitor = clusterStatusMonitor.getResourceMonitor(resource);

    Assert.assertEquals(resourceMonitor.getPendingRecoveryRebalancePartitionGauge(), numPartition);
    Assert.assertEquals(resourceMonitor.getRecoveryRebalanceThrottledPartitionGauge(),
        numPartition - maxPending);

    System.out
        .println("END testRecoveryRebalanceMetrics at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testLoadBalanceMetrics() {
    System.out
        .println("START testLoadBalanceMetrics at " + new Date(System.currentTimeMillis()));
    String resource = "testResourceName";

    int numPartition = 100;
    int numReplica = 3;
    int maxPending = 3;

    setupIdealState(5, new String[] {resource}, numPartition,
        numReplica, IdealState.RebalanceMode.FULL_AUTO,
        BuiltInStateModelDefinitions.MasterSlave.name());
    setupInstances(5);
    setupLiveInstances(4);
    setupStateModel();

    Map<String, Resource> resourceMap =
        getResourceMap(new String[] {resource}, numPartition,
            BuiltInStateModelDefinitions.MasterSlave.name());
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new ResourceControllerDataProvider());
    ClusterStatusMonitor monitor = new ClusterStatusMonitor(_clusterName);
    monitor.active();
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), monitor);

    runStage(event, new ReadClusterDataStage());
    runStage(event, new BestPossibleStateCalcStage());

    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    currentStateOutput = copyCurrentStateFromBestPossible(bestPossibleStateOutput, resource);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    setupLiveInstances(4);

    runStage(event, new ReadClusterDataStage());
    ResourceControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    setupThrottleConfig(cache.getClusterConfig(),
        StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE, maxPending);
    runStage(event, new BestPossibleStateCalcStage());
    runStage(event, new IntermediateStateCalcStage());

    ClusterStatusMonitor clusterStatusMonitor = event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceMonitor resourceMonitor = clusterStatusMonitor.getResourceMonitor(resource);

    long numPendingLoadBalance = resourceMonitor.getPendingLoadRebalancePartitionGauge();
    Assert.assertTrue(numPendingLoadBalance > 0);
    Assert.assertEquals(resourceMonitor.getLoadRebalanceThrottledPartitionGauge(),
        numPendingLoadBalance - maxPending);

    System.out
        .println("END testLoadBalanceMetrics at " + new Date(System.currentTimeMillis()));
  }

  private void setupThrottleConfig(ClusterConfig clusterConfig,
      StateTransitionThrottleConfig.RebalanceType rebalanceType, int maxPending) {
    StateTransitionThrottleConfig resourceThrottle =
        new StateTransitionThrottleConfig(rebalanceType,
            StateTransitionThrottleConfig.ThrottleScope.RESOURCE, maxPending);

    clusterConfig.setStateTransitionThrottleConfigs(Arrays.asList(resourceThrottle));
  }

  private CurrentStateOutput copyCurrentStateFromBestPossible(
      BestPossibleStateOutput bestPossibleStateOutput, String resource) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    PartitionStateMap partitionStateMap = bestPossibleStateOutput.getPartitionStateMap(resource);
    for (Partition partition : partitionStateMap.partitionSet()) {
      Map<String, String> stateMap = partitionStateMap.getPartitionMap(partition);
      for (String instance : stateMap.keySet()) {
        currentStateOutput.setCurrentState(resource, partition, instance, stateMap.get(instance));
      }
    }
    return currentStateOutput;
  }
}
