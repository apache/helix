package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestIntermediateStateCalcStage extends BaseStageTest {
  private ClusterConfig _clusterConfig;

  @Test
  public void testNoStateMissing() {
    String resourcePrefix = "resource";
    int nResource = 4;
    int nPartition = 2;
    int nReplica = 3;

    Set<String> resourceSet = new HashSet<>();
    for (int i = 0; i < nResource; i++) {
      resourceSet.add(resourcePrefix + "_" + i);
    }

    preSetup(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE, resourceSet, nReplica,
        nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(), getResourceMap(
        resourceSet.toArray(new String[resourceSet.size()]), nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), getResourceMap(
        resourceSet.toArray(new String[resourceSet.size()]), nPartition, "OnlineOffline"));

    // Initialize bestpossible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    IntermediateStateOutput expectedResult = new IntermediateStateOutput();

    _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(1);
    setClusterConfig(_clusterConfig);

    for (String resource : resourceSet) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<>();
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        for (int r = 0; r < nReplica; r++) {
          String instanceName = HOSTNAME_PREFIX + r;
          partitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
          if (resource.endsWith("0")) {
            // Regular recovery balance
            currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
            // should be recovered:
            expectedResult.setState(resource, partition, instanceName, "ONLINE");
          } else if (resource.endsWith("1")) {
            // Regular load balance
            currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
            currentStateOutput.setCurrentState(resource, partition, instanceName + "-1", "OFFLINE");
            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
            // should be recovered:
            expectedResult.setState(resource, partition, instanceName, "ONLINE");
          } else if (resource.endsWith("2")) {
            // Recovery balance with transient states, should keep the current states in the output.
            currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
            bestPossibleStateOutput.setState(resource, partition, instanceName, "OFFLINE");
            // should be kept unchanged:
            expectedResult.setState(resource, partition, instanceName, "OFFLINE");
          } else if (resource.endsWith("3")) {
            // One unresolved error should not prevent recovery balance
            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
            if (p == 0) {
              if (r == 0) {
                currentStateOutput.setCurrentState(resource, partition, instanceName, "ERROR");
                bestPossibleStateOutput.setState(resource, partition, instanceName, "ERROR");
                // This partition is still ERROR
                expectedResult.setState(resource, partition, instanceName, "ERROR");
              } else {
                currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
                // Recovery balance
                expectedResult.setState(resource, partition, instanceName, "ONLINE");
              }
            } else {
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              currentStateOutput.setCurrentState(resource, partition, instanceName + "-1",
                  "OFFLINE");
              // load balance is throttled, so keep all current states
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
              // The following must be removed because now downward state transitions are allowed
              // expectedResult.setState(resource, partition, instanceName + "-1", "OFFLINE");
            }
          } else if (resource.endsWith("4")) {
            // Test that partitions with replicas to drop are dropping them when recovery is
            // happening for other partitions
            if (p == 0) {
              // This partition requires recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              // After recovery, it should be back ONLINE
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            } else {
              // Other partitions require dropping of replicas
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              currentStateOutput.setCurrentState(resource, partition, instanceName + "-1",
                  "OFFLINE");
              // BestPossibleState dictates that we only need one ONLINE replica
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName + "-1", "DROPPED");
              // So instanceName-1 will NOT be expected to show up in expectedResult
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
              expectedResult.setState(resource, partition, instanceName + "-1", "DROPPED");
            }
          } else if (resource.endsWith("5")) {
            // Test that load balance bringing up a new replica does NOT happen with a recovery
            // partition
            if (p == 0) {
              // Set up a partition requiring recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              // After recovery, it should be back ONLINE
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            } else {
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              // Check that load balance (bringing up a new node) did not take place
              bestPossibleStateOutput.setState(resource, partition, instanceName + "-1", "ONLINE");
              expectedResult.setState(resource, partition, instanceName, "ONLINE");

            }
          }
        }
      }
      bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);
    }

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    for (String resource : resourceSet) {
      // Note Assert.assertEquals won't work. If "actual" is an empty map, it won't compare
      // anything.
      Assert.assertTrue(output.getPartitionStateMap(resource).getStateMap()
          .equals(expectedResult.getPartitionStateMap(resource).getStateMap()));
    }
  }

  @Test
  public void testWithClusterConfigChange() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 2;
    int nReplica = 3;

    Set<String> resourceSet = new HashSet<>();
    for (int i = 0; i < nResource; i++) {
      resourceSet.add(resourcePrefix + "_" + i);
    }

    preSetup(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE, resourceSet,
        nReplica, nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(), getResourceMap(
        resourceSet.toArray(new String[resourceSet.size()]), nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), getResourceMap(
        resourceSet.toArray(new String[resourceSet.size()]), nPartition, "OnlineOffline"));

    // Initialize best possible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    IntermediateStateOutput expectedResult = new IntermediateStateOutput();

    for (String resource : resourceSet) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<>();
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        for (int r = 0; r < nReplica; r++) {
          String instanceName = HOSTNAME_PREFIX + r;
          partitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
          if (resource.endsWith("0")) {
            // Test that when the threshold is set at a number greater than the number of error and
            // recovery partitions, load balance DOES take place
            _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(Integer.MAX_VALUE);
            setClusterConfig(_clusterConfig);
            if (p == 0) {
              // Set up a partition requiring recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              // After recovery, it should be back ONLINE
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            } else {
              // Ensure we have at least one ONLINE replica so that this partition does not need
              // recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              expectedResult.setState(resource, partition, instanceName, "ONLINE");

              // This partition to bring up a replica (load balance will happen)
              bestPossibleStateOutput.setState(resource, partition, instanceName + "-1", "ONLINE");
              expectedResult.setState(resource, partition, instanceName + "-1", "ONLINE");
            }
          }
        }
      }
      bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);
    }

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    for (String resource : resourceSet) {
      // Note Assert.assertEquals won't work. If "actual" is an empty map, it won't compare
      // anything.
      Assert.assertTrue(output.getPartitionStateMap(resource).getStateMap()
          .equals(expectedResult.getPartitionStateMap(resource).getStateMap()));
    }
  }

  private void preSetup(StateTransitionThrottleConfig.RebalanceType rebalanceType,
      Set<String> resourceSet, int numOfLiveInstances, int numOfReplicas) {
    setupIdealState(numOfLiveInstances, resourceSet.toArray(new String[resourceSet.size()]),
        numOfLiveInstances, numOfReplicas, IdealState.RebalanceMode.FULL_AUTO, "OnlineOffline");
    setupStateModel();
    setupLiveInstances(numOfLiveInstances);

    // Set up cluster configs
    _clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    StateTransitionThrottleConfig throttleConfig = new StateTransitionThrottleConfig(rebalanceType,
        StateTransitionThrottleConfig.ThrottleScope.CLUSTER, Integer.MAX_VALUE);
    _clusterConfig.setStateTransitionThrottleConfigs(Collections.singletonList(throttleConfig));
    setClusterConfig(_clusterConfig);
  }
}
