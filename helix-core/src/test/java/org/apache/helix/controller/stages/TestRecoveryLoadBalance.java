package org.apache.helix.controller.stages;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestRecoveryLoadBalance extends BaseStageTest {

  private final String INPUT = "inputs";
  private final String CURRENT_STATE = "currentStates";
  private final String BEST_POSSIBLE_STATE = "bestPossibleStates";
  private final String EXPECTED_STATE = "expectedStates";
  private final String ERROR_OR_RECOVERY_PARTITION_THRESHOLD =
      "errorOrRecoveryPartitionThresholdForLoadBalance";
  private final String STATE_MODEL = "statemodel";
  private ClusterConfig _clusterConfig;

  @Test(dataProvider = "recoveryLoadBalanceInput")
  public void testRecoveryAndLoadBalance(String stateModelDef,
      int errorOrRecoveryPartitionThresholdForLoadBalance,
      Map<String, Map<String, Map<String, String>>> stateMapping) {
    System.out.println("START TestRecoveryLoadBalance at " + new Date(System.currentTimeMillis()));

    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 2;
    int nReplica = 3;

    Set<String> resourceSet = new HashSet<>();
    for (int i = 0; i < nResource; i++) {
      resourceSet.add(resourcePrefix + "_" + i);
    }

    preSetup(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE, resourceSet, nReplica,
        nReplica, stateModelDef);

    _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(
        errorOrRecoveryPartitionThresholdForLoadBalance);
    setClusterConfig(_clusterConfig);

    event.addAttribute(AttributeName.RESOURCES.name(), getResourceMap(
        resourceSet.toArray(new String[resourceSet.size()]), nPartition, stateModelDef));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), getResourceMap(
        resourceSet.toArray(new String[resourceSet.size()]), nPartition, stateModelDef));
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    // Initialize bestpossible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    IntermediateStateOutput expectedResult = new IntermediateStateOutput();

    for (String resource : resourceSet) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<>();
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);

        // Set input
        for (int r = 0; r < stateMapping.get(partition.toString()).get(CURRENT_STATE).size(); r++) {
          String instanceName = HOSTNAME_PREFIX + r;
          currentStateOutput.setCurrentState(resource, partition, instanceName,
              stateMapping.get(partition.toString()).get(CURRENT_STATE).get(instanceName));
        }
        for (int r = 0; r < stateMapping.get(partition.toString()).get(BEST_POSSIBLE_STATE)
            .size(); r++) {
          String instanceName = HOSTNAME_PREFIX + r;
          bestPossibleStateOutput.setState(resource, partition, instanceName,
              stateMapping.get(partition.toString()).get(BEST_POSSIBLE_STATE).get(instanceName));
        }
        for (int r = 0; r < stateMapping.get(partition.toString()).get(EXPECTED_STATE)
            .size(); r++) {
          String instanceName = HOSTNAME_PREFIX + r;
          expectedResult.setState(resource, partition, instanceName,
              stateMapping.get(partition.toString()).get(EXPECTED_STATE).get(instanceName));
        }

        // Set partitionMap
        for (int r = 0; r < nReplica; r++) {
          String instanceName = HOSTNAME_PREFIX + r;
          partitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
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
      // For debugging purposes
      // Object map1 = output.getPartitionStateMap(resource).getStateMap();
      // Object map2 = expectedResult.getPartitionStateMap(resource).getStateMap();

      // Note Assert.assertEquals won't work. If "actual" is an empty map, it won't compare
      // anything.
      Assert.assertTrue(output.getPartitionStateMap(resource).getStateMap()
          .equals(expectedResult.getPartitionStateMap(resource).getStateMap()));
    }

    System.out.println("END TestRecoveryLoadBalance at " + new Date(System.currentTimeMillis()));
  }

  @DataProvider(name = "recoveryLoadBalanceInput")
  public Object[][] rebalanceStrategies() {

    try {
      List<Object[]> data = new ArrayList<>();
      // Add data
      data.addAll(loadTestInputs("TestRecoveryLoadBalance.OnlineOffline.json"));
      data.addAll(loadTestInputs("TestRecoveryLoadBalance.MasterSlave.json"));

      Object[][] ret = new Object[data.size()][];
      for (int i = 0; i < data.size(); i++) {
        ret[i] = data.get(i);
      }
      return ret;
    } catch (Throwable e) {
      return new Object[][] {
          {}
      };
    }
  }

  public List<Object[]> loadTestInputs(String fileName) {
    List<Object[]> ret = new ArrayList<>();
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
    try {
      ObjectReader mapReader = new ObjectMapper().reader(List.class);
      List<Map<String, Object>> inputList = mapReader.readValue(inputStream);
      for (Map<String, Object> inputMap : inputList) {
        String stateModelName = (String) inputMap.get(STATE_MODEL);
        int threshold = (int) inputMap.get(ERROR_OR_RECOVERY_PARTITION_THRESHOLD);
        Map<String, Map<String, Map<String, String>>> stateMapping =
            (Map<String, Map<String, Map<String, String>>>) inputMap.get(INPUT);
        ret.add(new Object[] {
            stateModelName, threshold, stateMapping
        });
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ret;
  }

  private void preSetup(StateTransitionThrottleConfig.RebalanceType rebalanceType,
      Set<String> resourceSet, int numOfLiveInstances, int numOfReplicas, String stateModelName) {
    setupIdealState(numOfLiveInstances, resourceSet.toArray(new String[resourceSet.size()]),
        numOfLiveInstances, numOfReplicas, IdealState.RebalanceMode.FULL_AUTO, stateModelName);
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
