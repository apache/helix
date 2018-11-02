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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestStateTransitionPrirority extends BaseStageTest {
  public static final String RESOURCE = "Resource";
  public static final String PARTITION = "Partition";

  // TODO : Reenable this when throttling enabled for recovery rebalance
  @Test(dataProvider = "ResourceLevelPriority", enabled = false)
  public void testResourceLevelPriorityForRecoveryBalance(
      Map<String, String> resourceMap, String priorityField, List<String> expectedPriority) {
    preSetup(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE, resourceMap.keySet(),
        priorityField, 10, 1);
    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resourceMap.keySet().toArray(new String[resourceMap.keySet().size()]), 1,
            "MasterSlave"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resourceMap.keySet().toArray(new String[resourceMap.keySet().size()]), 1,
            "MasterSlave"));

    // Initialize bestpossible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    for (String resource : resourceMap.keySet()) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      is.getRecord().setSimpleField(priorityField, resourceMap.get(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<String, List<String>>();
      Partition partition = new Partition(resource + "_0");
      String instanceName = HOSTNAME_PREFIX + resource.split("_")[1];
      partitionMap.put(partition.getPartitionName(),
          Collections.singletonList(instanceName));
      bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);
      bestPossibleStateOutput.setState(resource, partition, instanceName, "SLAVE");
      currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
    }


    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    runStage(event, new ReadClusterDataStage());

    // Keep update the current state.
    List<String> resourcePriority = new ArrayList<String>();
    for (int i = 0; i < resourceMap.size(); i++) {
      runStage(event, new IntermediateStateCalcStage());
      updateCurrentStatesForRecoveryBalance(resourcePriority, currentStateOutput);
    }

    Assert.assertEquals(resourcePriority, expectedPriority);
  }

  @Test(dataProvider = "ResourceLevelPriority")
  public void testResourceLevelPriorityForLoadBalance(
      Map<String, String> resourceMap, String priorityField, List<String> expectedPriority) {
    preSetup(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE, resourceMap.keySet(), priorityField,
        10, 1);
    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resourceMap.keySet().toArray(new String[resourceMap.keySet().size()]), 1,
            "MasterSlave"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resourceMap.keySet().toArray(new String[resourceMap.keySet().size()]), 1,
            "MasterSlave"));

    // Initialize bestpossible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    for (String resource : resourceMap.keySet()) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      is.getRecord().setSimpleField(priorityField, resourceMap.get(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<String, List<String>>();
      Partition partition = new Partition(resource + "_0");
      String instanceName = HOSTNAME_PREFIX + resource.split("_")[1];
      String nextInstanceName = HOSTNAME_PREFIX + (Integer.parseInt(resource.split("_")[1]) + 1);
      partitionMap.put(partition.getPartitionName(), Collections.singletonList(nextInstanceName));
      bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);
      bestPossibleStateOutput.setState(resource, partition, nextInstanceName, "MASTER");
      currentStateOutput.setCurrentState(resource, partition, instanceName, "MASTER");
    }

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    runStage(event, new ReadClusterDataStage());

    // Keep update the current state.
    List<String> resourcePriority = new ArrayList<String>();
    for (int i = 0; i < resourceMap.size(); i++) {
      runStage(event, new IntermediateStateCalcStage());
      updateCurrentStatesForLoadBalance(resourcePriority, currentStateOutput);
    }

    Assert.assertEquals(resourcePriority, expectedPriority);
  }

  @Test(dataProvider = "PartitionLevelPriority")
  public void testPartitionLevelPriority(String resourceName,
      Map<String, Map<String, String>> bestPossibleMap,
      Map<String, Map<String, String>> currentStateMap, List<String> preferenceList,
      List<String> expectedPriority) {
    preSetup(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
        new HashSet<String>(Arrays.asList(resourceName)), "no_field", 3, 3);

    // Add load rebalance throttle config
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    StateTransitionThrottleConfig throttleConfigForLoadRebalance =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.ANY,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 1);
    List<StateTransitionThrottleConfig> currentThrottleConfig =
        clusterConfig.getStateTransitionThrottleConfigs();
    currentThrottleConfig.add(throttleConfigForLoadRebalance);
    clusterConfig.setStateTransitionThrottleConfigs(currentThrottleConfig);
    setClusterConfig(clusterConfig);

    // Initialize best possible state, current state and resource map.
    Resource resource = new Resource(resourceName);
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    for (String partitionName : bestPossibleMap.keySet()) {
      Partition partition = new Partition(partitionName);
      bestPossibleStateOutput.setPreferenceList(resourceName, partitionName, preferenceList);
      for (String instanceName : bestPossibleMap.get(partitionName).keySet()) {
        bestPossibleStateOutput.setState(resourceName, partition, instanceName,
            bestPossibleMap.get(partitionName).get(instanceName));
        currentStateOutput.setCurrentState(resourceName, partition, instanceName,
            currentStateMap.get(partitionName).get(instanceName));
      }
      resource.addPartition(partitionName);
    }
    resource.setStateModelDefRef("MasterSlave");

    event.addAttribute(AttributeName.RESOURCES.name(),
        Collections.singletonMap(resourceName, resource));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        Collections.singletonMap(resourceName, resource));
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    runStage(event, new ReadClusterDataStage());

    // Keep update the current state.
    List<String> partitionPriority = new ArrayList<String>();
    for (int i = 0; i < bestPossibleMap.size(); i++) {
      runStage(event, new IntermediateStateCalcStage());
      updateCurrentStateForPartitionLevelPriority(partitionPriority, currentStateOutput,
          resourceName, bestPossibleMap);
    }

    Assert.assertEquals(partitionPriority, expectedPriority);
  }

  @DataProvider(name = "PartitionLevelPriority")
  private Object[][] loadPartitionInput() {
    return loadInputData(PARTITION);
  }


  @DataProvider(name = "ResourceLevelPriority")
  private Object[][] loadResourceInput() {
    return loadInputData(RESOURCE);
  }

  private static final String TEST_INPUT_FILE = "Test%sLevelPriority.json";
  private static final String PRIORITY_FIELD = "PriorityField";
  private static final String EXPECTED_PRIORITY = "ExpectedPriority";
  private static final String BEST_POSSIBLE_MAP = "BestPossibleMap";
  private static final String CURRENT_STATE_MAP = "CurrentStateMap";
  private static final String PREFERENCE_LIST = "PreferenceList";
  private Object[][] loadInputData(String inputEntry) {
    Object[][] inputData = null;
    InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream(String.format(TEST_INPUT_FILE, inputEntry));

    try {
      ObjectReader mapReader = new ObjectMapper().reader(Map.class);
      Map<String, Object> inputMaps = mapReader.readValue(inputStream);

      List<Map<String, Object>> inputs = (List<Map<String, Object>>) inputMaps.get(inputEntry);
      inputData = new Object[inputs.size()][];
      for (int i = 0; i < inputs.size(); i++) {
        if (inputEntry.equals(RESOURCE)) {
          Map<String, String> resourceMap =
              (Map<String, String>) inputs.get(i).get(RESOURCE + "Map");
          String priorityField = (String) inputs.get(i).get(PRIORITY_FIELD);
          List<String> expectedPriority = (List<String>) inputs.get(i).get(EXPECTED_PRIORITY);
          inputData[i] = new Object[] { resourceMap, priorityField, expectedPriority };
        } else if (inputEntry.equals(PARTITION)) {
          String resource = (String) inputs.get(i).get(RESOURCE);
          Map<String, Map<String, String>> bestPossibleMap =
              (Map<String, Map<String, String>>) inputs.get(i).get(BEST_POSSIBLE_MAP);
          Map<String, Map<String, String>> currentStateMap =
              (Map<String, Map<String, String>>) inputs.get(i).get(CURRENT_STATE_MAP);
          List<String> expectedPriority = (List<String>) inputs.get(i).get(EXPECTED_PRIORITY);
          List<String> preferenceList = (List<String>) inputs.get(i).get(PREFERENCE_LIST);

          inputData[i] = new Object[] { resource, bestPossibleMap, currentStateMap, preferenceList,
              expectedPriority
          };
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return inputData;
  }

  private void preSetup(StateTransitionThrottleConfig.RebalanceType rebalanceType,
      Set<String> resourceSet, String priorityField, int numOfLiveInstances, int numOfReplicas) {
    setupIdealState(numOfLiveInstances, resourceSet.toArray(new String[resourceSet.size()]),
        numOfLiveInstances, numOfReplicas, IdealState.RebalanceMode.FULL_AUTO, "MasterSlave");
    setupStateModel();
    setupLiveInstances(numOfLiveInstances);

    // Set up cluster configs
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    StateTransitionThrottleConfig throttleConfig = new StateTransitionThrottleConfig(rebalanceType,
        StateTransitionThrottleConfig.ThrottleScope.CLUSTER, 1);
    clusterConfig.setStateTransitionThrottleConfigs(Collections.singletonList(throttleConfig));
    clusterConfig.setResourcePriorityField(priorityField);
    setClusterConfig(clusterConfig);
  }

  private void updateCurrentStatesForRecoveryBalance(List<String> resourcePriority,
      CurrentStateOutput currentStateOutput) {
    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());
    for (PartitionStateMap partitionStateMap : output.getResourceStatesMap().values()) {
      String resourceName = partitionStateMap.getResourceName();
      Partition partition = new Partition(resourceName + "_0");
      String instanceName = HOSTNAME_PREFIX + resourceName.split("_")[1];
      if (partitionStateMap.getPartitionMap(partition).values().contains("SLAVE")
          && !resourcePriority.contains(resourceName)) {
        updateCurrentOutput(resourcePriority, currentStateOutput, resourceName, partition,
            instanceName, "SLAVE");
        break;
      }
    }
  }

  private void updateCurrentStatesForLoadBalance(List<String> resourcePriority,
      CurrentStateOutput currentStateOutput) {
    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());
    for (PartitionStateMap partitionStateMap : output.getResourceStatesMap().values()) {
      String resourceName = partitionStateMap.getResourceName();
      Partition partition = new Partition(resourceName + "_0");
      String oldInstance = HOSTNAME_PREFIX + resourceName.split("_")[1];
      String expectedInstance =
          HOSTNAME_PREFIX + (Integer.parseInt(resourceName.split("_")[1]) + 1);
      if (partitionStateMap.getPartitionMap(partition).containsKey(expectedInstance)
          && !resourcePriority.contains(resourceName)) {
        currentStateOutput.getCurrentStateMap(resourceName, partition).remove(oldInstance);
        updateCurrentOutput(resourcePriority, currentStateOutput, resourceName, partition,
            expectedInstance, "MASTER");
        break;
      }
    }
  }

  private void updateCurrentOutput(List<String> resourcePriority,
      CurrentStateOutput currentStateOutput, String resourceName, Partition partition,
      String instanceName, String state) {
    resourcePriority.add(resourceName);
    currentStateOutput.setCurrentState(resourceName, partition, instanceName, state);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
  }

  private void updateCurrentStateForPartitionLevelPriority(List<String> partitionPriority,
      CurrentStateOutput currentStateOutput, String resourceName,
      Map<String, Map<String, String>> bestPossibleMap) {
    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());
    PartitionStateMap partitionStateMap = output.getPartitionStateMap(resourceName);
    for (Partition partition : partitionStateMap.getStateMap().keySet()) {
      Map<String, String> instanceStateMap = bestPossibleMap.get(partition.getPartitionName());
      if (partitionStateMap.getPartitionMap(partition).equals(instanceStateMap)
          && !partitionPriority.contains(partition.getPartitionName())) {
        partitionPriority.add(partition.getPartitionName());
        for (String instanceName : instanceStateMap.keySet()) {
          currentStateOutput.setCurrentState(resourceName, partition, instanceName,
              instanceStateMap.get(instanceName));
        }
        break;
      }
    }
  }
}
