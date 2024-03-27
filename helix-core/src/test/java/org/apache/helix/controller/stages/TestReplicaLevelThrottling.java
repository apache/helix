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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestReplicaLevelThrottling extends BaseStageTest {
  static final String CLUSTER_NAME = "TestCluster";
  static final String RESOURCE_NAME = "TestResource";
  static final String NOT_SET = "-1";
  static final String DEFAULT_ERROR_THRESHOLD = String.valueOf(Integer.MAX_VALUE);

  @Test(dataProvider = "replicaLevelThrottlingInput")
  public void testPerReplicaThrottling(ClusterEvent event, Map<String, Map<String, String>> expectedOutput,
      Map<String, Object> cacheMap, Mock mock) {
    prepareCache(cacheMap, mock);
    runStage(event, new IntermediateStateCalcStage());
    Assert.assertTrue(matches(event, expectedOutput));
  }

  // Prepare the cache since it is well encapsulated, there is no way to set the cache things.
  // Also if we move this piece in data loading, the temporary created mock object and ClusterConfig will be overrided
  // by following test cases data.
  private void prepareCache(Map<String, Object> cacheMap, Mock mock) {
    when(mock.cache.getClusterConfig()).thenReturn((ClusterConfig) cacheMap.get(CacheKeys.clusterConfig.name()));
    when(mock.cache.getStateModelDef((String) cacheMap.get(CacheKeys.stateModelName.name()))).thenReturn(
        (StateModelDefinition) cacheMap.get(CacheKeys.stateModelDef.name()));
    when(mock.cache.getEnabledLiveInstances()).thenReturn(new HashSet<>(
        ((Map<String, List<String>>) cacheMap.get(CacheKeys.preferenceList.name())).values().iterator().next()));
    when(mock.cache.getLiveInstances()).thenReturn(new HashSet<>(
        ((Map<String, List<String>>) cacheMap.get(CacheKeys.preferenceList.name())).values().iterator().next()).stream()
        .collect(Collectors.toMap(e -> e, e -> new LiveInstance(e))));
    when(mock.cache.getIdealState(RESOURCE_NAME)).thenReturn(
        new FullAutoModeISBuilder(RESOURCE_NAME).setMinActiveReplica(
            (Integer) cacheMap.get(CacheKeys.minActiveReplica.name()))
            .setNumReplica((Integer) cacheMap.get(CacheKeys.numReplica.name()))
            .setStateModel((String) cacheMap.get(CacheKeys.stateModelName.name()))
            .setNumPartitions(2)
            .setRebalancerMode(IdealState.RebalanceMode.FULL_AUTO)
            .build());
  }

  private boolean matches(ClusterEvent event, Map<String, Map<String, String>> expectedOutPut) {
    Map<Partition, Map<String, String>> intermediateResult =
        ((IntermediateStateOutput) event.getAttribute(AttributeName.INTERMEDIATE_STATE.name())).getPartitionStateMap(
            RESOURCE_NAME).getStateMap();
    for (Partition partition : intermediateResult.keySet()) {
      if (!expectedOutPut.containsKey(partition.getPartitionName()) || !expectedOutPut.get(partition.getPartitionName())
          .equals(intermediateResult.get(partition))) {
        return false;
      }
    }
    return true;
  }

  @DataProvider(name = "replicaLevelThrottlingInput")
  public Object[][] rebalanceStrategies() {
    List<Object[]> data = new ArrayList<>();
    data.addAll(loadTestInputs("TestReplicaLevelThrottling.SingleTopState.json"));
    data.addAll(loadTestInputs("TestReplicaLevelThrottling.MultiTopStates.json"));

    Object[][] ret = new Object[data.size()][];
    for (int i = 0; i < data.size(); i++) {
      ret[i] = data.get(i);
    }
    return ret;
  }

  enum Entry {
    stateModel,
    numReplica,
    minActiveReplica,
    testCases,

    // Per Test
    partitionNames,
    messageOutput,           // instance -> target state of message
    bestPossible,
    preferenceList,
    clusterThrottleLoad,
    resourceThrottleLoad,
    instanceThrottleLoad,
    instanceThrottleRecovery,
    currentStates,
    pendingMessages,
    expectedOutput,
    errorThreshold
  }

  enum CacheKeys {
    clusterConfig,
    stateModelName,
    stateModelDef,
    minActiveReplica,
    numReplica,
    preferenceList
  }

  public List<Object[]> loadTestInputs(String fileName) {
    List<Object[]> ret = null;
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
    try {
      ObjectReader mapReader = new ObjectMapper().reader(Map.class);
      Map<String, Object> inputMaps = mapReader.readValue(inputStream);
      String stateModelName = (String) inputMaps.get(Entry.stateModel.name());

      StateModelDefinition stateModelDef =
          BuiltInStateModelDefinitions.valueOf(stateModelName).getStateModelDefinition();
      int minActiveReplica = Integer.parseInt((String) inputMaps.get(Entry.minActiveReplica.name()));
      int numReplica = Integer.parseInt((String) inputMaps.get(Entry.numReplica.name()));
      List<Map<String, Object>> inputs = (List<Map<String, Object>>) inputMaps.get(Entry.testCases.name());
      ret = new ArrayList<>();
      Mock mock = new Mock();
      for (Map<String, Object> inMap : inputs) {
        Resource resource = new Resource(RESOURCE_NAME);
        CurrentStateOutput currentStateOutput = new CurrentStateOutput();
        MessageOutput messageOutput = new MessageOutput();
        BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
        Map<String, List<String>> preferenceLists = (Map<String, List<String>>) inMap.get(Entry.preferenceList.name());
        Map<String, Map<String, String>> pendingMessages =
            (Map<String, Map<String, String>>) inMap.get(Entry.pendingMessages.name());
        Map<String, Map<String, String>> currentStates =
            (Map<String, Map<String, String>>) inMap.get(Entry.currentStates.name());
        Map<String, Map<String, String>> bestPossible =
            (Map<String, Map<String, String>>) inMap.get(Entry.bestPossible.name());
        Map<String, Map<String, String>> messageMap =
            (Map<String, Map<String, String>>) inMap.get(Entry.messageOutput.name());
        for (String partition : (List<String>) inMap.get(Entry.partitionNames.name())) {
          resource.addPartition(partition);
          bestPossibleStateOutput.setPreferenceList(RESOURCE_NAME, partition, preferenceLists.get(partition));
          bestPossibleStateOutput.setState(RESOURCE_NAME, resource.getPartition(partition),
              bestPossible.get(partition));
          List<Message> messages = generateMessages(messageMap.get(partition), currentStates.get(partition));
          messageOutput.addMessages(RESOURCE_NAME, resource.getPartition(partition), messages);
          currentStates.get(partition)
              .entrySet()
              .forEach(
                  e -> currentStateOutput.setCurrentState(resource.getResourceName(), resource.getPartition(partition),
                      e.getKey(), e.getValue()));
          generateMessages(pendingMessages.get(partition), currentStates.get(partition)).forEach(
              m -> currentStateOutput.setPendingMessage(resource.getResourceName(), resource.getPartition(partition),
                  m.getTgtName(), m));
        }
        ClusterEvent event = new ClusterEvent(CLUSTER_NAME, ClusterEventType.Unknown);
        event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput); // add current states
        event.addAttribute(AttributeName.ControllerDataProvider.name(),
            buildCache(mock, numReplica, minActiveReplica, stateModelDef, stateModelName, preferenceLists));
        event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageOutput);
        event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
        event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
            Collections.singletonMap(RESOURCE_NAME, resource));
        Map<String, Map<String, String>> expectedOutput =
            (Map<String, Map<String, String>>) inMap.get(Entry.expectedOutput.name());

        // Build throttle configs
        ClusterConfig clusterConfig = new ClusterConfig(CLUSTER_NAME);
        List<StateTransitionThrottleConfig> throttleConfigs = new ArrayList<>();
        getSingleThrottleEntry(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.CLUSTER, Entry.clusterThrottleLoad.name(), throttleConfigs,
            inMap);
        getSingleThrottleEntry(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.RESOURCE, Entry.resourceThrottleLoad.name(), throttleConfigs,
            inMap);
        getSingleThrottleEntry(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, Entry.instanceThrottleLoad.name(), throttleConfigs,
            inMap);
        getSingleThrottleEntry(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, Entry.instanceThrottleRecovery.name(),
            throttleConfigs, inMap);
        clusterConfig.setStateTransitionThrottleConfigs(throttleConfigs);
        clusterConfig.setErrorPartitionThresholdForLoadBalance(Integer.parseInt(
            (String) inMap.getOrDefault(Entry.errorThreshold.name(), DEFAULT_ERROR_THRESHOLD)));

        Map<String, Object> cacheMap = new HashMap<>();
        cacheMap.put(CacheKeys.clusterConfig.name(), clusterConfig);
        cacheMap.put(CacheKeys.stateModelName.name(), stateModelName);
        cacheMap.put(CacheKeys.stateModelDef.name(), stateModelDef);
        cacheMap.put(CacheKeys.preferenceList.name(), preferenceLists);
        cacheMap.put(CacheKeys.minActiveReplica.name(), minActiveReplica);
        cacheMap.put(CacheKeys.numReplica.name(), numReplica);
        ret.add(new Object[]{event, expectedOutput, cacheMap, mock});
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return ret;
  }

  private List<Message> generateMessages(Map<String, String> messageMap, Map<String, String> currentStates) {
    if (messageMap == null || currentStates == null) {
      return Collections.emptyList();
    }
    List<Message> messages = new ArrayList<>();

    for (Map.Entry<String, String> entry : messageMap.entrySet()) {
      Message message = new Message(new ZNRecord(UUID.randomUUID().toString()));
      message.setFromState(currentStates.get(entry.getKey()));
      message.setToState(entry.getValue());
      message.setTgtName(entry.getKey());
      messages.add(message);
    }

    return messages;
  }

  private ResourceControllerDataProvider buildCache(Mock mock, int numReplica, int minActive,
      StateModelDefinition stateModelDefinition, String stateModel, Map<String, List<String>> preferenceLists) {

    return mock.cache;
  }

  private void getSingleThrottleEntry(StateTransitionThrottleConfig.RebalanceType rebalanceType,
      StateTransitionThrottleConfig.ThrottleScope throttleScope, String entryName,
      List<StateTransitionThrottleConfig> throttleConfigs, Map<String, Object> inMap) {
    if (!inMap.get(entryName).equals(NOT_SET)) {
      throttleConfigs.add(new StateTransitionThrottleConfig(rebalanceType, throttleScope,
          Integer.valueOf((String) inMap.get(entryName))));
    }
  }

  private final class Mock {
    private ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
  }
}
