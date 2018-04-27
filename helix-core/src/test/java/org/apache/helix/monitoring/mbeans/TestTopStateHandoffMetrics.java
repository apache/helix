package org.apache.helix.monitoring.mbeans;

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

import org.apache.helix.controller.stages.*;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Resource;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class TestTopStateHandoffMetrics extends BaseStageTest {
  public final static String TEST_INPUT_FILE = "TestTopStateHandoffMetrics.json";
  public final static String INITIAL_CURRENT_STATES = "initialCurrentStates";
  public final static String MISSING_TOP_STATES = "MissingTopStates";
  public final static String HANDOFF_CURRENT_STATES = "handoffCurrentStates";
  public final static String EXPECTED_DURATION = "expectedDuration";
  public final static String TEST_RESOURCE = "TestResource";
  public final static String PARTITION = "PARTITION";

  public void preSetup() {
    setupLiveInstances(3);
    setupStateModel();
    Resource resource = new Resource(TEST_RESOURCE);
    resource.setStateModelDefRef("MasterSlave");
    resource.addPartition(PARTITION);
    event.addAttribute(AttributeName.RESOURCES.name(),
        Collections.singletonMap(TEST_RESOURCE, resource));
    ClusterStatusMonitor monitor = new ClusterStatusMonitor("TestCluster");
    monitor.active();
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), monitor);
  }

  @Test(dataProvider = "successCurrentStateInput")
  public void testTopStateSuccessHandoff(Map<String, Map<String, String>> initialCurrentStates,
      Map<String, Map<String, String>> missingTopStates,
      Map<String, Map<String, String>> handOffCurrentStates, Long expectedDuration) {
    preSetup();
    runCurrentStage(initialCurrentStates, missingTopStates, handOffCurrentStates, null);
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceMonitor monitor = clusterStatusMonitor.getResourceMonitor(TEST_RESOURCE);

    // Should have 1 transition succeeded due to threshold.
    Assert.assertEquals(monitor.getSucceededTopStateHandoffCounter(), 1);
    Assert.assertEquals(monitor.getFailedTopStateHandoffCounter(), 0);

    // Duration should match the expected result
    Assert.assertEquals(monitor.getSuccessfulTopStateHandoffDurationCounter(),
        (long) expectedDuration);
    Assert.assertEquals(monitor.getMaxSinglePartitionTopStateHandoffDurationGauge(),
        (long) expectedDuration);
  }

  @Test(dataProvider = "failedCurrentStateInput")
  public void testTopStateFailedHandoff(Map<String, Map<String, String>> initialCurrentStates,
      Map<String, Map<String, String>> missingTopStates,
      Map<String, Map<String, String>> handOffCurrentStates, Long expectedDuration) {
    preSetup();
    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.setMissTopStateDurationThreshold(5000L);
    setClusterConfig(clusterConfig);
    runCurrentStage(initialCurrentStates, missingTopStates, handOffCurrentStates, null);
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceMonitor monitor = clusterStatusMonitor.getResourceMonitor(TEST_RESOURCE);

    // Should have 1 transition failed due to threshold.
    Assert.assertEquals(monitor.getSucceededTopStateHandoffCounter(), 0);
    Assert.assertEquals(monitor.getFailedTopStateHandoffCounter(), 1);

    // No duration updated.
    Assert.assertEquals(monitor.getSuccessfulTopStateHandoffDurationCounter(),
        (long) expectedDuration);
    Assert.assertEquals(monitor.getMaxSinglePartitionTopStateHandoffDurationGauge(),
        (long) expectedDuration);
  }

  // Test handoff that are triggered by an offline master instance
  @Test(dataProvider = "successCurrentStateInput", dependsOnMethods = "testTopStateSuccessHandoff")
  public void testTopStateSuccessHandoffWithOfflineNode(
      final Map<String, Map<String, String>> initialCurrentStates,
      final Map<String, Map<String, String>> missingTopStates,
      Map<String, Map<String, String>> handOffCurrentStates, Long expectedDuration) {
    final long offlineTimeBeforeMasterless = 125;

    preSetup();
    runCurrentStage(initialCurrentStates, missingTopStates, handOffCurrentStates,
        new MissingStatesDataCacheInject() {
          @Override
          public void doInject(ClusterDataCache cache) {
            Set<String> topStateNodes = new HashSet<>();
            for (String instance : initialCurrentStates.keySet()) {
              if (initialCurrentStates.get(instance).get("CurrentState").equals("MASTER")) {
                topStateNodes.add(instance);
              }
            }
            // Simulate the previous top state instance goes offline
            if (!topStateNodes.isEmpty()) {
              cache.getLiveInstances().keySet().removeAll(topStateNodes);
              for (String topStateNode : topStateNodes) {
                long originalStartTime =
                    Long.parseLong(missingTopStates.get(topStateNode).get("StartTime"));
                cache.getInstanceOfflineTimeMap()
                    .put(topStateNode, originalStartTime - offlineTimeBeforeMasterless);
              }
            }
          }
        });
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceMonitor monitor = clusterStatusMonitor.getResourceMonitor(TEST_RESOURCE);

    // Should have 1 transition succeeded due to threshold.
    Assert.assertEquals(monitor.getSucceededTopStateHandoffCounter(), 1);
    Assert.assertEquals(monitor.getFailedTopStateHandoffCounter(), 0);

    // Duration should match the expected result
    Assert.assertEquals(monitor.getSuccessfulTopStateHandoffDurationCounter(),
        expectedDuration + offlineTimeBeforeMasterless);
    Assert.assertEquals(monitor.getMaxSinglePartitionTopStateHandoffDurationGauge(),
        expectedDuration + offlineTimeBeforeMasterless);
  }

  // Test success with no available clue about previous master.
  // For example, controller is just changed to a new node.
  @Test(dataProvider = "successCurrentStateInput", dependsOnMethods = "testTopStateSuccessHandoff")
  public void testHandoffDurationWithDefaultStartTime(
      Map<String, Map<String, String>> initialCurrentStates,
      Map<String, Map<String, String>> missingTopStates,
      Map<String, Map<String, String>> handOffCurrentStates, Long expectedDuration) {
    preSetup();

    // reset expectedDuration since current system time would be used
    for (Map<String, String> states : handOffCurrentStates.values()) {
      if (states.get("CurrentState").equals("MASTER")) {
        expectedDuration = Long.parseLong(states.get("EndTime")) - System.currentTimeMillis();
        break;
      }
    }

    // No initialCurrentStates means no input can be used as the clue of the previous master.
    runCurrentStage(Collections.EMPTY_MAP, missingTopStates, handOffCurrentStates, null);
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceMonitor monitor = clusterStatusMonitor.getResourceMonitor(TEST_RESOURCE);

    // Should have 1 transition succeeded due to threshold.
    Assert.assertEquals(monitor.getSucceededTopStateHandoffCounter(), 1);
    Assert.assertEquals(monitor.getFailedTopStateHandoffCounter(), 0);

    // Duration should match the expected result.
    // Note that the gap between expectedDuration calculated and monitor calculates the value should be allowed
    Assert.assertTrue(
        expectedDuration - monitor.getSuccessfulTopStateHandoffDurationCounter() < 1500);
    Assert.assertTrue(
        expectedDuration - monitor.getMaxSinglePartitionTopStateHandoffDurationGauge() < 1500);
  }

  /**
   * Test success with only a pending message as the clue.
   * For instance, if the master was dropped, there is no way to track the dropping time.
   * So either use current system time.
   *
   * @see org.apache.helix.monitoring.mbeans.TestTopStateHandoffMetrics#testHandoffDurationWithDefaultStartTime
   * Or we can check if any pending message to be used as the start time.
   */
  @Test(dataProvider = "successCurrentStateInput", dependsOnMethods = "testTopStateSuccessHandoff")
  public void testHandoffDurationWithPendingMessage(
      final Map<String, Map<String, String>> initialCurrentStates,
      final Map<String, Map<String, String>> missingTopStates,
      Map<String, Map<String, String>> handOffCurrentStates, Long expectedDuration) {
    final long messageTimeBeforeMasterless = 145;
    preSetup();
    // No initialCurrentStates means no input can be used as the clue of the previous master.
    runCurrentStage(Collections.EMPTY_MAP, missingTopStates, handOffCurrentStates,
        new MissingStatesDataCacheInject() {
          @Override
          public void doInject(ClusterDataCache cache) {
            String topStateNode = null;
            for (String instance : initialCurrentStates.keySet()) {
              if (initialCurrentStates.get(instance).get("CurrentState").equals("MASTER")) {
                topStateNode = instance;
                break;
              }
            }
            // Simulate the previous top state instance goes offline
            if (topStateNode != null) {
              long originalStartTime =
                  Long.parseLong(missingTopStates.get(topStateNode).get("StartTime"));
              // Inject a message that fit expectedDuration
              Message message =
                  new Message(Message.MessageType.STATE_TRANSITION, "thisisafakemessage");
              message.setTgtSessionId(SESSION_PREFIX + topStateNode.split("_")[1]);
              message.setToState("MASTER");
              message.setCreateTimeStamp(originalStartTime - messageTimeBeforeMasterless);
              message.setTgtName(topStateNode);
              message.setResourceName(TEST_RESOURCE);
              message.setPartitionName(PARTITION);
              cache.cacheMessages(Collections.singletonList(message));
            }
          }
        });

    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceMonitor monitor = clusterStatusMonitor.getResourceMonitor(TEST_RESOURCE);

    // Should have 1 transition succeeded due to threshold.
    Assert.assertEquals(monitor.getSucceededTopStateHandoffCounter(), 1);
    Assert.assertEquals(monitor.getFailedTopStateHandoffCounter(), 0);

    // Duration should match the expected result
    Assert.assertEquals(monitor.getSuccessfulTopStateHandoffDurationCounter(),
        expectedDuration + messageTimeBeforeMasterless);
    Assert.assertEquals(monitor.getMaxSinglePartitionTopStateHandoffDurationGauge(),
        expectedDuration + messageTimeBeforeMasterless);
  }

  private final String CURRENT_STATE = "CurrentState";
  private final String PREVIOUS_STATE = "PreviousState";
  private final String START_TIME = "StartTime";
  private final String END_TIME = "EndTime";

  @DataProvider(name = "successCurrentStateInput")
  public Object[][] successCurrentState() {
    return loadInputData("succeeded");
  }

  @DataProvider(name = "failedCurrentStateInput")
  public Object[][] failedCurrentState() {
    return loadInputData("failed");
  }

  private Object[][] loadInputData(String inputEntry) {
    Object[][] inputData = null;
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(TEST_INPUT_FILE);

    try {
      ObjectReader mapReader = new ObjectMapper().reader(Map.class);
      Map<String, Object> inputMaps = mapReader.readValue(inputStream);

      List<Map<String, Object>> inputs = (List<Map<String, Object>>) inputMaps.get(inputEntry);
      inputData = new Object[inputs.size()][];
      for (int i = 0; i < inputData.length; i++) {
        Map<String, Map<String, String>> intialCurrentStates =
            (Map<String, Map<String, String>>) inputs.get(i).get(INITIAL_CURRENT_STATES);
        Map<String, Map<String, String>> missingTopStates =
            (Map<String, Map<String, String>>) inputs.get(i).get(MISSING_TOP_STATES);
        Map<String, Map<String, String>> handoffCurrentStates =
            (Map<String, Map<String, String>>) inputs.get(i).get(HANDOFF_CURRENT_STATES);
        Long expectedDuration = Long.parseLong((String) inputs.get(i).get(EXPECTED_DURATION));

        inputData[i] = new Object[] { intialCurrentStates, missingTopStates, handoffCurrentStates,
            expectedDuration
        };
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return inputData;
  }

  private Map<String, CurrentState> generateCurrentStateMap(
      Map<String, Map<String, String>> currentStateRawData) {
    Map<String, CurrentState> currentStateMap = new HashMap<String, CurrentState>();
    for (String instanceName : currentStateRawData.keySet()) {
      Map<String, String> propertyMap = currentStateRawData.get(instanceName);
      CurrentState currentState = new CurrentState(TEST_RESOURCE);
      currentState.setSessionId(SESSION_PREFIX + instanceName.split("_")[1]);
      currentState.setState(PARTITION, propertyMap.get(CURRENT_STATE));
      currentState.setPreviousState(PARTITION, propertyMap.get(PREVIOUS_STATE));
      currentState.setStartTime(PARTITION, Long.parseLong(propertyMap.get(START_TIME)));
      currentState.setEndTime(PARTITION, Long.parseLong(propertyMap.get(END_TIME)));
      currentStateMap.put(instanceName, currentState);
    }
    return currentStateMap;
  }

  private void runCurrentStage(Map<String, Map<String, String>> initialCurrentStates,
      Map<String, Map<String, String>> missingTopStates,
      Map<String, Map<String, String>> handOffCurrentStates,
      MissingStatesDataCacheInject testInjection) {

    if (initialCurrentStates != null && !initialCurrentStates.isEmpty()) {
      setupCurrentStates(generateCurrentStateMap(initialCurrentStates));
      runStage(event, new ReadClusterDataStage());
      runStage(event, new CurrentStateComputationStage());
    }

    setupCurrentStates(generateCurrentStateMap(missingTopStates));
    runStage(event, new ReadClusterDataStage());
    if (testInjection != null) {
      ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
      testInjection.doInject(cache);
    }
    runStage(event, new CurrentStateComputationStage());

    setupCurrentStates(generateCurrentStateMap(handOffCurrentStates));
    runStage(event, new ReadClusterDataStage());
    runStage(event, new CurrentStateComputationStage());
  }

  interface MissingStatesDataCacheInject {
    void doInject(ClusterDataCache cache);
  }
}
