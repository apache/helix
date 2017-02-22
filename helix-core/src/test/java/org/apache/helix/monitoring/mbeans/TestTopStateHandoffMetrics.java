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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BaseStageTest;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Resource;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestTopStateHandoffMetrics extends BaseStageTest {
  public final static String TEST_INPUT_FILE = "TestTopStateHandoffMetrics.json";
  public final static String INITIAL_CURRENT_STATES = "initialCurrentStates";
  public final static String HANDOFF_CURRENT_STATES = "handoffCurrentStates";
  public final static String EXPECTED_DURATION = "expectedDuration";
  public final static String DATA_CACHE = "ClusterDataCache";
  public final static String TEST_RESOURCE = "TestResource";
  public final static String CLUSTER_STATUS_MONITOR = "clusterStatusMonitor";
  public final static String PARTITION = "PARTITION";


  public void preSetup() {
    setupLiveInstances(3);
    setupStateModel();
    Resource resource = new Resource(TEST_RESOURCE);
    resource.setStateModelDefRef("MasterSlave");
    resource.addPartition(PARTITION);
    event.addAttribute(AttributeName.RESOURCES.name(),
        Collections.singletonMap(TEST_RESOURCE, resource));
    event.addAttribute(CLUSTER_STATUS_MONITOR, new ClusterStatusMonitor("TestCluster"));
  }

  @Test(dataProvider = "successCurrentStateInput")
  public void testTopStateSuccessHandoff(Map<String, Map<String, String>> initialCurrentStates,
      Map<String, Map<String, String>> handOffCurrentStates, Long expectedDuration) {
    preSetup();
    runCurrentStage(initialCurrentStates, handOffCurrentStates);
    ClusterStatusMonitor clusterStatusMonitor = event.getAttribute(CLUSTER_STATUS_MONITOR);
    ResourceMonitor monitor = clusterStatusMonitor.getResourceMonitor(TEST_RESOURCE);

    // Should have 1 transition succeeded due to threshold.
    Assert.assertEquals(monitor.getSucceededTopStateHandoffCounter(), 1);

    // Duration should match the expected result
    Assert.assertEquals(monitor.getSuccessfulTopStateHandoffDurationCounter(), (long) expectedDuration);
    Assert.assertEquals(monitor.getMaxSinglePartitionTopStateHandoffDurationGauge(), (long) expectedDuration);
  }

  @Test(dataProvider = "failedCurrentStateInput")
  public void testTopStateFailedHandoff(Map<String, Map<String, String>> initialCurrentStates,
      Map<String, Map<String, String>> handOffCurrentStates, Long expectedDuration) {
    preSetup();
    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.setMissTopStateDurationThreshold(5000L);
    setClusterConfig(clusterConfig);
    runCurrentStage(initialCurrentStates, handOffCurrentStates);
    ClusterStatusMonitor clusterStatusMonitor = event.getAttribute(CLUSTER_STATUS_MONITOR);
    ResourceMonitor monitor = clusterStatusMonitor.getResourceMonitor(TEST_RESOURCE);

    // Should have 1 transition failed due to threshold.
    Assert.assertEquals(monitor.getFailedTopStateHandoffCounter(), 1);

    // No duration updated.
    Assert.assertEquals(monitor.getSuccessfulTopStateHandoffDurationCounter(), (long)expectedDuration);
    Assert.assertEquals(monitor.getMaxSinglePartitionTopStateHandoffDurationGauge(), (long) expectedDuration);
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
      for (int i = 0; i < inputs.size(); i++) {
        Map<String, Map<String, String>> intialCurrentStates =
            (Map<String, Map<String, String>>) inputs.get(i).get(INITIAL_CURRENT_STATES);
        Map<String, Map<String, String>> handoffCurrentStates =
            (Map<String, Map<String, String>>) inputs.get(i).get(HANDOFF_CURRENT_STATES);
        Long expectedDuration = Long.parseLong((String) inputs.get(i).get(EXPECTED_DURATION));

        inputData[i] = new Object[] { intialCurrentStates, handoffCurrentStates, expectedDuration };
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
      Map<String, Map<String, String>> handOffCurrentStates) {
    setupCurrentStates(generateCurrentStateMap(initialCurrentStates));
    runStage(event, new ReadClusterDataStage());
    runStage(event, new CurrentStateComputationStage());
    setupCurrentStates(generateCurrentStateMap(handOffCurrentStates));
    runStage(event, new ReadClusterDataStage());
    runStage(event, new CurrentStateComputationStage());
  }
}
