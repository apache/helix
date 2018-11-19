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

import com.google.common.collect.Range;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixConstants;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BaseStageTest;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.TopStateHandoffReportStage;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Resource;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestTopStateHandoffMetrics extends BaseStageTest {
  public final static String TEST_INPUT_FILE = "TestTopStateHandoffMetrics.json";
  public final static String TEST_RESOURCE = "TestResource";
  public final static String PARTITION = "PARTITION";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String NON_GRACEFUL_HANDOFF_DURATION = "PartitionTopStateNonGracefulHandoffGauge.Max";
  private static final String GRACEFUL_HANDOFF_DURATION = "PartitionTopStateHandoffDurationGauge.Max";
  private static final String HANDOFF_HELIX_LATENCY = "PartitionTopStateHandoffHelixLatencyGauge.Max";
  private static final Range<Long> DURATION_ZERO = Range.closed(0L, 0L);
  private TestConfig config;

  @BeforeClass
  public void beforeClass() {
    super.beforeClass();
    try {
      config = OBJECT_MAPPER
          .readValue(getClass().getClassLoader().getResourceAsStream(TEST_INPUT_FILE), TestConfig.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static class CurrentStateInfo {
    String currentState;
    String previousState;
    long startTime;
    long endTime;

    @JsonCreator
    public CurrentStateInfo(
        @JsonProperty("CurrentState") String cs,
        @JsonProperty("PreviousState") String ps,
        @JsonProperty("StartTime") long start,
        @JsonProperty("EndTime") long end
    ) {
      currentState = cs;
      previousState = ps;
      startTime = start;
      endTime = end;
    }
  }

  private static class TestCaseConfig {
    final Map<String, CurrentStateInfo> initialCurrentStates;
    final Map<String, CurrentStateInfo> currentStateWithMissingTopState;
    final Map<String, CurrentStateInfo> finalCurrentState;
    final long duration;
    final boolean isGraceful;
    final long helixLatency;

    @JsonCreator
    public TestCaseConfig(
        @JsonProperty("InitialCurrentStates") Map<String, CurrentStateInfo> initial,
        @JsonProperty("MissingTopStates") Map<String, CurrentStateInfo> missing,
        @JsonProperty("HandoffCurrentStates") Map<String, CurrentStateInfo> handoff,
        @JsonProperty("Duration") long d,
        @JsonProperty("HelixLatency") long helix,
        @JsonProperty("IsGraceful") boolean graceful
    ) {
      initialCurrentStates = initial;
      currentStateWithMissingTopState = missing;
      finalCurrentState = handoff;
      duration = d;
      helixLatency = helix;
      isGraceful = graceful;
    }
  }

  private static class TestConfig {
    final List<TestCaseConfig> succeeded;
    final List<TestCaseConfig> failed;
    final List<TestCaseConfig> fast;
    final List<TestCaseConfig> succeededNonGraceful;

    @JsonCreator
    public TestConfig(
        @JsonProperty("succeeded") List<TestCaseConfig> succeededCfg,
        @JsonProperty("failed") List<TestCaseConfig> failedCfg,
        @JsonProperty("fast") List<TestCaseConfig> fastCfg,
        @JsonProperty("succeededNonGraceful") List<TestCaseConfig> nonGraceful
    ) {
      succeeded = succeededCfg;
      failed = failedCfg;
      fast = fastCfg;
      succeededNonGraceful = nonGraceful;
    }
  }

  private void preSetup() {
    setupLiveInstances(3);
    setupStateModel();
    Resource resource = new Resource(TEST_RESOURCE);
    resource.setStateModelDefRef("MasterSlave");
    resource.addPartition(PARTITION);
    event.addAttribute(AttributeName.RESOURCES.name(),
        Collections.singletonMap(TEST_RESOURCE, resource));
    event.addAttribute(AttributeName.LastRebalanceFinishTimeStamp.name(),
        TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED);
    ClusterStatusMonitor monitor = new ClusterStatusMonitor("TestCluster");
    monitor.active();
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), monitor);
  }

  @Test(dataProvider = "successCurrentStateInput")
  public void testTopStateSuccessHandoff(TestCaseConfig cfg) {
    runTestWithNoInjection(cfg, false);
  }

  @Test(dataProvider = "fastCurrentStateInput")
  public void testFastTopStateHandoffWithNoMissingTopState(TestCaseConfig cfg) {
    runTestWithNoInjection(cfg, false);
  }

  @Test(dataProvider = "fastCurrentStateInput")
  public void testFastTopStateHandoffWithNoMissingTopStateAndOldInstanceCrash(TestCaseConfig cfg) {
    preSetup();
    event.addAttribute(AttributeName.LastRebalanceFinishTimeStamp.name(), 7500L);
    // By simulating last master instance crash, we now have:
    //  - M->S from 6000 to 7000
    //  - lastPipelineFinishTimestamp is 7500
    //  - S->M from 8000 to 9000
    // Therefore the recorded latency should be 9000 - 7500 = 1500, though original master crashed,
    // since this is a single top state handoff observed within 1 pipeline, we treat it as graceful,
    // and only record user latency for transiting to master
    Range<Long> expectedDuration = Range.closed(1500L, 1500L);
    Range<Long> expectedHelixLatency = Range.closed(500L, 500L);
    runStageAndVerify(
        cfg.initialCurrentStates, cfg.currentStateWithMissingTopState, cfg.finalCurrentState,
        new MissingStatesDataCacheInject() {
          @Override
          public void doInject(ClusterDataCache cache) {
            Map<String, LiveInstance> liMap = new HashMap<>(cache.getLiveInstances());
            liMap.remove("localhost_1");
            cache.setLiveInstances(new ArrayList<>(liMap.values()));
          }
        }, 1, 0,
        expectedDuration,
        DURATION_ZERO,
        expectedDuration, expectedHelixLatency
    );
    event.addAttribute(AttributeName.LastRebalanceFinishTimeStamp.name(),
        TopStateHandoffReportStage.TIMESTAMP_NOT_RECORDED);
  }

  @Test(dataProvider = "succeededNonGraceful")
  public void testTopStateSuccessfulYetNonGracefulHandoff(TestCaseConfig cfg) {
    // localhost_0 crashed at 15000
    // localhost_1 slave -> master started 20000, ended 22000, top state handoff = 7000
    preSetup();
    final String downInstance = "localhost_0";
    final Long lastOfflineTime = 15000L;
    Range<Long> expectedDuration = Range.closed(7000L, 7000L);
    runStageAndVerify(
        cfg.initialCurrentStates, cfg.currentStateWithMissingTopState, cfg.finalCurrentState,
        new MissingStatesDataCacheInject() {
          @Override
          public void doInject(ClusterDataCache cache) {
            accessor.removeProperty(accessor.keyBuilder().liveInstance(downInstance));
            Map<String, LiveInstance> liMap = new HashMap<>(cache.getLiveInstances());
            liMap.remove("localhost_0");
            cache.setLiveInstances(new ArrayList<>(liMap.values()));
            cache.getInstanceOfflineTimeMap().put("localhost_0", lastOfflineTime);
            cache.notifyDataChange(HelixConstants.ChangeType.LIVE_INSTANCE);
          }
        }, 1, 0,
        DURATION_ZERO, // graceful handoff duration should be 0
        expectedDuration, // we should have an record for non-graceful handoff
        expectedDuration, // max handoff should be same as non-graceful handoff
        DURATION_ZERO // we don't record user latency for non-graceful transition
    );
  }

  @Test(dataProvider = "failedCurrentStateInput")
  public void testTopStateFailedHandoff(TestCaseConfig cfg) {
    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.setMissTopStateDurationThreshold(5000L);
    setClusterConfig(clusterConfig);
    runTestWithNoInjection(cfg, true);
  }

  // Test success with no available clue about previous master.
  // For example, controller is just changed to a new node.
  @Test(
      dataProvider = "successCurrentStateInput",
      dependsOnMethods = "testHandoffDurationWithPendingMessage"
  )
  public void testHandoffDurationWithDefaultStartTime(final TestCaseConfig cfg) {
    preSetup();

    // No initialCurrentStates means no input can be used as the clue of the previous master.
    // in such case, reportTopStateMissing will use current system time as missing top state
    // start time, and we assume it is a graceful handoff, and only "to top state" user latency
    // will be recorded
    long helixLatency = 1000;
    long userLatency = 1000;
    for (CurrentStateInfo states : cfg.finalCurrentState.values()) {
      if (states.currentState.equals("MASTER")) {
        states.endTime = System.currentTimeMillis() + helixLatency + userLatency;
        states.startTime = System.currentTimeMillis() + helixLatency;
        break;
      }
    }

    // actual timestamp when running the stage will be later than current time, so the expected
    // helix latency will be less than the mocked helix latency
    runStageAndVerify(Collections.EMPTY_MAP, cfg.currentStateWithMissingTopState,
        cfg.finalCurrentState, null, 1, 0,
        Range.closed(0L, helixLatency + userLatency),
        DURATION_ZERO,
        Range.closed(0L, helixLatency + userLatency),
        Range.closed(0L, helixLatency)
    );
  }

  /**
   * Test success with only a pending message as the clue.
   * For instance, if the master was dropped, there is no way to track the dropping time.
   * So either use current system time.
   * @see org.apache.helix.monitoring.mbeans.TestTopStateHandoffMetrics#testHandoffDurationWithDefaultStartTime
   * Or we can check if any pending message to be used as the start time.
   */
  @Test(dataProvider = "successCurrentStateInput", dependsOnMethods = "testTopStateSuccessHandoff")
  public void testHandoffDurationWithPendingMessage(final TestCaseConfig cfg) {
    final long messageTimeBeforeMasterless = 145;
    preSetup();

    long durationToVerify = cfg.duration + messageTimeBeforeMasterless;
    long userLatency = 0;
    for (CurrentStateInfo info : cfg.finalCurrentState.values()) {
      if (info.currentState.equals("MASTER")) {
        userLatency = info.endTime - info.startTime;
      }
    }
    long helixLatency = durationToVerify - userLatency;

    // No initialCurrentStates means no input can be used as the clue of the previous master.
    // in this case, we will treat the handoff as graceful and only to-master user latency
    // will be recorded
    runStageAndVerify(
        Collections.EMPTY_MAP, cfg.currentStateWithMissingTopState, cfg.finalCurrentState,
        new MissingStatesDataCacheInject() {
          @Override public void doInject(ClusterDataCache cache) {
            String topStateNode = null;
            for (String instance : cfg.initialCurrentStates.keySet()) {
              if (cfg.initialCurrentStates.get(instance).currentState.equals("MASTER")) {
                topStateNode = instance;
                break;
              }
            }
            // Simulate the previous top state instance goes offline
            if (topStateNode != null) {
              long originalStartTime = cfg.currentStateWithMissingTopState.get(topStateNode).startTime;
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
        }, 1, 0,
        Range.closed(durationToVerify, durationToVerify),
        DURATION_ZERO,
        Range.closed(durationToVerify, durationToVerify),
        Range.closed(helixLatency, helixLatency));
  }

  @DataProvider(name = "successCurrentStateInput")
  public Object[][] successCurrentState() {
    return testCaseConfigListToObjectArray(config.succeeded);
  }

  @DataProvider(name = "failedCurrentStateInput")
  public Object[][] failedCurrentState() {
    return testCaseConfigListToObjectArray(config.failed);
  }

  @DataProvider(name = "fastCurrentStateInput")
  public Object[][] fastCurrentState() {
    return testCaseConfigListToObjectArray(config.fast);
  }

  @DataProvider(name = "succeededNonGraceful")
  public Object[][] nonGracefulCurrentState() {
    return testCaseConfigListToObjectArray(config.succeededNonGraceful);
  }

  private Object[][] testCaseConfigListToObjectArray(List<TestCaseConfig> configs) {
    Object[][] result = new Object[configs.size()][];
    for (int i = 0; i < configs.size(); i++) {
      result[i] = new Object[] {configs.get(i)};
    }
    return result;
  }

  private void runTestWithNoInjection(TestCaseConfig cfg, boolean expectFail) {
    preSetup();
    Range<Long> duration = Range.closed(cfg.duration, cfg.duration);
    Range<Long> expectedDuration = cfg.isGraceful ? duration : DURATION_ZERO;
    Range<Long> expectedNonGracefulDuration = cfg.isGraceful ? DURATION_ZERO : duration;
    Range<Long> expectedHelixLatency =
        cfg.isGraceful ? Range.closed(cfg.helixLatency, cfg.helixLatency) : DURATION_ZERO;
    runStageAndVerify(cfg.initialCurrentStates, cfg.currentStateWithMissingTopState,
        cfg.finalCurrentState, null, expectFail ? 0 : 1, expectFail ? 1 : 0, expectedDuration, expectedNonGracefulDuration,
        expectedDuration, expectedHelixLatency);
  }

  private Map<String, CurrentState> generateCurrentStateMap(
      Map<String, CurrentStateInfo> currentStateRawData) {
    Map<String, CurrentState> currentStateMap = new HashMap<String, CurrentState>();
    for (String instanceName : currentStateRawData.keySet()) {
      CurrentStateInfo info = currentStateRawData.get(instanceName);
      CurrentState currentState = new CurrentState(TEST_RESOURCE);
      currentState.setSessionId(SESSION_PREFIX + instanceName.split("_")[1]);
      currentState.setState(PARTITION, info.currentState);
      currentState.setPreviousState(PARTITION, info.previousState);
      currentState.setStartTime(PARTITION, info.startTime);
      currentState.setEndTime(PARTITION, info.endTime);
      currentStateMap.put(instanceName, currentState);
    }
    return currentStateMap;
  }

  private void runPipeLine(Map<String, CurrentStateInfo> initialCurrentStates,
      Map<String, CurrentStateInfo> missingTopStates,
      Map<String, CurrentStateInfo> handOffCurrentStates,
      MissingStatesDataCacheInject testInjection) {

    if (initialCurrentStates != null && !initialCurrentStates.isEmpty()) {
      doRunStages(initialCurrentStates, null);
    }

    if (missingTopStates != null && !missingTopStates.isEmpty()) {
      doRunStages(missingTopStates, testInjection);
    }

    if (handOffCurrentStates != null && !handOffCurrentStates.isEmpty()) {
      doRunStages(handOffCurrentStates, null);
    }
  }

  private void doRunStages(Map<String, CurrentStateInfo> currentStates,
      MissingStatesDataCacheInject clusterDataInjection) {
    setupCurrentStates(generateCurrentStateMap(currentStates));
    runStage(event, new ReadClusterDataStage());
    if (clusterDataInjection != null) {
      ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
      clusterDataInjection.doInject(cache);
    }
    runStage(event, new CurrentStateComputationStage());
    runStage(event, new TopStateHandoffReportStage());
  }

  private void runStageAndVerify(
      Map<String, CurrentStateInfo> initialCurrentStates,
      Map<String, CurrentStateInfo> missingTopStates,
      Map<String, CurrentStateInfo> handOffCurrentStates,
      MissingStatesDataCacheInject inject,
      int successCnt,
      int failCnt,
      Range<Long> expectedDuration,
      Range<Long> expectedNonGracefulDuration,
      Range<Long> expectedMaxDuration,
      Range<Long> expectedHelixLatency
  ) {
    runPipeLine(initialCurrentStates, missingTopStates, handOffCurrentStates, inject);
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    ResourceMonitor monitor = clusterStatusMonitor.getResourceMonitor(TEST_RESOURCE);

    Assert.assertEquals(monitor.getSucceededTopStateHandoffCounter(), successCnt);
    Assert.assertEquals(monitor.getFailedTopStateHandoffCounter(), failCnt);

    long graceful = monitor.getPartitionTopStateHandoffDurationGauge()
        .getAttributeValue(GRACEFUL_HANDOFF_DURATION).longValue();
    long nonGraceful = monitor.getPartitionTopStateNonGracefulHandoffDurationGauge()
        .getAttributeValue(NON_GRACEFUL_HANDOFF_DURATION).longValue();
    long helix = monitor.getPartitionTopStateHandoffHelixLatencyGauge()
        .getAttributeValue(HANDOFF_HELIX_LATENCY).longValue();
    long max = monitor.getMaxSinglePartitionTopStateHandoffDurationGauge();
    Assert.assertTrue(expectedDuration.contains(graceful));
    Assert.assertTrue(expectedNonGracefulDuration.contains(nonGraceful));
    Assert.assertTrue(expectedHelixLatency.contains(helix));
    Assert.assertTrue(expectedMaxDuration.contains(max));
  }

  interface MissingStatesDataCacheInject {
    void doInject(ClusterDataCache cache);
  }
}
