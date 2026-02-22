package org.apache.helix.integration.controller;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

import static org.apache.helix.monitoring.mbeans.ClusterStatusMonitor.CLUSTER_DN_KEY;


public class TestClusterMaintenanceMode extends TaskTestBase {
  private static final long TIMEOUT = 180 * 1000L;
  private MockParticipantManager _newInstance;
  private String newResourceAddedDuringMaintenanceMode =
      String.format("%s_%s", WorkflowGenerator.DEFAULT_TGT_DB, 1);
  private HelixDataAccessor _dataAccessor;
  private PropertyKey.Builder _keyBuilder;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 3;
    _numReplicas = 3;
    _numPartitions = 5;
    super.beforeClass();
    _dataAccessor = _manager.getHelixDataAccessor();
    _keyBuilder = _dataAccessor.keyBuilder();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_newInstance != null && _newInstance.isConnected()) {
      _newInstance.syncStop();
    }
    super.afterClass();
  }

  @Test
  public void testNotInMaintenanceMode() {
    boolean isInMaintenanceMode =
        _gSetupTool.getClusterManagementTool().isInMaintenanceMode(CLUSTER_NAME);
    Assert.assertFalse(isInMaintenanceMode);
  }

  @Test(dependsOnMethods = "testNotInMaintenanceMode")
  public void testInMaintenanceMode() {
    _gSetupTool.getClusterManagementTool().enableMaintenanceMode(CLUSTER_NAME, true, TestHelper.getTestMethodName());
    boolean isInMaintenanceMode = _gSetupTool.getClusterManagementTool().isInMaintenanceMode(CLUSTER_NAME);
    Assert.assertTrue(isInMaintenanceMode);
  }

  @Test(dependsOnMethods = "testInMaintenanceMode")
  public void testMaintenanceModeAddNewInstance() {
    _gSetupTool.getClusterManagementTool().enableMaintenanceMode(CLUSTER_NAME, true, TestHelper.getTestMethodName());
    ExternalView prevExternalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + 10);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
    _newInstance = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    _newInstance.syncStart();
    _gSetupTool.getClusterManagementTool().rebalance(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB,
        3);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView newExternalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(prevExternalView.getRecord().getMapFields(),
        newExternalView.getRecord().getMapFields());
  }

  @Test(dependsOnMethods = "testMaintenanceModeAddNewInstance")
  public void testMaintenanceModeAddNewResource() {
    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME,
        newResourceAddedDuringMaintenanceMode, 7, "MasterSlave",
        IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
    _gSetupTool.getClusterManagementTool().rebalance(CLUSTER_NAME,
        newResourceAddedDuringMaintenanceMode, 3);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, newResourceAddedDuringMaintenanceMode);
    Assert.assertNull(externalView);
  }

  @Test(dependsOnMethods = "testMaintenanceModeAddNewResource")
  public void testMaintenanceModeInstanceDown() {
    _participants[0].syncStop();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      Assert.assertTrue(stateMap.values().contains("MASTER"));
    }
  }

  @Test(dependsOnMethods = "testMaintenanceModeInstanceDown")
  public void testMaintenanceModeInstanceBack() {
    _participants[0] =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _participants[0].getInstanceName());
    _participants[0].syncStart();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      if (stateMap.containsKey(_participants[0].getInstanceName())) {
        Assert.assertEquals(stateMap.get(_participants[0].getInstanceName()), "SLAVE");
      }
    }
  }

  @Test(dependsOnMethods = "testMaintenanceModeInstanceBack")
  public void testExitMaintenanceModeNewResourceRecovery() {
    _gSetupTool.getClusterManagementTool().enableMaintenanceMode(CLUSTER_NAME, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, newResourceAddedDuringMaintenanceMode);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), 7);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      Assert.assertTrue(stateMap.values().contains("MASTER"));
    }
  }

  /**
   * Test that the auto-exit functionality works.
   */
  @Test(dependsOnMethods = "testExitMaintenanceModeNewResourceRecovery")
  public void testAutoExitMaintenanceMode() throws Exception {
    // Set the config for auto-exiting maintenance mode
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxOfflineInstancesAllowed(2);
    clusterConfig.setNumOfflineInstancesForAutoExit(1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Kill 3 instances
    for (int i = 0; i < 3; i++) {
      _participants[i].syncStop();
    }
    TestHelper.verify(() -> _dataAccessor.getChildNames(_keyBuilder.liveInstances()).size() == 0, 2000L);

    // Check that the cluster is in maintenance
    MaintenanceSignal maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(maintenanceSignal);

    // Now bring up 2 instances
    for (int i = 0; i < 2; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }
    TestHelper.verify(() -> _dataAccessor.getChildNames(_keyBuilder.liveInstances()).size() == 3, 2000L);

    // Check that the cluster is no longer in maintenance (auto-recovered)
    maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNull(maintenanceSignal);
  }

  @Test(dependsOnMethods = "testAutoExitMaintenanceMode")
  public void testNoAutoExitWhenManuallyPutInMaintenance() throws Exception {
    // Manually put the cluster in maintenance
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null,
        null);

    // Kill 2 instances, which makes it a total of 3 down instances
    for (int i = 0; i < 2; i++) {
      _participants[i].syncStop();
    }
    TestHelper.verify(() -> _dataAccessor.getChildNames(_keyBuilder.liveInstances()).size() == 0, 2000L);

    // Now bring up all instances
    for (int i = 0; i < 3; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }
    TestHelper.verify(() -> _dataAccessor.getChildNames(_keyBuilder.liveInstances()).size() == 3, 2000L);

    // The cluster should still be in maintenance because it was enabled manually
    MaintenanceSignal maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(maintenanceSignal);
  }

  /**
   * Test that manual triggering of maintenance mode overrides auto-enabled maintenance.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testNoAutoExitWhenManuallyPutInMaintenance")
  public void testManualEnablingOverridesAutoEnabling() throws Exception {
    // Exit maintenance mode manually
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);

    // Kill 3 instances, which would put cluster in maintenance automatically
    for (int i = 0; i < 3; i++) {
      _participants[i].syncStop();
    }
    TestHelper.verify(() -> _dataAccessor.getChildNames(_keyBuilder.liveInstances()).size() == 0, 2000L);

    // Check that maintenance signal was triggered by Controller
    MaintenanceSignal maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.CONTROLLER);

    // Manually enable maintenance mode with customFields
    Map<String, String> customFields = ImmutableMap.of("LDAP", "hulee", "JIRA", "HELIX-999",
        "TRIGGERED_BY", "SHOULD NOT BE RECORDED");
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null,
        customFields);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) != null, 2000L);

    // Check that maintenance mode has successfully overwritten with the right TRIGGERED_BY field
    maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.USER);
    for (Map.Entry<String, String> entry : customFields.entrySet()) {
      if (entry.getKey().equals("TRIGGERED_BY")) {
        Assert.assertEquals(maintenanceSignal.getRecord().getSimpleField(entry.getKey()), "USER");
      } else {
        Assert.assertEquals(maintenanceSignal.getRecord().getSimpleField(entry.getKey()),
            entry.getValue());
      }
    }
  }

  /**
   * Test that maxNumPartitionPerInstance still applies (if any Participant has more replicas than
   * the threshold, the cluster should not auto-exit maintenance mode).
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testManualEnablingOverridesAutoEnabling")
  public void testMaxPartitionLimit() throws Exception {
    // Manually exit maintenance mode
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) != null, 2000L);

    // Since 3 instances are missing, the cluster should have gone back under maintenance
    // automatically
    MaintenanceSignal maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.CONTROLLER);
    Assert.assertEquals(maintenanceSignal.getAutoTriggerReason(),
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);

    // Bring up all instances
    for (int i = 0; i < 3; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }
    TestHelper.verify(() -> _dataAccessor.getChildNames(_keyBuilder.liveInstances()).size() == 3, 2000L);

    // Check that the cluster exited maintenance
    maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNull(maintenanceSignal);

    // Kill 3 instances, which would put cluster in maintenance automatically
    for (int i = 0; i < 3; i++) {
      _participants[i].syncStop();
    }
    TestHelper.verify(() -> _dataAccessor.getChildNames(_keyBuilder.liveInstances()).size() == 0, 2000L);

    // Check that cluster is back under maintenance
    maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.CONTROLLER);
    Assert.assertEquals(maintenanceSignal.getAutoTriggerReason(),
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);

    // Set the cluster config for auto-exiting maintenance mode
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    // Setting MaxPartitionsPerInstance to 1 will prevent the cluster from exiting maintenance mode
    // automatically because the instances currently have more than 1
    clusterConfig.setMaxPartitionsPerInstance(1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);
    TestHelper.verify(
        () -> ((ClusterConfig) _dataAccessor.getProperty(_keyBuilder.clusterConfig())).getMaxPartitionsPerInstance() == 1,
        2000L);


    // Now bring up all instances
    for (int i = 0; i < 3; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }
    TestHelper.verify(() -> _dataAccessor.getChildNames(_keyBuilder.liveInstances()).size() == 3, 2000L);

    // Check that the cluster is still in maintenance (should not have auto-exited because it would
    // fail the MaxPartitionsPerInstance check)
    maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.CONTROLLER);
    Assert.assertEquals(maintenanceSignal.getAutoTriggerReason(),
        MaintenanceSignal.AutoTriggerReason.MAX_PARTITION_PER_INSTANCE_EXCEEDED);

    // Check if failed rebalance counter is updated
    boolean result = TestHelper.verify(() -> {
      try {
        Long value =
            (Long) _server.getAttribute(getMbeanName(CLUSTER_NAME), "RebalanceFailureCounter");
        return value != null && (value > 0);
      } catch (Exception e) {
        return false;
      }
    }, TIMEOUT);
    Assert.assertTrue(result);

    // Check failed continuous task rebalance counter is not updated
    result = TestHelper.verify(() -> {
      try {
        Long value = (Long) _server
            .getAttribute(getMbeanName(CLUSTER_NAME), "ContinuousTaskRebalanceFailureCount");
        return value != null && (value == 0);
      } catch (Exception e) {
        return false;
      }
    }, TIMEOUT);
    Assert.assertTrue(result);

    // Check if failed continuous resource rebalance counter is updated
    result = TestHelper.verify(() -> {
      try {
        Long value = (Long) _server
            .getAttribute(getMbeanName(CLUSTER_NAME), "ContinuousResourceRebalanceFailureCount");
        return value != null && (value > 0);
      } catch (Exception e) {
        return false;
      }
    }, TIMEOUT);
    Assert.assertTrue(result);
  }


  private ObjectName getMbeanName(String clusterName) throws MalformedObjectNameException {
    String clusterBeanName = String.format("%s=%s", CLUSTER_DN_KEY, clusterName);
    return new ObjectName(
        String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), clusterBeanName));
  }

  /**
   * Test that the Controller correctly records maintenance history in various situations.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testMaxPartitionLimit")
  public void testMaintenanceHistory() throws Exception {
    // In maintenance mode, by controller, for MAX_PARTITION_PER_INSTANCE_EXCEEDED
    ControllerHistory history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    Map<String, String> lastHistoryEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));

    // **The KV pairs are hard-coded in here for the ease of reading!**
    Assert.assertEquals(lastHistoryEntry.get("OPERATION_TYPE"), "ENTER");
    Assert.assertEquals(lastHistoryEntry.get("TRIGGERED_BY"), "CONTROLLER");
    Assert.assertEquals(lastHistoryEntry.get("AUTO_TRIGGER_REASON"),
        "MAX_PARTITION_PER_INSTANCE_EXCEEDED");

    // Remove the maxPartitionPerInstance config
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxPartitionsPerInstance(-1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);

    // Now check that the cluster exited maintenance
    // EXIT, CONTROLLER, for MAX_PARTITION_PER_INSTANCE_EXCEEDED
    history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    lastHistoryEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastHistoryEntry.get("OPERATION_TYPE"), "EXIT");
    Assert.assertEquals(lastHistoryEntry.get("TRIGGERED_BY"), "CONTROLLER");
    Assert.assertEquals(lastHistoryEntry.get("AUTO_TRIGGER_REASON"),
        "MAX_PARTITION_PER_INSTANCE_EXCEEDED");

    // Manually put the cluster in maintenance with a custom field
    Map<String, String> customFieldMap = ImmutableMap.of("k1", "v1", "k2", "v2");
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, TestHelper.getTestMethodName(), customFieldMap);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) != null, 2000L);

    // ENTER, USER, for reason TEST, no internalReason
    history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    lastHistoryEntry =
        convertStringToMap(history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastHistoryEntry.get("OPERATION_TYPE"), "ENTER");
    Assert.assertEquals(lastHistoryEntry.get("TRIGGERED_BY"), "USER");
    Assert.assertEquals(lastHistoryEntry.get("REASON"), TestHelper.getTestMethodName());
    Assert.assertNull(lastHistoryEntry.get("AUTO_TRIGGER_REASON"));
  }

  /**
   * Convert a String representation of a Map into a Map object for verification purposes.
   * @param value
   * @return
   */
  private static Map<String, String> convertStringToMap(String value) throws IOException {
    return new ObjectMapper().readValue(value,
        TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, String.class));
  }

  /**
   * Helper method to get maintenance reason for a specific triggering entity.
   * @param signal The maintenance signal
   * @param triggeringEntity The entity to get reason for
   * @return The reason string, or null if not found
   */
  private static String getMaintenanceReason(MaintenanceSignal signal, MaintenanceSignal.TriggeringEntity triggeringEntity) {
    List<Map<String, String>> reasons = signal.getMaintenanceReasons();
    for (Map<String, String> reason : reasons) {
      String triggeredByStr = reason.get(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name());
      if (triggeredByStr != null && MaintenanceSignal.TriggeringEntity.valueOf(triggeredByStr) == triggeringEntity) {
        return reason.get(PauseSignal.PauseSignalProperty.REASON.name());
      }
    }
    return null;
  }

  /**
   * Utility method to verify maintenance history entry.
   * @param expectedOperationType Expected operation type (ENTER/EXIT)
   * @param expectedTriggeredBy Expected triggering entity (USER/AUTOMATION/CONTROLLER)
   * @param expectedInMaintenanceAfterOperation Expected maintenance state after operation (true/false)
   * @param expectedReason Expected reason (optional, can be null)
   */
  private void verifyMaintenanceHistory(String expectedOperationType, String expectedTriggeredBy,
      String expectedInMaintenanceAfterOperation, String expectedReason) throws Exception {
    ControllerHistory history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    Map<String, String> lastEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastEntry.get("OPERATION_TYPE"), expectedOperationType);
    Assert.assertEquals(lastEntry.get("TRIGGERED_BY"), expectedTriggeredBy);
    Assert.assertEquals(lastEntry.get("IN_MAINTENANCE_AFTER_OPERATION"), expectedInMaintenanceAfterOperation);
    if (expectedReason != null) {
      Assert.assertEquals(lastEntry.get("REASON"), expectedReason);
    }
  }

  /**
   * Test basic multi-actor stacking behavior.
   * Verifies core functionality: actor-based stacking, actor override, simpleFields most recent
   */
  @Test
  public void testAutomationMaintenanceMode() throws Exception {
    boolean result;
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    result = TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
    Assert.assertTrue(result, "Should be out of maintenance mode.");

    // Step 1: USER enters MM (t1) with reason_A
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "reason_A", null);

    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 1);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "reason_A");
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.USER), "reason_A");

    // Verify history entry for USER entering maintenance
    verifyMaintenanceHistory("ENTER", "USER", "true", "reason_A");

    Thread.sleep(10); // Ensure different timestamps

    // Step 2: AUTOMATION enters MM (t2) with reason_B - should stack with USER
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true, "reason_B", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.AUTOMATION); // Most recent
    Assert.assertEquals(signal.getReason(), "reason_B"); // Most recent
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.USER), "reason_A");
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.AUTOMATION), "reason_B");

    // Verify history entry for AUTOMATION entering maintenance
    verifyMaintenanceHistory("ENTER", "AUTOMATION", "true", "reason_B");

    Thread.sleep(10);

    // Step 3: USER enters MM again (t3) with reason_C - should override previous USER entry
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_C", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2); // Still only 2 (USER overrode itself)
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER); // Most recent
    Assert.assertEquals(signal.getReason(), "reason_C"); // Most recent
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.USER),
        "reason_C"); // Updated
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.AUTOMATION),
        "reason_B"); // Unchanged

    // Verify history entry for USER overriding previous entry
    verifyMaintenanceHistory("ENTER", "USER", "true", "reason_C");

    Thread.sleep(10);

    // Step 4: AUTOMATION enters MM again (t4) with reason_D - should override previous AUTOMATION entry
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_D", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2); // Still only 2 (AUTOMATION overrode itself)
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.AUTOMATION); // Most recent
    Assert.assertEquals(signal.getReason(), "reason_D"); // Most recent
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.USER),
        "reason_C"); // Unchanged
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.AUTOMATION),
        "reason_D"); // Updated

    // Verify history entry for AUTOMATION overriding previous entry
    verifyMaintenanceHistory("ENTER", "AUTOMATION", "true", "reason_D");

    // Clean exit sequence: actors exit in order
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 1);
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));
    Assert.assertEquals(signal.getReason(), "reason_D"); // Updated to remaining reason

    // Verify history entry for USER exiting maintenance (but still in maintenance due to AUTOMATION)
    verifyMaintenanceHistory("EXIT", "USER", "true", null);

    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);
    result = TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
    Assert.assertTrue(result, "Should be completely out of maintenance mode.");

    // Verify history entry for AUTOMATION exiting maintenance (completely out)
    verifyMaintenanceHistory("EXIT", "AUTOMATION", "false", null);
  }

  /**
   * USER administrative override after old client data loss
   * 1. Multi-actor setup with CONTROLLER, USER, AUTOMATION
   * 2. Old client wipes listField data (keeps only simpleFields)
   * 3. AUTOMATION tries to exit MM (no-op since its data was wiped)
   * 4. USER exits MM (administrative override - forces complete exit)
   */
  @Test
  public void testLegacyClientCompatibility() throws Exception {
    boolean result;
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxPartitionsPerInstance(-1);
    clusterConfig.setNumOfflineInstancesForAutoExit(-1); // Disable auto-exit to prevent race conditions
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Wait for config to be applied
    result = TestHelper.verify(() -> {
      ClusterConfig currentConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
      return currentConfig.getNumOfflineInstancesForAutoExit() == -1;
    }, 2000L);
    Assert.assertTrue(result, "Config should be applied.");
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_A", null);
    Thread.sleep(10);
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(CLUSTER_NAME, true, "reason_B",
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);
    Thread.sleep(10);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_C", null);

    // Verify multi-actor setup
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 3);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Verify history entry for AUTOMATION entering maintenance
    verifyMaintenanceHistory("ENTER", "AUTOMATION", "true", "reason_C");

    // Simulate old client wiping listField data (only keeps simpleFields)
    Thread.sleep(10);
    ZNRecord record = new ZNRecord("maintenance");
    record.setSimpleField(PauseSignal.PauseSignalProperty.REASON.name(), "reason_D");
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TIMESTAMP.name(),
        String.valueOf(System.currentTimeMillis()));
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name(),
        MaintenanceSignal.TriggeringEntity.USER.name());
    // Old client doesn't set listField - simulates wiping all listField data
    _dataAccessor.setProperty(_keyBuilder.maintenance(), new MaintenanceSignal(record));

    // Verify old client wiped listField data but simpleFields remain
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0,
        "Old client should have wiped listField data");
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "reason_D");
    Assert.assertFalse(signal.hasMaintenanceReasons(),
        "Should have no listField reasons after old client wipe");

    // AUTOMATION tries to exit MM -> should be no-op because its entry was wiped
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);

    // Verify maintenance signal remains the same (no-op)
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal, "Should still be in maintenance after AUTOMATION no-op");
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0);
    Assert.assertEquals(signal.getReason(), "reason_D");
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);

    // Verify history entry for AUTOMATION no-op exit (still in maintenance)
    verifyMaintenanceHistory("EXIT", "AUTOMATION", "true", null);

    // USER tries to exit MM -> should trigger administrative override and delete maintenance ZNode
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);

    // Verify we're completely out of maintenance mode due to USER administrative override
    result = TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
    Assert.assertTrue(result, "Should be completely out of maintenance mode due to USER administrative override.");

    // Verify in history that we're no longer in maintenance
    verifyMaintenanceHistory("EXIT", "USER", "false", null);
  }

  /**
   * Helper method to set up a common scenario for maintenance mode tests:
   * 1. USER enters maintenance mode
   * 2. AUTOMATION enters maintenance mode
   * 3. User B enters maintenance mode (overrides User A)
   * 4. Old client enters maintenance mode (simple fields only - wipes listField data)
   */
  private void setupMultiActorMaintenanceScenario() throws Exception {
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxPartitionsPerInstance(-1);
    clusterConfig.setNumOfflineInstancesForAutoExit(-1); // Disable auto-exit to prevent race conditions
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);
    // Step 0: CONTROLLER puts the cluster into MM (t1)
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_Controller",
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);

    // Verify maintenance signal with USER reason only
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 1);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.CONTROLLER);

    // Step 1: USER (UserA) puts the cluster into MM (t1)
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_A", null);

    // Verify maintenance signal with USER reason only
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);

    // Step 2: AUTOMATION puts the cluster into MM (t2)
    Thread.sleep(10); // Ensure different timestamps
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_B", null);

    // Verify maintenance signal has both reasons
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 3);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Step 3: USER (UserB) puts the cluster into MM (t3) - overrides UserA's entry
    Thread.sleep(10);
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_C", null);

    // Verify maintenance signal still has same number of reasons but UserB's reason replaced UserA's
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 3);
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.USER), "reason_C");

    // Step 4: USER (Old Client) enters cluster into MM (t4)
    // Simulate old client by directly creating a MaintenanceSignal with simple fields only
    // Per design doc: "Legacy clients use the dataAccessor.set() API to create Maintenance signals,
    // which results in the entire ZNRecord being overwritten, including purging all existing ListField entries"
    Thread.sleep(10);
    ZNRecord record = new ZNRecord("maintenance");
    record.setSimpleField(PauseSignal.PauseSignalProperty.REASON.name(), "reason_D");
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TIMESTAMP.name(),
        String.valueOf(System.currentTimeMillis()));
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name(),
        MaintenanceSignal.TriggeringEntity.USER.name());

    // Use setProperty (not updateProperty) to simulate old client completely overwriting the ZNode
    _dataAccessor.setProperty(_keyBuilder.maintenance(), new MaintenanceSignal(record));

    // Verify maintenance signal has updated simple fields but listField data was wiped by old client
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "reason_D");

    // Verify reasons list was wiped by old client (data loss accepted by design)
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0,
        "Old client should have wiped all listField data");
  }

  /**
   * Test Case A: USER administrative override after old client data loss
   * 1. After old client wipes data, verify no listField reasons exist
   * 2. USER tries to exit MM - should trigger administrative override and delete maintenance ZNode
   * 3. Verify maintenance mode is completely exited
   */
  @Test
  public void testUserAdministrativeOverride() throws Exception {
    boolean result;
    // Set up the initial state with all actors having entered maintenance mode
    // Note: setupMultiActorMaintenanceScenario() ends with old client wiping listField data
    setupMultiActorMaintenanceScenario();

    // Verify the old client has wiped the listField data
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0,
        "Old client should have wiped listField data");
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "reason_D");

    // Step 6A: USER (New client) tries to exit MM
    // Since USER doesn't have an entry in listFields.reasons, this should trigger administrative override
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);

    // Verify we're completely out of maintenance mode due to USER administrative override
    result = TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
    Assert.assertTrue(result, "Should be completely out of maintenance mode due to USER administrative override.");

    // Verify in history that we're no longer in maintenance
    verifyMaintenanceHistory("EXIT", "USER", "false", null);
  }

  /**
   * Old client enters, new clients operate independently (no reconciliation)
   * 1. Old client enters maintenance mode without updating the reasons list (data wiped)
   * 2. New client enters maintenance mode and operates independently
   * 3. All actors exit in sequence
   */
  @Test
  public void testOldClientDataLoss() throws Exception {
    boolean result;
    // Set up the initial state with all actors having entered maintenance mode
    setupMultiActorMaintenanceScenario();

    // At this point, the old client in setupMultiActorMaintenanceScenario has wiped the listField data
    // Verify that the old client action resulted in data loss
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());

    // simpleFields should show USER data (from old client)
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "reason_D");

    // But listFields.reasons should be empty (wiped by old client)
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0,
        "Old client should have wiped listField data");
    Assert.assertFalse(signal.hasMaintenanceReasons(),
        "Should not have maintenance reasons after old client wipe");

    // Step 6B: AUTOMATION enters MM again - should work independently (no reconciliation)
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_F", null);

    // Verify signal now has only AUTOMATION reason (no reconciliation of old USER data)
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER),
        "Controller data was not lost");
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER),
        "User data was lost");
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Verify the new AUTOMATION reason
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.AUTOMATION),
        "reason_F");

    // Verify history entry for AUTOMATION entering maintenance after data loss
    verifyMaintenanceHistory("ENTER", "AUTOMATION",
        "true", "reason_F");

    // Exit the only remaining actors
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);

    // Verify we're out of maintenance mode
    result = TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null,
        2000L);
    Assert.assertTrue(result, "Should be completely out of maintenance mode after AUTOMATION exit.");
  }

  /**
   * Test AUTOMATION re-entry after old client wipes data
   * Old client wipes data, AUTOMATION re-enters independently,
   * then sequence of exits with no-ops and administrative override
   */
  @Test
  public void testAutomationReentryAfterDataLoss() throws Exception {
    boolean result;
    // Setup the multi-actor scenario which ends with old client wiping data
    setupMultiActorMaintenanceScenario();

    // AUTOMATION enters MM again (works independently, no reconciliation)
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "reason_E", null);

    // Verify signal now has both USER and AUTOMATION entries.
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.AUTOMATION),
        "reason_E");

    // Verify history entry for AUTOMATION entering maintenance after data loss
    verifyMaintenanceHistory("ENTER", "AUTOMATION",
        "true", "reason_E");


    // Automation exits MM
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);
    // USER exits MM
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);

    // Verify we're completely out of maintenance mode due to USER administrative override
    result = TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
    Assert.assertTrue(result, "Should be out of maintenance mode.");
    // Verify in history that we're no longer in maintenance
    verifyMaintenanceHistory("EXIT", "USER", "false", null);
  }

  /**
   * Test old client force exit by deleting entire maintenance znode
   */
  @Test
  public void testOldClientDeletesEntireZnode() throws Exception {
    // Setup: Multiple actors enter maintenance mode
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "user_reason", null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "automation_reason", null);

    // Verify we have both actors
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);

    // Case D: USER (old client) exits MM by deleting entire znode
    _dataAccessor.removeProperty(_keyBuilder.maintenance());

    // Verify we're completely out of maintenance mode (all data lost)
    boolean result = TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null,
        2000L);
    Assert.assertTrue(result, "Should be completely out of maintenance mode after old client deletes znode.");
  }

  /**
   * Test that simpleFields always reflect the most recently added reason
   */
  @Test
  public void testSimpleFieldsReflectMostRecent() throws Exception {
    // Entry 1: USER at t1
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "user_first", null);

    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getReason(), "user_first");
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);

    // Verify history entry for USER entering maintenance
    verifyMaintenanceHistory("ENTER", "USER", "true", "user_first");

    Thread.sleep(10);

    // Entry 2: AUTOMATION at t2 (should become most recent)
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "automation_second", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getReason(), "automation_second");
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.AUTOMATION);

    // Verify history entry for AUTOMATION entering maintenance
    verifyMaintenanceHistory("ENTER", "AUTOMATION", "true", "automation_second");

    Thread.sleep(10);

    // Entry 3: USER at t3 (should become most recent)
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "user_third", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getReason(), "user_third");
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);

    // Verify history entry for USER overriding previous entry
    verifyMaintenanceHistory("ENTER", "USER", "true", "user_third");

    Thread.sleep(10);

    // Entry 4: AUTOMATION again at t4 (should become most recent, overriding its previous entry)
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "automation_fourth", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getReason(), "automation_fourth");
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.AUTOMATION);
    // Should still have 2 actors (AUTOMATION entry was overwritten)
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);

    // Verify history entry for AUTOMATION overriding previous entry
    verifyMaintenanceHistory("ENTER", "AUTOMATION", "true", "automation_fourth");

    // Clean up
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);

    // Verify in history that we're no longer in maintenance
    verifyMaintenanceHistory("EXIT", "AUTOMATION", "false", null);
  }

  /**
   * Test edge cases around empty maintenance mode
   */
  @Test
  public void testEmptyStateEdgeCases() throws Exception {
    boolean result;
    // Test 1: Try to exit when no maintenance mode exists

    // AUTOMATION tries to exit when no MM exists -> should be no-op
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);
    Assert.assertNull(_dataAccessor.getProperty(_keyBuilder.maintenance()));

    // USER tries to exit when no MM exists -> should be no-op (nothing to override)
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);
    Assert.assertNull(_dataAccessor.getProperty(_keyBuilder.maintenance()));

    // Test 2: Create maintenance mode with only simpleFields (old client style)
    ZNRecord record = new ZNRecord("maintenance");
    record.setSimpleField(PauseSignal.PauseSignalProperty.REASON.name(), "Old client reason");
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name(),
        MaintenanceSignal.TriggeringEntity.USER.name());
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TIMESTAMP.name(),
        String.valueOf(System.currentTimeMillis()));
    _dataAccessor.setProperty(_keyBuilder.maintenance(), new MaintenanceSignal(record));

    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0, "Should have no listField reasons");
    Assert.assertFalse(signal.hasMaintenanceReasons(), "Should report no maintenance reasons");

    // AUTOMATION tries to exit -> should be no-op
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);
    Assert.assertNotNull(_dataAccessor.getProperty(_keyBuilder.maintenance()),
        "Should still exist after no-op");

    // USER tries to exit -> should be administrative override
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);
    result = TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
    Assert.assertTrue(result, "Should be completely out of maintenance mode.");

    // Verify in history that we're no longer in maintenance
    verifyMaintenanceHistory("EXIT", "USER", "false", null);
  }

  /**
   * Test mixed entry/exit scenarios to stress test maintenance mode stacking
   * Verifies complex sequences of actors entering and exiting in different orders
   */
  @Test
  public void testMixedEntryExitScenarios() throws Exception {
    boolean result;
    // Phase 1: Multiple actors enter
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "user_reason_1", null);
    Thread.sleep(10);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "automation_reason_1", null);

    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);
    Assert.assertEquals(signal.getReason(), "automation_reason_1"); // Most recent

    // Verify history entry for AUTOMATION entering maintenance
    verifyMaintenanceHistory("ENTER", "AUTOMATION", "true", "automation_reason_1");

    // Phase 2: One actor exits, then re-enters with different reason
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null); // USER exits
    Thread.sleep(10);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 1);
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Verify history entry for USER exiting maintenance (but still in maintenance due to AUTOMATION)
    verifyMaintenanceHistory("EXIT", "USER", "true", null);

    // USER re-enters with new reason
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "user_reason_2", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);
    Assert.assertEquals(signal.getReason(), "user_reason_2"); // Most recent

    // Verify history entry for USER re-entering maintenance
    verifyMaintenanceHistory("ENTER", "USER", "true", "user_reason_2");

    // Phase 3: Clean exit
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);

    // Verify in history that we're no longer in maintenance
    verifyMaintenanceHistory("EXIT", "AUTOMATION", "false", null);
  }

  /**
   * Test basic multi-actor stacking behavior including CONTROLLER entity
   * Creates actual conditions that trigger CONTROLLER maintenance mode
   */
  @Test
  public void testMultiActorStackingWithController() throws Exception {
    boolean result;
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxPartitionsPerInstance(-1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);
    // Step 1: Directly trigger CONTROLLER maintenance mode using API
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(CLUSTER_NAME, true,
        "Test controller maintenance",
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);

    // Verify CONTROLLER entered maintenance mode automatically
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.CONTROLLER);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 1);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));

    // Verify history entry for CONTROLLER entering maintenance
    verifyMaintenanceHistory("ENTER", "CONTROLLER", "true", "Test controller maintenance");

    // Step 2: USER enters MM - should stack with CONTROLLER
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "user_reason", null);
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "user_reason"); // Most recent
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));

    // Verify history entry for USER entering maintenance
    verifyMaintenanceHistory("ENTER", "USER", "true", "user_reason");

    // Step 3: AUTOMATION enters MM - should stack with both
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "automation_reason", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 3);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.AUTOMATION);
    Assert.assertEquals(signal.getReason(), "automation_reason"); // Most recent
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Verify history entry for AUTOMATION entering maintenance
    verifyMaintenanceHistory("ENTER", "AUTOMATION", "true", "automation_reason");

    // Step 4: USER exits - should remain in maintenance with CONTROLLER and AUTOMATION
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Verify history entry for USER exiting maintenance (but still in maintenance due to others)
    verifyMaintenanceHistory("EXIT", "USER", "true", null);

    // Step 5: AUTOMATION exits - should remain in maintenance with only CONTROLLER
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 1);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Verify history entry for AUTOMATION exiting maintenance (but still in maintenance due to CONTROLLER)
    verifyMaintenanceHistory("EXIT", "AUTOMATION", "true", null);

    // Step 6: Exit CONTROLLER maintenance mode
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(CLUSTER_NAME, false, null,
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);

    // Verify maintenance mode is completely off
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);

    // Verify history entry for CONTROLLER exiting maintenance (completely out)
    verifyMaintenanceHistory("EXIT", "CONTROLLER", "false", null);
  }

  /**
   * Test old client wipes data including CONTROLLER entry
   * Verifies CONTROLLER no-op behavior after data loss
   */
  @Test
  public void testControllerNoOpAfterOldClientWipe() throws Exception {
    boolean result;
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxPartitionsPerInstance(-1);
    clusterConfig.setNumOfflineInstancesForAutoExit(-1); // Disable auto-exit to prevent race conditions
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);
    // Step 1: Directly trigger CONTROLLER maintenance mode using API
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(CLUSTER_NAME, true,
        "Test controller maintenance",
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);

    // Verify CONTROLLER entered maintenance mode
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.CONTROLLER);

    // Add USER and AUTOMATION to create multi-actor scenario
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "user_reason", null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "automation_reason", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 3);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Simulate old client wiping all data
    ZNRecord record = new ZNRecord("maintenance");
    record.setSimpleField(PauseSignal.PauseSignalProperty.REASON.name(), "Old client reason");
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TIMESTAMP.name(),
        String.valueOf(System.currentTimeMillis()));
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name(),
        MaintenanceSignal.TriggeringEntity.USER.name());
    // Old client doesn't set listField - simulates wiping all listField data
    _dataAccessor.setProperty(_keyBuilder.maintenance(), new MaintenanceSignal(record));

    // Verify old client wiped listField data
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0,
        "Old client should have wiped listField data");
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);

    // Try CONTROLLER exit - should be no-op since its entry was wiped
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(CLUSTER_NAME, false, null,
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);

    // Verify maintenance signal remains the same (no-op)
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal, "Should still be in maintenance after CONTROLLER no-op");
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0);
    Assert.assertEquals(signal.getReason(), "Old client reason");

    // Verify history entry for CONTROLLER no-op exit (still in maintenance)
    verifyMaintenanceHistory("EXIT", "CONTROLLER", "true", null);

    // Try AUTOMATION exit - should also be no-op since its entry was wiped
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);

    // Verify maintenance signal remains the same (no-op)
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal, "Should still be in maintenance after AUTOMATION no-op");
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0);
    Assert.assertEquals(signal.getReason(), "Old client reason");

    // Verify history entry for AUTOMATION no-op exit (still in maintenance)
    verifyMaintenanceHistory("EXIT", "AUTOMATION", "true", null);

    // USER tries to exit -> should be administrative override
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);

    // Verify completely out of maintenance mode
    result = TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
    Assert.assertTrue(result, "Should be completely out of maintenance mode due to USER administrative override.");

    // Verify in history that we're no longer in maintenance
    verifyMaintenanceHistory("EXIT", "USER", "false", null);
  }

  /**
   * Test CONTROLLER override behavior - same entity overwrites previous entry
   */
  @Test
  public void testControllerOverrideBehavior() throws Exception {
    boolean result;
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxPartitionsPerInstance(-1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);
    // Step 1: Directly trigger CONTROLLER maintenance mode with specific auto-trigger reason
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(CLUSTER_NAME, true,
        "Initial controller reason",
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);

    // Verify CONTROLLER entered with auto-trigger reason
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.CONTROLLER);
    Assert.assertEquals(signal.getAutoTriggerReason(),
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 1);

    // Manually trigger CONTROLLER entry again with different reason (to test override)
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(CLUSTER_NAME, true,
        "manual_controller_reason",
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.CONTROLLER);
    Assert.assertEquals(signal.getAutoTriggerReason(),
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);
    Assert.assertEquals(signal.getMaintenanceReasons().size(),
        1); // Should still be only 1 (CONTROLLER overrode itself)

    // Add another actor to verify stacking still works
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "user_reason", null);

    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));

    // Cleanup
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(CLUSTER_NAME, false,
        null,
        MaintenanceSignal.AutoTriggerReason.MAX_INSTANCES_UNABLE_TO_ACCEPT_ONLINE_REPLICAS);
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
  }

  /**
   * Test reconciliation of legacy USER data when new client adds reason
   * Verifies the critical design requirement to preserve old client data
   */
  @Test
  public void testReconciliationOfLegacyUserData() throws Exception {
    boolean result;
    // Step 1: Old client sets simpleFields only (no listFields)
    ZNRecord record = new ZNRecord("maintenance");
    record.setSimpleField(PauseSignal.PauseSignalProperty.REASON.name(), "legacy_user_reason");
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name(),
        MaintenanceSignal.TriggeringEntity.USER.name());
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TIMESTAMP.name(),
        String.valueOf(System.currentTimeMillis()));
    _dataAccessor.setProperty(_keyBuilder.maintenance(), new MaintenanceSignal(record));

    // Verify old client state (no listFields reasons)
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 0,
        "Old client should have no listField data");
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "legacy_user_reason");

    // Step 2: New client adds AUTOMATION reason - should trigger reconciliation
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "automation_reason", null);

    // Step 3: Verify both reasons are preserved after reconciliation
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasons().size(), 2,
        "Should have both reconciled USER and new AUTOMATION reasons");
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER),
        "USER reason should be preserved");
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION),
        "AUTOMATION reason should be added");
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.USER),
        "legacy_user_reason");
    Assert.assertEquals(getMaintenanceReason(signal, MaintenanceSignal.TriggeringEntity.AUTOMATION),
        "automation_reason");

    // Cleanup
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME,
        false, null, null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME,
        false, null, null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null,
        2000L);
  }
}
