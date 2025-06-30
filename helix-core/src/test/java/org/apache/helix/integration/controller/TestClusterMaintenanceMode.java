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
   * Test that automation triggered maintenance mode works correctly
   * and multi-actor maintenance mode requires all actors to exit
   */
  @Test
  public void testAutomationMaintenanceMode() throws Exception {
    // Make sure we're not in maintenance mode at the start
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);

    // Put cluster in maintenance mode via automation
    Map<String, String> customFields = ImmutableMap.of("TICKET", "AUTO-123", "SOURCE", "HelixACM");
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "Automation maintenance", customFields);

    // Verify we are in maintenance mode with the right attributes
    MaintenanceSignal maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.AUTOMATION);

    // Verify custom fields were set
    for (Map.Entry<String, String> entry : customFields.entrySet()) {
      Assert.assertEquals(maintenanceSignal.getRecord().getSimpleField(entry.getKey()),
          entry.getValue());
    }

    // Manually put the cluster in maintenance too - this should keep both reasons
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "User maintenance", null);

    // Verify we have both reasons
    maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertTrue(maintenanceSignal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));
    Assert.assertTrue(maintenanceSignal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertEquals(maintenanceSignal.getMaintenanceReasonsCount(), 2);

    // Exit maintenance mode for USER only
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Verify we're still in maintenance mode with only AUTOMATION reason
    maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertFalse(maintenanceSignal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(maintenanceSignal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));
    Assert.assertEquals(maintenanceSignal.getMaintenanceReasonsCount(), 1);

    // Exit maintenance mode for AUTOMATION
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Verify we're now out of maintenance mode
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
  }

  /**
   * Test that removing a maintenance reason doesn't add duplicate entries in the reasons list
   */
  @Test
  public void testRemoveMaintenanceReasonNoDuplicates() throws Exception {
    // Make sure we start clean
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);

    // First put the cluster in maintenance mode via USER
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "User entry", null);

    // Then add AUTOMATION reason
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "Automation entry", null);

    // Verify we have both reasons
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 2);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Remove AUTOMATION reason
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Verify we only have USER reason and no duplicate entries
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Verify the reasons list field to ensure no duplicates
    List<String> reasonsList = signal.getRecord().getListField("reasons");
    Assert.assertEquals(reasonsList.size(), 1, "Should have exactly 1 entry in reasons list");

    // Verify the simple fields are correct
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "User entry");

    // Remove USER reason
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Verify we're out of maintenance mode
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
  }

  /**
   * Test that the code can handle an old client that writes a MaintenanceSignal
   * without using the multi-actor maintenance API (no reasons list).
   */
  @Test
  public void testLegacyClientCompatibility() throws Exception {
    // Simulate an old client by creating a MaintenanceSignal with only simple fields
    ZNRecord record = new ZNRecord("maintenance");
    // Add just the simple fields like an old client would
    record.setSimpleField(PauseSignal.PauseSignalProperty.REASON.name(), "Legacy client reason");
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name(),
        MaintenanceSignal.TriggeringEntity.USER.name());
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TIMESTAMP.name(),
        String.valueOf(System.currentTimeMillis()));

    // Write it directly to ZK
    _dataAccessor.setProperty(_keyBuilder.maintenance(), new MaintenanceSignal(record));

    // Verify the maintenance signal is there
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "Legacy client reason");

    // Verify that the list field does not exist yet
    List<String> reasonsListBefore = signal.getRecord().getListField("reasons");
    Assert.assertNull(reasonsListBefore, "Should not have reasons list field yet");

    // Explicitly call reconcileMaintenanceData
    boolean updated = signal.reconcileMaintenanceData();
    Assert.assertTrue(updated, "Should have updated the ZNode during reconciliation");

    // Verify the reasons list was created
    List<String> reasonsListAfter = signal.getRecord().getListField("reasons");
    Assert.assertNotNull(reasonsListAfter, "Should have created reasons list field");
    Assert.assertEquals(reasonsListAfter.size(), 1, "Should have added exactly one entry");

    // Verify the entry content contains all simple fields
    String entryString = reasonsListAfter.get(0);
    Assert.assertTrue(entryString.contains("Legacy client reason"),
        "Entry should contain the reason text");
    Assert.assertTrue(entryString.contains(MaintenanceSignal.TriggeringEntity.USER.name()),
        "Entry should contain the entity");

    // Save to ZK
    _dataAccessor.setProperty(_keyBuilder.maintenance(), signal);

    // Now read it back to verify persistence
    MaintenanceSignal readBackSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    List<String> persistedReasonsList = readBackSignal.getRecord().getListField("reasons");
    Assert.assertEquals(persistedReasonsList.size(), 1, "Should have persisted exactly one entry");

    // Now try to add a new reason via the new API - should work with the legacy format
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "Automation entry with legacy client", null);

    // Verify both reasons exist
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 2);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Try removing the automation reason
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Verify only USER reason remains
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));

    // Clean up
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
  }

  /**
   * Test that the IN_MAINTENANCE_AFTER_OPERATION field in the history record
   * is only set to false when all maintenance reasons are gone.
   */
  @Test(dependsOnMethods = "testLegacyClientCompatibility")
  public void testMaintenanceHistoryAfterOperationFlag() throws Exception {
    // Make sure we start clean
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);

    // First put the cluster in maintenance mode via USER
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true,
        "User entry2", null);

    // Verify history shows IN_MAINTENANCE_AFTER_OPERATION as true
    ControllerHistory history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    Map<String, String> lastEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastEntry.get("OPERATION_TYPE"), "ENTER");
    Assert.assertEquals(lastEntry.get("TRIGGERED_BY"), "USER");
    Assert.assertEquals(lastEntry.get("IN_MAINTENANCE_AFTER_OPERATION"), "true");

    // Add a second actor (AUTOMATION)
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "Automation entry2", null);

    // Verify history shows IN_MAINTENANCE_AFTER_OPERATION as true
    history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    lastEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastEntry.get("OPERATION_TYPE"), "ENTER");
    Assert.assertEquals(lastEntry.get("TRIGGERED_BY"), "AUTOMATION");
    Assert.assertEquals(lastEntry.get("IN_MAINTENANCE_AFTER_OPERATION"), "true");

    // Remove AUTOMATION actor, but we should still be in maintenance mode because USER remains
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);

    // Verify we're still in maintenance mode with a single actor
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));

    // Verify history shows IN_MAINTENANCE_AFTER_OPERATION as true
    history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    lastEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastEntry.get("OPERATION_TYPE"), "EXIT");
    Assert.assertEquals(lastEntry.get("TRIGGERED_BY"), "AUTOMATION");
    Assert.assertEquals(lastEntry.get("IN_MAINTENANCE_AFTER_OPERATION"), "true");

    // Remove USER actor, which should get us out of maintenance mode
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false,
        null, null);

    // Verify we're out of maintenance mode
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);

    // Verify history shows IN_MAINTENANCE_AFTER_OPERATION as false
    history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    lastEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastEntry.get("OPERATION_TYPE"), "EXIT");
    Assert.assertEquals(lastEntry.get("TRIGGERED_BY"), "USER");
    Assert.assertEquals(lastEntry.get("IN_MAINTENANCE_AFTER_OPERATION"), "false");
  }

  /**
   * Set up the initial state and mock components for maintenance mode tests.
   * This ensures maintenance mode doesn't get automatically exited.
   */
  private void setupMaintenanceModeTest() throws Exception {
    // Set cluster config to ensure auto-exit conditions are never met
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxOfflineInstancesAllowed(2);
    clusterConfig.setNumOfflineInstancesForAutoExit(1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Kill 3 instances
    for (int i = 0; i < 3; i++) {
      _participants[i].syncStop();
    }
    TestHelper.verify(() -> _dataAccessor.getChildNames(_keyBuilder.liveInstances()).isEmpty(), 2000L);

    // Check that the cluster is in maintenance
    MaintenanceSignal maintenanceSignal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(maintenanceSignal);
  }

  /**
   * Helper method to set up a common scenario for maintenance mode tests:
   * 1. Controller enters maintenance mode
   * 2. User A enters maintenance mode
   * 3. User B enters maintenance mode (overrides User A)
   * 4. Automation enters maintenance mode
   * 5. Old client enters maintenance mode (simple fields only)
   *
   */
  private void setupMultiActorMaintenanceScenario() throws Exception {
    // Set up the maintenance mode test environment
    setupMaintenanceModeTest();

    // Verify maintenance signal with CONTROLLER reason only
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.CONTROLLER);

    // Step 2: USER (UserA) puts the cluster into MM (t2)
    Thread.sleep(10); // Ensure different timestamps
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "reason_B", null);

    // Verify maintenance signal has both reasons
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 2);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));

    // Step 3: USER (UserB) puts the cluster into MM (t3) - overrides UserA's entry
    Thread.sleep(10);
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "reason_C", null);

    // Verify maintenance signal still has same number of reasons but UserB's reason replaced UserA's
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 2);
    Assert.assertEquals(signal.getMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER), "reason_C");

    // Step 4: AUTOMATION (HelixACM) puts the cluster into MM (t4)
    Thread.sleep(10);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true, "reason_D", null);

    // Verify maintenance signal has all three reasons
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 3);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Step 5: USER (Old Client) enters cluster into MM (t5)
    // Simulate old client by directly creating a MaintenanceSignal with simple fields only
    Thread.sleep(10);
    ZNRecord record = new ZNRecord("maintenance");
    record.setSimpleField(PauseSignal.PauseSignalProperty.REASON.name(), "reason_E");
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TIMESTAMP.name(),
        String.valueOf(System.currentTimeMillis()));
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name(),
        MaintenanceSignal.TriggeringEntity.USER.name());

    // Write directly to ZK
    _dataAccessor.updateProperty(_keyBuilder.maintenance(), new MaintenanceSignal(record));

    // Verify maintenance signal has updated simple fields but listField still has old USER entry
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "reason_E");

    // Verify reasons list still has original 3 entries (not updated by old client)
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 3);
  }

  /**
   * Test Case A: New clients interaction where each actor enters and exits properly
   * 1. Each actor (User, Automation, Controller) enters maintenance mode
   * 2. Each actor exits maintenance mode in sequence
   * 3. Verify maintenance flags in history records
   */
  @Test
  public void testMultiActorMaintenanceModeExitSequence() throws Exception {
    // Set up the initial state with all actors having entered maintenance mode
    setupMultiActorMaintenanceScenario();

    // Step 6A: USER (New client) exits MM
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Verify USER reason is gone, but CONTROLLER and AUTOMATION remain
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 2);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));

    // Verify in history that we're still in maintenance
    ControllerHistory history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    Map<String, String> lastEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastEntry.get("OPERATION_TYPE"), "EXIT");
    Assert.assertEquals(lastEntry.get("TRIGGERED_BY"), "USER");
    Assert.assertEquals(lastEntry.get("IN_MAINTENANCE_AFTER_OPERATION"), "true");

    // Step 7A: AUTOMATION exits MM
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Verify AUTOMATION reason is gone, only CONTROLLER remains
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));

    // Verify in history that we're still in maintenance
    history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    lastEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastEntry.get("OPERATION_TYPE"), "EXIT");
    Assert.assertEquals(lastEntry.get("TRIGGERED_BY"), "AUTOMATION");
    Assert.assertEquals(lastEntry.get("IN_MAINTENANCE_AFTER_OPERATION"), "true");

    // Step 8A: CONTROLLER exits MM
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(
        CLUSTER_NAME, false, null, MaintenanceSignal.AutoTriggerReason.NOT_APPLICABLE);

    // Verify we're out of maintenance mode
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);

    // Verify in history that we're no longer in maintenance
    history = _dataAccessor.getProperty(_keyBuilder.controllerLeaderHistory());
    lastEntry = convertStringToMap(
        history.getMaintenanceHistoryList().get(history.getMaintenanceHistoryList().size() - 1));
    Assert.assertEquals(lastEntry.get("OPERATION_TYPE"), "EXIT");
    Assert.assertEquals(lastEntry.get("TRIGGERED_BY"), "CONTROLLER");
    Assert.assertEquals(lastEntry.get("IN_MAINTENANCE_AFTER_OPERATION"), "false");
  }

  /**
   * Test Case B: Old client enters, new clients reconcile during future operations
   * 1. Old client enters maintenance mode without updating the reasons list
   * 2. New client enters maintenance mode and reconciles the list
   * 3. All actors exit in sequence
   */
  @Test
  public void testMultiActorMaintenanceModeReconciliation() throws Exception {
    // Set up the initial state with all actors having entered maintenance mode
    setupMultiActorMaintenanceScenario();

    // Step 6B: AUTOMATION enters MM again - should reconcile the old client's USER update
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true, "reason_F", null);

    // Verify signal has reconciled the old client's USER update with the timestamp
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 3);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.CONTROLLER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Verify reason is now "reason_E"
    Assert.assertEquals(signal.getMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER), "reason_E");

    // Exit all actors in sequence
    _gSetupTool.getClusterManagementTool().autoEnableMaintenanceMode(
        CLUSTER_NAME, false, null, MaintenanceSignal.AutoTriggerReason.NOT_APPLICABLE);
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Verify we're out of maintenance mode
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
  }

  /**
   * Test Case C: Old client exits maintenance mode while other actors still have reasons
   * 1. Old client bypasses the normal API and just deletes the maintenance node
   * 2. Verify the cluster exits maintenance mode completely
   */
  @Test
  public void testMultiActorMaintenanceModeOldClientExit() throws Exception {
    // Set up the initial state with all actors having entered maintenance mode
    setupMultiActorMaintenanceScenario();

    // Step 6C: USER (old client) exits MM - simulating old client removing entire node
    _dataAccessor.removeProperty(_keyBuilder.maintenance());

    // Verify we're out of maintenance mode completely
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
  }

  /**
   * Old client overrides simple fields after new clients enter MM,
   * then new client exits but should not exit maintenance mode completely
   * 1. AUTOMATION enters MM (new client)
   * 2. USER enters MM again (old client - only updates simple fields)
   * 3. AUTOMATION exits MM
   * 4. Verify we're still in maintenance mode because USER reason remains
   */
  @Test
  public void testMultiActorMaintenanceModeOldClientOverride() throws Exception {
    // Step 1: AUTOMATION enters MM (t2)
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true,
        "AUTOMATION reason", null);

    // Verify maintenance signal has only AUTOMATION reason.
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Step 2: USER enters MM (t3) via old client (only updates simple fields)
    Thread.sleep(10);
    long t3 = System.currentTimeMillis();
    // Simulate an old client by creating a MaintenanceSignal with only simple fields
    ZNRecord record = new ZNRecord(signal.getRecord());
    record.setSimpleField(PauseSignal.PauseSignalProperty.REASON.name(), "USER old client reason");
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TIMESTAMP.name(), String.valueOf(t3));
    record.setSimpleField(MaintenanceSignal.MaintenanceSignalProperty.TRIGGERED_BY.name(),
        MaintenanceSignal.TriggeringEntity.USER.name());
    // Don't update the listField - simulate old client behavior

    // Write it directly to ZK
    _dataAccessor.setProperty(_keyBuilder.maintenance(), new MaintenanceSignal(record));

    // Verify the simple fields were updated but the reasons list still has old USER entry
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "USER old client reason");

    // MaintenanceReasonsCount should still be 2 because the old client didn't update the reasons list
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);

    // Step 3: AUTOMATION exits MM
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Verify we're still in maintenance mode because USER reason remains
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.USER));
    Assert.assertFalse(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Check that simple fields were updated to USER values
    Assert.assertEquals(signal.getTriggeringEntity(), MaintenanceSignal.TriggeringEntity.USER);
    Assert.assertEquals(signal.getReason(), "USER old client reason");

    // Clean up
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
  }

  /**
   * Entity tries to exit MM without having entered it
   * 1. AUTOMATION enters MM
   * 2. USER tries to exit MM (shouldn't affect MM state)
   * 3. Verify we're still in maintenance mode because USER wasn't an actor
   * 4. AUTOMATION exits MM
   * 5. Verify we're out of maintenance mode
   */
  @Test
  public void testMultiActorMaintenanceModeInvalidExit() throws Exception {
    // Step 1: AUTOMATION enters MM
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, true, "AUTOMATION reason", null);

    // Verify maintenance signal with AUTOMATION reason
    MaintenanceSignal signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Step 2: USER tries to exit MM (shouldn't affect MM state)
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Step 3: Verify we're still in maintenance mode
    signal = _dataAccessor.getProperty(_keyBuilder.maintenance());
    Assert.assertNotNull(signal);
    Assert.assertEquals(signal.getMaintenanceReasonsCount(), 1);
    Assert.assertTrue(signal.hasMaintenanceReason(MaintenanceSignal.TriggeringEntity.AUTOMATION));

    // Step 4: AUTOMATION exits MM
    _gSetupTool.getClusterManagementTool().automationEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Step 5: Verify we're out of maintenance mode
    TestHelper.verify(() -> _dataAccessor.getProperty(_keyBuilder.maintenance()) == null, 2000L);
  }
}
